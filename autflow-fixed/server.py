#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AutoFlow · 多智能体 Workflow 编排平台 —— 原型后端

仅依赖 Python 标准库（3.9+）。实现：
  - 工作流定义与 DAG 校验（环检测、悬空依赖、重复边、deps/edges 一致性）
  - 确定性调度引擎（状态机、并发领取、条件分支、汇聚、重试、人工审批）
  - 质量门（格式门 / 编译门 / 评审门 / 人工门）
  - 事件流与快照（SSE 推送 + 全量快照兜底）

启动:  python server.py [--port 8010]
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
import queue
import re
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
# 数据目录可用环境变量覆盖，便于测试与多实例隔离；默认仍是项目内 data/
DATA_DIR = os.environ.get("AUTFLOW_DATA_DIR") or os.path.join(BASE_DIR, "data")
STORE_PATH = os.path.join(DATA_DIR, "workflows.json")
RUN_STATE_PATH = os.path.join(DATA_DIR, "run_state.json")
SPEC_PATH = os.path.join(DATA_DIR, "specs.json")
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")
# 每次运行一个隔离工作区：模型在这里真的写代码、真的跑测试
WORKSPACES_DIR = os.path.join(DATA_DIR, "workspaces")
# 「查看代码」的按需生成缓存：模拟运行没有落盘文件时生成一次存这里，之后只读不调模型
PREVIEW_CACHE_DIR = os.path.join(DATA_DIR, "preview_cache")
# 默认只跑模拟工作流。真实代码写盘/命令执行必须通过环境变量显式启用。
LOCAL_CODE_EXECUTION_ENABLED = os.environ.get("AUTFLOW_ENABLE_LOCAL_CODE_EXECUTION") == "1"

sys.path.insert(0, BASE_DIR)
import llm_client                                    # noqa: E402  模型接入与密钥管理
import spec_import                                   # noqa: E402  设计规格文档解析
import symbols as symbol_index                       # noqa: E402  通用符号切分
import tool_agent                                    # noqa: E402  带工具的真实执行器
import multi_agent                                   # noqa: E402  多智能体规划讨论

SETTINGS = llm_client.SettingsStore(SETTINGS_PATH)
LLM = llm_client.LLMClient(SETTINGS)
PLANNER = multi_agent.PlanManager(LLM, SETTINGS)

# --------------------------------------------------------------------------
# 种子工作流定义（首次启动写入仓库，作为 v1 发布版本）
# --------------------------------------------------------------------------
WORKFLOW = {
    "workflow_id": "wf_login_001",
    "name": "登录功能开发工作流",
    "version": "v1",
    "description": "需求 → 设计 → 并行实现/扫描 → 质量门判定 → 分支 → 汇聚 → 人工审批 → 交付",
    "requirement": "为系统实现用户名密码登录：支持记住登录态、失败重试锁定、登录日志审计",
    "concurrency_limit": 3,
    "nodes": [
        {"id": "N1", "type": "start", "name": "需求输入", "role": "system", "deps": []},
        {"id": "N2", "type": "agent", "name": "接口与数据模型设计", "role": "architect",
         "deps": ["N1"], "out_schema": "design-v1", "max_retries": 2, "timeout": 300},
        {"id": "N3", "type": "agent", "name": "登录接口实现", "role": "coder",
         "deps": ["N2"], "out_schema": "code-result-v1", "max_retries": 3, "timeout": 600},
        {"id": "N4", "type": "agent", "name": "安全基线扫描", "role": "qa",
         "deps": ["N2"], "out_schema": "scan-v1", "max_retries": 2, "timeout": 300},
        {"id": "N5", "type": "condition", "name": "质量门判定", "role": "qa",
         "deps": ["N3", "N4"], "expression": "qa_score >= 80", "max_retries": 0},
        {"id": "N6", "type": "agent", "name": "回归测试", "role": "qa",
         "deps": ["N5"], "branch": "true", "out_schema": "test-v1", "max_retries": 2},
        {"id": "N7", "type": "agent", "name": "按评审意见重构", "role": "coder",
         "deps": ["N5"], "branch": "false", "out_schema": "code-result-v1", "max_retries": 2},
        {"id": "N8", "type": "agent", "name": "合并验证", "role": "qa",
         "deps": ["N6", "N7"], "join": "any", "out_schema": "verify-v1", "max_retries": 2},
        {"id": "N9", "type": "approval", "name": "上线审批", "role": "human",
         "deps": ["N8"], "risk": "high"},
        {"id": "N10", "type": "end", "name": "交付完成", "role": "system", "deps": ["N9"]},
    ],
    "edges": [
        {"source": "N1", "target": "N2"},
        {"source": "N2", "target": "N3"},
        {"source": "N2", "target": "N4"},
        {"source": "N3", "target": "N5"},
        {"source": "N4", "target": "N5"},
        {"source": "N5", "target": "N6", "label": "true"},
        {"source": "N5", "target": "N7", "label": "false"},
        {"source": "N6", "target": "N8"},
        {"source": "N7", "target": "N8"},
        {"source": "N8", "target": "N9"},
        {"source": "N9", "target": "N10"},
    ],
}

ROLE_META = {
    "system":    {"label": "系统",     "color": "#64748b", "base_sec": 0.6},
    "pm":        {"label": "产品经理", "color": "#8b5cf6", "base_sec": 1.2},
    "architect": {"label": "架构师",   "color": "#0ea5e9", "base_sec": 1.8},
    "coder":     {"label": "开发者",   "color": "#2563eb", "base_sec": 2.6},
    "tester":    {"label": "测试工程师", "color": "#14b8a6", "base_sec": 1.6},
    "reviewer":  {"label": "代码评审", "color": "#a855f7", "base_sec": 1.2},
    "qa":        {"label": "质量工程师", "color": "#f59e0b", "base_sec": 1.4},
    "human":     {"label": "人工审批", "color": "#d97706", "base_sec": 0.0},
}

TERMINAL = {"SUCCESS", "FAILED", "SKIPPED", "CANCELLED"}
# 会让下游无法继续推进的上游状态（含重启恢复期的“待核对”）
BLOCKING = {"FAILED", "WAITING_HUMAN", "CANCELLED", "UNCERTAIN"}

NODE_FIELDS = ("id", "type", "name", "role", "deps", "status", "retry_count", "attempts",
               "gate", "output", "logs", "duration", "message", "expression", "branch",
               "join", "max_retries", "out_schema", "started_at", "finished_at", "branch_taken",
               # 任务清单字段（由规格分解而来）：进快照后前端右栏才能显示验收条款与规格出处
               "task_id", "module", "spec_ref", "priority", "estimate", "acceptance",
               "acceptance_sources", "kind", "spec_revision", "spec_hash",
               # 任务分组（part）与这个节点指定的 AI（ai 为空表示按分组/默认走）
               "part", "part_label", "ai")


# 任务分组：比"阶段"更细的一层，画布上同组节点用同色底框圈起来
PARTS = [
    {"id": "skeleton", "label": "工程骨架", "color": "#64748b"},
    {"id": "data", "label": "数据模型", "color": "#0ea5e9"},
    {"id": "api", "label": "接口实现", "color": "#8b5cf6"},
    {"id": "feature", "label": "功能实现", "color": "#22c55e"},
    {"id": "test", "label": "测试验证", "color": "#f59e0b"},
    {"id": "review", "label": "代码评审", "color": "#ef4444"},
    {"id": "integrate", "label": "集成验收", "color": "#14b8a6"},
    {"id": "delivery", "label": "交付", "color": "#a855f7"},
]
PART_BY_ID = {p["id"]: p for p in PARTS}
# 模板自带的流程节点没有 task 记录，按名字/角色归入分组
PART_NAME_HINTS = (
    ("需求输入", "skeleton"), ("需求分析", "skeleton"), ("架构", "skeleton"),
    ("质量门", "test"), ("回归测试", "test"), ("修复", "feature"),
    ("合并验证", "integrate"), ("安全", "review"), ("合规评审", "review"),
    ("上线审批", "delivery"), ("交付", "delivery"),
)
PART_ROLE_HINTS = {"pm": "skeleton", "architect": "skeleton", "tester": "test",
                   "reviewer": "review", "ops": "delivery", "human": "delivery",
                   "system": "skeleton", "coder": "feature"}
KIND_PART = {"foundation": "skeleton", "design": "skeleton",
             "implementation": "feature", "verification": "test",
             "review": "review", "integration": "integrate",
             "delivery": "delivery"}


def part_of_task(task):
    """任务属于哪个分组：先看显式 part，再按标题细化，最后按 kind 回落。"""
    raw = (task.get("part") or "").strip()
    if raw in PART_BY_ID:
        return raw
    title = (task.get("title") or "") + " " + (task.get("what") or "")
    if "数据模型" in title or "迁移" in title:
        return "data"
    if "接口" in title:
        return "api"
    return KIND_PART.get(task.get("kind") or "", "feature")


def part_label(part_id):
    return PART_BY_ID.get(part_id, {}).get("label") or "其他"


def annotate_parts(nodes):
    """给没有分组的节点补上 part（任务节点在 plan_from_tasks 里已带）。"""
    for n in nodes:
        if n.get("part"):
            continue
        name = n.get("name") or ""
        pid = ""
        for kw, cand in PART_NAME_HINTS:
            if kw in name:
                pid = cand
                break
        n["part"] = pid or PART_ROLE_HINTS.get(n.get("role") or "", "feature")
        n["part_label"] = part_label(n["part"])


# 内置演示流程是手写常量，没有 task 记录：进内存时补一次分组
annotate_parts(WORKFLOW["nodes"])


# --------------------------------------------------------------------------
# DAG 校验
# --------------------------------------------------------------------------
def validate_workflow(wf):
    """返回 (ok, errors[], warnings[], stats)。错误定位到具体节点或边。"""
    errors, warnings = [], []
    nodes = {n["id"]: n for n in wf.get("nodes", [])}
    edges = wf.get("edges", [])

    if not nodes:
        errors.append({"code": "E-EMPTY", "msg": "工作流没有任何节点"})
        return False, errors, warnings, {}

    seen = set()
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s not in nodes:
            errors.append({"code": "E-EDGE", "msg": "边 %s→%s 的源节点不存在" % (s, t)})
        if t not in nodes:
            errors.append({"code": "E-EDGE", "msg": "边 %s→%s 的目标节点不存在" % (s, t)})
        if (s, t) in seen:
            errors.append({"code": "E-DUP-EDGE", "msg": "重复的边 %s→%s" % (s, t)})
        seen.add((s, t))

    for nid, n in nodes.items():
        deps = list(n.get("deps", []))
        for d in deps:
            if d not in nodes:
                errors.append({"code": "E-DANGLING", "msg": "节点 %s 依赖了不存在的节点 %s" % (nid, d)})
            elif (d, nid) not in seen:
                errors.append({"code": "E-INCONSISTENT",
                               "msg": "节点 %s 声明依赖 %s，但缺少对应边 %s→%s" % (nid, d, d, nid)})
        for e in edges:
            if e.get("target") == nid and e.get("source") not in deps:
                errors.append({"code": "E-INCONSISTENT",
                               "msg": "存在边 %s→%s，但 %s 的 deps 未声明该依赖"
                                      % (e.get("source"), nid, nid)})

    indeg = {nid: 0 for nid in nodes}
    adj = {nid: [] for nid in nodes}
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s in nodes and t in nodes:
            adj[s].append(t)
            indeg[t] += 1

    q = queue.Queue()
    for nid, d in indeg.items():
        if d == 0:
            q.put(nid)
    order = []
    while not q.empty():
        cur = q.get()
        order.append(cur)
        for nxt in adj[cur]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.put(nxt)
    if len(order) != len(nodes):
        cyc = [nid for nid, d in indeg.items() if d > 0]
        errors.append({"code": "E-CYCLE",
                       "msg": "检测到环依赖：" + " → ".join(cyc) + " → …", "nodes": cyc})

    if order:
        reachable = set()
        stack = [n for n in nodes if not nodes[n].get("deps")]
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(adj[cur])
        orphan = [n for n in nodes if n not in reachable]
        if orphan:
            warnings.append({"code": "W-ORPHAN", "msg": "不可达节点：" + "、".join(orphan)})

    for nid, n in nodes.items():
        if n.get("type") == "condition":
            labels = {e.get("label") for e in edges if e.get("source") == nid}
            if not {"true", "false"} <= labels:
                errors.append({"code": "E-BRANCH",
                               "msg": "条件节点 %s 的出边必须分别标注 true / false" % nid})

    errors.extend(_validate_task_graph(wf, nodes))
    stats = {"nodes": len(nodes), "edges": len(edges),
             "tasks": len(wf.get("tasks") or []),
             "critical_path": _critical_path(nodes, adj, order)}
    return len(errors) == 0, errors, warnings, stats


def _validate_task_graph(wf, nodes):
    """任务清单与 DAG 任务节点必须一一对应，避免编辑后引用悬空。"""
    tasks = wf.get("tasks")
    if tasks is None:
        return []
    if not isinstance(tasks, list):
        return [{"code": "E-TASK", "msg": "tasks 必须是数组"}]
    errors = []
    task_ids = set()
    for task in tasks:
        tid = task.get("id") if isinstance(task, dict) else None
        if not tid:
            errors.append({"code": "E-TASK", "msg": "任务缺少 id"})
            continue
        if tid in task_ids:
            errors.append({"code": "E-TASK-DUP", "msg": "任务 ID 重复：%s" % tid})
        task_ids.add(tid)
        criteria = task.get("acceptance") or []
        sources = task.get("acceptance_sources")
        if sources is not None and len(sources) != len(criteria):
            errors.append({
                "code": "E-TASK-SOURCE",
                "msg": "任务 %s 的验收标准与规格出处数量不一致" % tid,
            })
        if task.get("out_schema") and not isinstance(task.get("out_schema"), str):
            errors.append({"code": "E-TASK-SCHEMA", "msg": "任务 %s 的 out_schema 不合法" % tid})

    node_of_task = {}
    for nid, node in nodes.items():
        tid = node.get("task_id")
        if not tid:
            continue
        if tid not in task_ids:
            errors.append({"code": "E-TASK-ORPHAN",
                           "msg": "节点 %s 引用了不存在的任务 %s" % (nid, tid)})
        if tid in node_of_task:
            errors.append({"code": "E-TASK-NODE-DUP",
                           "msg": "任务 %s 对应多个节点：%s、%s"
                                  % (tid, node_of_task[tid], nid)})
        node_of_task[tid] = nid

    for task in tasks:
        if not isinstance(task, dict) or not task.get("id"):
            continue
        tid = task["id"]
        if tid not in node_of_task:
            errors.append({"code": "E-TASK-NODE",
                           "msg": "任务 %s 没有对应的 DAG 节点" % tid})
        for dep in task.get("depends_on") or []:
            if dep not in task_ids:
                errors.append({"code": "E-TASK-DEP",
                               "msg": "任务 %s 依赖了不存在的任务 %s" % (tid, dep)})
    return errors


def _critical_path(nodes, adj, order):
    if not order:
        return []
    dist = {nid: 0 for nid in nodes}
    prev = {}
    for cur in order:
        for nxt in adj[cur]:
            if dist[cur] + 1 > dist[nxt]:
                dist[nxt] = dist[cur] + 1
                prev[nxt] = cur
    end = max(dist, key=lambda k: dist[k])
    path = [end]
    while path[-1] in prev:
        path.append(prev[path[-1]])
    return list(reversed(path))


# --------------------------------------------------------------------------
# 需求 → DAG 草稿生成器（模板驱动）
#
# 能力域词典、风险词和流程骨架都放在 templates/*.json，生成器只负责"解释执行"
# 模板：同一个函数换模板就换一整套 SE 流程，不需要改代码。原型阶段不接大模型，
# 确定性生成保证同样的需求每次得到同样的图、且一定能通过 validate_workflow。
# 接 LLM 时只需替换本函数的返回（同样返回 definition + trace），下游的
# 编辑/校验/发布/运行都不用改。
# --------------------------------------------------------------------------
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
_TEMPLATE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# 阶段种类 → 节点 type；未列出的按 agent 处理
_KIND_TYPES = {"start": "start", "agent": "agent", "fanout": "agent",
               "risk_agent": "agent", "branch": "agent", "join": "agent",
               "condition": "condition", "approval": "approval", "end": "end"}
# 允许从模板阶段声明直接透传到节点的字段
_NODE_PASSTHROUGH = ("out_schema", "expression", "join", "timeout")


class TemplateError(Exception):
    """模板缺失、损坏或声明非法。"""


def template_names():
    try:
        files = os.listdir(TEMPLATES_DIR)
    except OSError:
        return []
    return sorted(f[:-5] for f in files if f.endswith(".json") and not f.startswith("."))


def load_template(name="default"):
    name = (name or "default").strip() or "default"
    if not _TEMPLATE_NAME_RE.match(name):
        raise TemplateError("模板名不合法：%s" % name)
    path = os.path.join(TEMPLATES_DIR, name + ".json")
    if not os.path.isfile(path):
        raise TemplateError("找不到模板 %s（可用：%s）"
                            % (name, "、".join(template_names()) or "无"))
    try:
        with open(path, "r", encoding="utf-8") as f:
            tpl = json.load(f)
    except Exception as exc:
        raise TemplateError("模板 %s 解析失败：%s" % (name, exc))
    if not isinstance(tpl, dict):
        raise TemplateError("模板 %s 结构非法：顶层必须是对象" % name)
    if not isinstance(tpl.get("pipeline"), list) or not tpl["pipeline"]:
        raise TemplateError("模板 %s 缺少 pipeline 阶段列表" % name)
    for step in tpl["pipeline"]:
        if not isinstance(step, dict) or step.get("kind") not in _KIND_TYPES:
            raise TemplateError("模板 %s 存在非法阶段：%r" % (name, step))
        if not step.get("name") and step.get("kind") != "fanout":
            raise TemplateError("模板 %s 的阶段缺少 name：%r" % (name, step))
    tpl["id"] = tpl.get("id") or name
    tpl["name"] = tpl.get("name") or name
    tpl["domains"] = tpl.get("domains") or []
    tpl["risk_words"] = tpl.get("risk_words") or []
    return tpl


def list_templates():
    """返回可用模板的摘要，供前端/接口展示可切换的流程形态。"""
    out = []
    for name in template_names():
        try:
            tpl = load_template(name)
        except TemplateError as exc:
            out.append({"id": name, "name": name, "error": str(exc), "stages": 0})
            continue
        out.append({"id": tpl["id"], "name": tpl["name"],
                    "description": tpl.get("description", ""),
                    "stages": len(tpl["pipeline"]),
                    "domains": len(tpl["domains"]),
                    "concurrency_limit": tpl.get("concurrency_limit", 3)})
    return out


def _match_domains(tpl, text):
    matched = []
    for d in tpl["domains"]:
        words = [w for w in (d.get("keywords") or []) if w]
        if any(w in text for w in words):
            matched.append(d)
    return matched


def _walk_pipeline(tpl, text, trace, segment, risk=False):
    """按模板 pipeline 逐阶段建图，返回 (nodes, edges, index, info)。

    fanout 阶段是唯一可插拔的一段：由 segment 决定这一段放哪些节点、返回新的
    前沿节点。领域骨架（关键词命中）和任务清单（spec 分解）走同一套解释器，
    所以换模板就换流程形状，换规格就换任务内容。
    """
    nodes, edges, index = [], [], {}

    def link(src, dst, label=None):
        for e in edges:
            if e["source"] == src and e["target"] == dst:
                return
        edge = {"source": src, "target": dst}
        if label:
            edge["label"] = label
        edges.append(edge)
        index[dst]["deps"].append(src)

    def add(step, label, role, deps):
        kind = step["kind"]
        node = {"id": "N%d" % (len(nodes) + 1), "type": _KIND_TYPES[kind],
                "role": role, "name": label, "deps": [],
                "max_retries": step.get("max_retries", 2)}
        for key in _NODE_PASSTHROUGH:
            if key in step:
                node[key] = step[key]
        if kind == "start":
            node["description"] = text
        nodes.append(node)
        index[node["id"]] = node
        for src in deps:
            link(src, node["id"])
        return node["id"]

    heads, branch_ids, branch_map, cond = [], [], {}, None
    fanout_line = None

    for step in tpl["pipeline"]:
        kind = step["kind"]
        if kind == "start":
            heads = [add(step, step["name"], step.get("role", "system"), [])]
        elif kind == "fanout":
            heads, fanout_line = segment(step, heads, add, link, index)
        elif kind == "condition":
            heads = [add(step, step["name"], step.get("role", "qa"), heads)]
            cond = heads[0]
        elif kind == "branch":
            if cond is None:
                raise TemplateError("模板 %s 的 branch 阶段前必须先有 condition" % tpl["id"])
            new_id = add(step, step["name"], step.get("role", "qa"), [])
            link(cond, new_id, step.get("label"))
            branch_ids.append(new_id)
            branch_map[step.get("label") or "else"] = index[new_id]["name"]
        elif kind == "join":
            deps = branch_ids or heads
            new_id = add(step, step["name"], step.get("role", "qa"), [])
            for src in deps:
                link(src, new_id)
            if len(deps) > 1:
                index[new_id]["join"] = step.get("join", "any")
            heads, branch_ids = [new_id], []
        elif kind == "risk_agent":
            if not risk:
                continue
            heads = [add(step, step["name"], step.get("role", "qa"), heads)]
        else:  # agent / approval / end
            heads = [add(step, step["name"], step.get("role", "agent"), heads)]

    return nodes, edges, index, {"cond": cond, "branch_map": branch_map,
                                 "fanout_line": fanout_line}


def generate_workflow(requirement, name=None, template="default"):
    """按模板把一句需求展开成可执行的 DAG 草稿，并返回生成依据。"""
    text = (requirement or "").strip()
    tpl = load_template(template)
    trace = []

    matched = _match_domains(tpl, text)
    risk = [w for w in tpl["risk_words"] if w and w in text]
    trace.append("解析需求：文本 %d 字，识别能力域 %d 个%s"
                 % (len(text), len(matched),
                    "（" + "、".join(d["label"] for d in matched) + "）" if matched
                    else "（未命中词典，走通用骨架）"))
    # 风险词是否有落点，取决于是这个模板里配了 risk_agent 阶段
    has_risk_stage = any(s["kind"] == "risk_agent" for s in tpl["pipeline"])
    if risk:
        trace.append("风险识别：命中 " + "、".join(risk)
                     + ("，追加安全评审与上线审批" if has_risk_stage
                        else "（本模板无安全评审阶段，仅记录风险）"))
    else:
        trace.append("风险识别：未命中高风险词"
                     + ("，仍保留上线审批节点" if has_risk_stage else ""))
    trace.append("套用模板「%s」：%d 个阶段，%d 个能力域，并发上限 %s"
                 % (tpl["name"], len(tpl["pipeline"]), len(tpl["domains"]),
                    tpl.get("concurrency_limit", 3)))

    def segment(step, heads, add, link, index):
        items = matched[:max(1, int(step.get("max") or 3))]
        if not items:
            items = [step.get("fallback") or {"name": "核心功能实现", "role": "coder"}]
        made = []
        for item in items:
            label = item.get("name") or (item.get("label", "功能") + step.get("suffix", ""))
            made.append(add(step, label, item.get("role") or step.get("role", "coder"), heads))
        return made, ("并行实现：" + "、".join(index[i]["name"] for i in made)
                      + "（同一层，受并发上限约束）")

    nodes, edges, index, info = _walk_pipeline(tpl, text, trace, segment, risk=bool(risk))
    # 模板节点也要有分组，否则画布上只剩任务节点有色框
    annotate_parts(nodes)
    fanout_line = info["fanout_line"]
    cond, branch_map = info["cond"], info["branch_map"]

    if fanout_line:
        trace.append(fanout_line)
    if cond is not None and branch_map:
        trace.append("质量门：%s → %s"
                     % (index[cond].get("expression", "条件"),
                        "，".join("%s 走 %s" % (k, v) for k, v in branch_map.items())))
    trace.append("收尾：" + " → ".join(n["name"] for n in nodes[-3:]))
    trace.append("共生成 %d 个节点 / %d 条边" % (len(nodes), len(edges)))

    title = (name or "").strip()
    if not title:
        flat = " ".join(text.split())
        title = (flat[:16] + "…") if len(flat) > 16 else (flat or "未命名需求")
        title = title + " · 工作流"
    definition = {
        "workflow_id": "",
        "name": title,
        "version": "v0+草稿",
        "description": text or "由需求生成的工作流草稿",
        "requirement": text,
        "concurrency_limit": tpl.get("concurrency_limit", 3),
        "template": tpl["id"],
        "nodes": nodes,
        "edges": edges,
    }
    return definition, {"trace": trace, "matched": [d["label"] for d in matched],
                        "risk": risk, "nodes": len(nodes), "edges": len(edges),
                        "template": tpl["id"]}


TASK_ROLE_TO_AGENT = {
    "架构师": "architect", "前端": "coder", "后端": "coder",
    "测试": "tester", "评审": "reviewer", "运维": "coder",
}


def plan_from_tasks(spec, tasks, template="default"):
    """任务清单 → 可执行 DAG（编排入口）。

    模板负责前后两段流程：起始 / PM / 架构，以及质量门、合并、审批、收尾；
    fanout 那一段换成由 tasks[].depends_on 决定的真实任务子图。规格里有多少
    模块、模块之间怎么依赖，画出来就是什么样——同一模板下不同规格得到不同结构。
    """
    tpl = load_template(template)
    text = (spec.get("summary") or spec.get("body") or spec.get("name") or "").strip()
    haystack = " ".join([text, spec.get("body") or ""])
    risk = [w for w in tpl["risk_words"] if w and w in haystack]
    trace = ["读取规格「%s」：%d 个模块 / %d 条约束 → %d 条任务"
             % (spec.get("name") or spec.get("id"),
                len(spec.get("modules") or []),
                len(spec.get("constraints") or []), len(tasks))]

    node_of = {}

    def segment(step, heads, add, link, index):
        for t in tasks:
            role = TASK_ROLE_TO_AGENT.get(t.get("role") or "", "coder")
            nid = add(step, "%s %s" % (t["id"], t["title"]), role, [])
            node = index[nid]
            node["task_id"] = t["id"]
            node["module"] = t.get("module")
            node["spec_ref"] = t.get("spec_ref")
            node["priority"] = t.get("priority")
            node["estimate"] = t.get("estimate")
            node["acceptance"] = list(t.get("acceptance") or [])
            node["acceptance_sources"] = list(t.get("acceptance_sources") or [])
            node["kind"] = t.get("kind")
            node["part"] = part_of_task(t)
            node["part_label"] = part_label(node["part"])
            node["ai"] = (t.get("ai") or "").strip()
            node["spec_revision"] = t.get("spec_revision")
            node["spec_hash"] = t.get("spec_hash")
            node["out_schema"] = t.get("out_schema") or step.get("out_schema")
            node_of[t["id"]] = nid
        for t in tasks:
            deps = [node_of[d] for d in (t.get("depends_on") or []) if d in node_of]
            for src in (deps or heads):
                link(src, node_of[t["id"]])
        depended = set()
        for t in tasks:
            depended.update(t.get("depends_on") or [])
        sinks = [node_of[t["id"]] for t in tasks if t["id"] not in depended]
        trace.append("依赖连边：%d 条任务、%d 条依赖关系，%d 条任务汇入下游流程"
                     % (len(tasks),
                        sum(len(t.get("depends_on") or []) for t in tasks), len(sinks)))
        return sinks, ("任务并行：%d 条任务中 %d 条无前置依赖，可同时开工"
                       % (len(tasks),
                          len([t for t in tasks if not t.get("depends_on")])))

    nodes, edges, index, info = _walk_pipeline(tpl, text, trace, segment, risk=bool(risk))
    annotate_parts(nodes)
    if info["fanout_line"]:
        trace.append(info["fanout_line"])
    gate = info["cond"]
    if gate is not None and info["branch_map"]:
        trace.append("质量门：%s → %s"
                     % (index[gate].get("expression", "条件"),
                        "，".join("%s 走 %s" % (k, v) for k, v in info["branch_map"].items())))
    if risk:
        trace.append("风险识别：命中 " + "、".join(risk) + "，追加安全评审")
    trace.append("收尾：" + " → ".join(n["name"] for n in nodes[-3:]))
    trace.append("共编排 %d 个节点 / %d 条边" % (len(nodes), len(edges)))

    title = spec.get("name") or "未命名规格"
    definition = {
        "workflow_id": "",
        "name": title + " · 任务编排",
        "version": "v0+草稿",
        "description": text or "由设计规格编排的工作流草稿",
        "requirement": text,
        "spec_id": spec.get("id"),
        "spec_revision": int(spec.get("revision") or 1),
        "spec_hash": spec.get("content_hash") or _spec_hash(spec),
        "spec_snapshot": _clone(spec),
        "tasks": tasks,
        "concurrency_limit": tpl.get("concurrency_limit", 3),
        "template": tpl["id"],
        "nodes": nodes,
        "edges": edges,
    }
    return definition, {"trace": trace, "risk": risk, "tasks": len(tasks),
                        "nodes": len(nodes), "edges": len(edges), "template": tpl["id"]}


# --------------------------------------------------------------------------
# 运行归档：历史查看
# --------------------------------------------------------------------------
class RunArchive:
    """保存已结束的运行快照，供历史回看（保留最近 20 次）。"""

    LIMIT = 20

    def __init__(self, path):
        self.lock = threading.RLock()
        self.path = path
        self.runs = []
        self._load()

    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                self.runs = json.load(f)
        except Exception:
            self.runs = []

    def _flush(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.runs, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def add(self, snap):
        with self.lock:
            run = snap.get("run", {})
            item = {
                "run_id": run.get("run_id"),
                "workflow_id": run.get("workflow_id"),
                "workflow_name": run.get("workflow_name"),
                "workflow_version": run.get("workflow_version"),
                "requirement": run.get("requirement", ""),
                "status": run.get("status"),
                "started_at": run.get("started_at"),
                "finished_at": run.get("finished_at"),
                "ended_ts": time.time(),
                "elapsed": run.get("elapsed"),
                "stats": run.get("stats", {}),
                "nodes": snap.get("nodes", []),
                "edges": snap.get("edges", []),
                "events": snap.get("events", []),
                "definition": snap.get("workflow", {}),
            }
            self.runs = [r for r in self.runs if r.get("run_id") != item["run_id"]]
            self.runs.insert(0, item)
            self.runs = self.runs[:self.LIMIT]
            self._flush()

    def list(self):
        with self.lock:
            return [{
                "run_id": r.get("run_id"),
                "workflow_name": r.get("workflow_name"),
                "workflow_version": r.get("workflow_version"),
                "requirement": r.get("requirement", ""),
                "status": r.get("status"),
                "started_at": r.get("started_at"),
                "ended_ts": r.get("ended_ts"),
                "elapsed": r.get("elapsed"),
                "stats": r.get("stats", {}),
                "node_count": len(r.get("nodes", [])),
                "event_count": len(r.get("events", [])),
            } for r in self.runs]

    def get(self, run_id):
        with self.lock:
            for r in self.runs:
                if r.get("run_id") == run_id:
                    return _clone(r)
            return None


# --------------------------------------------------------------------------
# 工作流定义仓库：草稿 + 不可变发布版本
# --------------------------------------------------------------------------
def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _clone(obj):
    return json.loads(json.dumps(obj))


def _spec_hash(spec):
    """规格内容指纹：不包含 ID、时间和 revision 等元数据。"""
    payload = {
        "name": spec.get("name", ""),
        "summary": spec.get("summary", ""),
        "body": spec.get("body", ""),
        "modules": spec.get("modules", []),
        "constraints": spec.get("constraints", []),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class Store:
    """保存工作流草稿与已发布版本。

    原型阶段用单个 JSON 文件落盘（原子替换），接口与后续换 SQLite 时保持一致：
    草稿可反复修改，发布后的版本不可变；运行只引用已发布版本或当前草稿的快照。
    """

    def __init__(self, path=STORE_PATH):
        self.lock = threading.RLock()
        self.path = path
        self.workflows = {}
        self._load()

    # ---- 持久化 ----
    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                self.workflows = json.load(f)
        except Exception:
            self.workflows = {}
            self._seed()
            self._flush()

    def _seed(self):
        base = _clone(WORKFLOW)
        base["version"] = "v1"
        wid = base["workflow_id"]
        self.workflows[wid] = {
            "id": wid,
            "name": base["name"],
            "requirement": base.get("requirement", ""),
            "versions": [{"version": "v1", "created_at": _now_iso(),
                          "note": "初始版本", "definition": base}],
            "draft": {"definition": _clone(base), "base_version": "v1",
                      "dirty": False, "updated_at": _now_iso()},
        }

    def _flush(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.workflows, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    # ---- 查询 ----
    def default_id(self):
        with self.lock:
            return next(iter(self.workflows), None)

    def list(self):
        with self.lock:
            return [{
                "id": w["id"], "name": w["name"],
                "latest": w["versions"][-1]["version"] if w["versions"] else None,
                "versions": [v["version"] for v in w["versions"]],
                "dirty": bool(w["draft"].get("dirty")),
            } for w in self.workflows.values()]

    def get(self, wf_id):
        with self.lock:
            wf = self.workflows.get(wf_id)
            return _clone(wf) if wf else None

    def version(self, wf_id, label):
        with self.lock:
            wf = self.workflows.get(wf_id)
            if not wf:
                return None
            for v in wf["versions"]:
                if v["version"] == label:
                    return _clone(v["definition"])
            return None

    # ---- 写入 ----
    def save_draft(self, wf_id, definition, name=None, requirement=None, dirty=True):
        with self.lock:
            wf = self.workflows.get(wf_id)
            if not wf:
                return None, "工作流不存在"
            base = wf["draft"].get("base_version") or "v0"
            definition["workflow_id"] = wf_id
            definition["version"] = base + "+草稿"
            wf["draft"] = {"definition": definition, "base_version": base,
                           "dirty": dirty, "updated_at": _now_iso()}
            if name:
                wf["name"] = name
            if requirement is not None:
                wf["requirement"] = requirement
                definition["requirement"] = requirement
            self._flush()
            return _clone(wf["draft"]), None

    def publish(self, wf_id, note=""):
        with self.lock:
            wf = self.workflows.get(wf_id)
            if not wf:
                return None, "工作流不存在"
            label = "v%d" % (len(wf["versions"]) + 1)
            definition = _clone(wf["draft"]["definition"])
            definition["workflow_id"] = wf_id
            definition["version"] = label
            wf["versions"].append({"version": label, "created_at": _now_iso(),
                                   "note": note or "发布", "definition": definition})
            wf["draft"] = {"definition": _clone(definition), "base_version": label,
                           "dirty": False, "updated_at": _now_iso()}
            self._flush()
            return {"version": label, "definition": definition}, None

    def create(self, name, definition):
        with self.lock:
            wid = "wf_" + uuid.uuid4().hex[:8]
            definition["workflow_id"] = wid
            definition["version"] = "v0+草稿"
            definition.setdefault("name", name)
            self.workflows[wid] = {
                "id": wid, "name": name, "versions": [],
                "draft": {"definition": definition, "base_version": "v0",
                          "dirty": True, "updated_at": _now_iso()},
            }
            self._flush()
            return self.get(wid)


# --------------------------------------------------------------------------
# 设计规格仓库：SE 流程的入口（spec → tasks → DAG）
#
# 规格回答"要做什么"，模板回答"按什么流程做"，编排器把两者合成一张 DAG。
# 规格与工作流分开存：同一份规格可以被不同模板反复编排，产物各自成为草稿。
# --------------------------------------------------------------------------
SPEC_MODULE_PRIORITIES = ("P0", "P1", "P2")


class SpecError(ValueError):
    """规格字段不合法；message 直接回给前端。"""


def _lines(value):
    """把数组 / 多行文本统一成"去空行、去空白"的字符串列表。"""
    if value is None:
        return []
    if isinstance(value, str):
        parts = [value]
    elif isinstance(value, (list, tuple)):
        parts = [v for v in value if v is not None]
    else:
        parts = [value]
    out = []
    for item in parts:
        text = item if isinstance(item, str) else str(item)
        for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = line.strip()
            if line:
                out.append(line)
    return out


def normalize_spec(body, spec_id=None):
    """把前端提交的规格整理成规范结构；不合法时抛 SpecError。"""
    name = (body.get("name") or "").strip()
    if not name:
        raise SpecError("请填写规格名称")
    if len(name) > 80:
        raise SpecError("规格名称太长（最多 80 字）")

    raw_modules = body.get("modules") or []
    if not isinstance(raw_modules, list):
        raise SpecError("modules 必须是数组")
    if len(raw_modules) > 20:
        raise SpecError("一份规格最多 20 个模块")
    modules = []
    used_ids = set()
    for i, m in enumerate(raw_modules, 1):
        if isinstance(m, str):
            m = {"name": m}
        if not isinstance(m, dict):
            raise SpecError("第 %d 个模块格式不正确" % i)
        mname = (m.get("name") or "").strip()
        if not mname:
            raise SpecError("第 %d 个模块缺少名称" % i)
        if len(mname) > 60:
            raise SpecError("第 %d 个模块名称太长（最多 60 字）" % i)
        module_id = (m.get("id") or "").strip()
        if not re.match(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$", module_id) or module_id in used_ids:
            module_id = "M%d" % i
            suffix = 2
            while module_id in used_ids:
                module_id = "M%d-%d" % (i, suffix)
                suffix += 1
        used_ids.add(module_id)
        priority = (m.get("priority") or "P1").strip().upper()
        if priority not in SPEC_MODULE_PRIORITIES:
            priority = "P1"
        modules.append({
            "id": module_id,
            "name": mname,
            "goal": (m.get("goal") or "").strip()[:200],
            "priority": priority,
            "acceptance": _lines(m.get("acceptance"))[:10],
            # 文档里的「模块划分表」带依赖列：保留下来，分解任务时据此排先后
            "deps": [d for d in _lines(m.get("deps"))
                     if re.match(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$", d)][:20],
        })

    text = (body.get("body") or "").strip()
    if not text and not modules:
        raise SpecError("设计规格至少要写正文，或者列一个模块")

    summary = (body.get("summary") or "").strip()[:200]
    if not summary:
        first = next((ln.strip() for ln in text.split("\n") if ln.strip()), "")
        summary = first[:60] or name

    return {
        "id": spec_id,
        "name": name,
        "summary": summary,
        "body": text,
        "modules": modules,
        "constraints": _lines(body.get("constraints"))[:20],
    }


class SpecStore:
    """设计规格落盘仓库（data/specs.json）。

    与工作流仓库分开：规格是输入，工作流是产物；改规格不影响已发布的流程图。
    """

    def __init__(self, path=SPEC_PATH):
        self.lock = threading.RLock()
        self.path = path
        self.specs = {}
        self._load()

    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                self.specs = json.load(f)
            for spec in self.specs.values():
                spec.setdefault("revision", 1)
                spec["content_hash"] = _spec_hash(spec)
        except Exception:
            self.specs = {}
            self._seed()
            self._flush()

    def _seed(self):
        spec = normalize_spec({
            "name": "登录功能设计规格",
            "summary": WORKFLOW.get("requirement", ""),
            "body": "为系统实现用户名密码登录：支持记住登录态、失败重试锁定、登录日志审计。",
            "modules": [
                {"name": "账号与登录", "goal": "用户名密码登录、记住登录态", "priority": "P0",
                 "acceptance": ["连续 5 次输错锁定 15 分钟", "登录态可保持 7 天"]},
                {"name": "安全审计", "goal": "登录日志与异常告警", "priority": "P1",
                 "acceptance": ["每次登录写入审计日志"]},
            ],
            "constraints": ["不新增第三方依赖", "密码只存哈希"],
        }, spec_id="SP1")
        spec["created_at"] = _now_iso()
        spec["updated_at"] = spec["created_at"]
        spec["revision"] = 1
        spec["content_hash"] = _spec_hash(spec)
        self.specs["SP1"] = spec

    def _flush(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.specs, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    # ---- 查询 ----
    def list(self):
        with self.lock:
            items = sorted(self.specs.values(), key=lambda s: s.get("updated_at") or "",
                           reverse=True)
            return [{
                "id": s["id"], "name": s["name"], "summary": s.get("summary", ""),
                "module_count": len(s.get("modules", [])),
                "modules": [m["name"] for m in s.get("modules", [])],
                "revision": s.get("revision", 1),
                "content_hash": s.get("content_hash") or _spec_hash(s),
                "updated_at": s.get("updated_at"),
            } for s in items]

    def get(self, spec_id):
        with self.lock:
            spec = self.specs.get(spec_id)
            return _clone(spec) if spec else None

    def _next_id(self):
        n = len(self.specs) + 1
        while ("SP%d" % n) in self.specs:
            n += 1
        return "SP%d" % n

    # ---- 写入 ----
    def create(self, body):
        spec = normalize_spec(body)
        with self.lock:
            spec["id"] = self._next_id()
            spec["created_at"] = spec["updated_at"] = _now_iso()
            spec["revision"] = 1
            spec["content_hash"] = _spec_hash(spec)
            self.specs[spec["id"]] = spec
            self._flush()
            return _clone(spec)

    def update(self, spec_id, body):
        spec = normalize_spec(body, spec_id=spec_id)
        with self.lock:
            old = self.specs.get(spec_id)
            if not old:
                return None, "规格不存在"
            spec["created_at"] = old.get("created_at") or _now_iso()
            spec["updated_at"] = _now_iso()
            spec["revision"] = int(old.get("revision") or 1) + 1
            spec["content_hash"] = _spec_hash(spec)
            self.specs[spec_id] = spec
            self._flush()
            return _clone(spec), None


STORE = Store()
ARCHIVE = RunArchive(os.path.join(DATA_DIR, "runs.json"))
SPECS = SpecStore()


# --------------------------------------------------------------------------
# PM Agent：设计规格 → 任务清单（T3）
#
# 输入是规格里的模块、验收条款与全局约束，输出是可直接编排的 tasks[]。
# 这里是确定性的规则式分解：可解释、可复现、不依赖外部模型；将来接真实模型
# 时只换 decompose_spec 的实现，tasks 的字段契约不变。
# --------------------------------------------------------------------------
TASK_ROLE_RULES = (
    ("前端", ("前端", "页面", "界面", "交互", "组件", "样式", "小程序", "客户端", "表单")),
    ("后端", ("接口", "服务", "数据库", "存储", "后端", "api", "并发", "幂等", "算法", "回调", "任务")),
    ("测试", ("测试", "用例", "回归", "压测")),
    ("运维", ("部署", "监控", "运维", "容量", "扩容")),
)
TASK_PRIORITY_ESTIMATE = {"P0": 5, "P1": 3, "P2": 2}
TASK_DESIGN_WORDS = ("接口", "数据模型", "数据表", "流程", "架构", "算法", "协议",
                     "状态机", "对账", "回调")


def _task_role(text):
    """按任务描述猜执行角色；猜不到就归后端。"""
    low = (text or "").lower()
    for role, words in TASK_ROLE_RULES:
        if any(w in low for w in words):
            return role
    return "后端"


def _needs_design(module):
    """P0 模块，或描述里带接口/数据模型这类词的模块，单独出一条设计任务。"""
    if module.get("priority") == "P0":
        return True
    text = ((module.get("name") or "") + " " + (module.get("goal") or "")).lower()
    return any(w in text for w in TASK_DESIGN_WORDS)


def _task_depth(tasks):
    """最长依赖链层数；顺带找出没有后继的收尾任务。"""
    by_id = {t["id"]: t for t in tasks}
    memo = {}

    def depth(tid):
        if tid in memo:
            return memo[tid]
        task = by_id.get(tid)
        if not task:
            return 0
        memo[tid] = 1 + max([depth(d) for d in task["depends_on"]] or [0])
        return memo[tid]

    return max([depth(t["id"]) for t in tasks] or [0])


# 五段式工程阶段：设计 → 编码 → 测试 → 评审 → 交付
PHASES = ("设计", "编码", "测试", "评审", "交付")
PHASE_OF = {
    "foundation": "设计", "design": "设计", "implementation": "编码",
    "verification": "测试", "review": "评审", "integration": "测试", "delivery": "交付",
}


def _module_slug(module):
    """给文件路径用的稳定短名：优先 ASCII，中文名退回 M1/M2。"""
    raw = (module.get("name") or "").strip().lower()
    ascii_part = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    if len(ascii_part) >= 3:
        return ascii_part[:24]
    return (module.get("id") or "m1").lower()


def _impl_files(module, role):
    """按角色给出该模块的落地文件——写代码的 Agent 需要明确的落点。"""
    slug = _module_slug(module)
    if role == "前端":
        return ["src/%s/view.js" % slug, "src/%s/style.css" % slug, "tests/test_%s_ui.js" % slug]
    if role == "运维":
        return ["deploy/%s.md" % slug, "src/%s/healthcheck.py" % slug]
    return ["src/%s/service.py" % slug, "src/%s/models.py" % slug, "tests/test_%s.py" % slug]


def _impl_how(module, role, acceptance):
    slug = _module_slug(module)
    if role == "前端":
        base = ("原生 JS + SVG 直接操作 DOM，不引入构建链；先写静态结构与样式，"
                "再接 src/%s/service.py 暴露的接口" % slug)
    elif role == "运维":
        base = "写部署说明与健康检查脚本，用 python 跑一次自检"
    else:
        base = ("Python 标准库实现：models.py 放数据模型与校验，service.py 放业务逻辑与接口路由，"
                "不引第三方依赖")
    if acceptance:
        base += "；完成后按验收条款逐条自测（取第一条条款写一条用例）"
    return base


def _impl_what(module, acceptance):
    name = module.get("name") or "模块"
    goal = (module.get("goal") or "").strip()
    text = "交付「%s」的可运行实现" % name
    if goal:
        text += "：%s" % goal[:80]
    if acceptance:
        text += "；满足 %d 条验收（%s）" % (
            len(acceptance), "；".join(str(a)[:30] for a in acceptance[:2]))
    return text


def _steps_for(kind, module, role, acceptance):
    """把任务拆成 2-4 个子步骤：每一步都写清"做什么 + 怎么做"。"""
    slug = _module_slug(module)
    if kind == "foundation":
        return [{"title": "建立目录与运行入口", "how": "创建 src/、tests/ 目录与 README，给出启动命令"},
                {"title": "建立测试基线", "how": "python -m pytest -q 能跑起来（哪怕只有 1 条用例）"}]
    if kind == "design":
        return [{"title": "定义接口与数据结构", "how": "写 docs/design-%s.md：接口表、字段表、错误码" % slug},
                {"title": "对齐验收条款", "how": "逐条映射验收标准到接口或数据结构"}]
    if kind == "implementation":
        files = _impl_files(module, role)
        steps = [{"title": "实现数据模型", "how": "写 %s" % files[1] if len(files) > 1 else "定义数据结构"}]
        steps.append({"title": "实现业务逻辑与接口", "how": "写 %s" % files[0]})
        if len(files) > 2:
            steps.append({"title": "补最小可运行用例", "how": "写 %s 并跑通" % files[-1]})
        if acceptance:
            steps.append({"title": "按验收条款自测", "how": "逐条对照：%s" %
                          str(acceptance[0])[:40]})
        return steps
    if kind == "verification":
        return [{"title": "设计用例矩阵", "how": "正常路径 / 边界 / 异常各至少 1 条"},
                {"title": "落地测试文件", "how": "写 tests/test_%s.py，用 pytest 断言" % slug},
                {"title": "跑测试并记录证据", "how": "python -m pytest -q，把通过/失败数写进产物"}]
    if kind == "review":
        return [{"title": "对照验收条款检查实现", "how": "逐条读代码，标出未满足项"},
                {"title": "检查代码质量", "how": "重复逻辑、异常处理、硬编码、越界与注入风险"},
                {"title": "给出结论", "how": "阻塞问题必须列出，最后给 pass/fail"}]
    if kind in ("integration", "delivery"):
        return [{"title": "装配所有模块", "how": "确认入口能同时加载各模块的接口"},
                {"title": "端到端跑主流程", "how": "python -m pytest -q 跑一遍全量用例"},
                {"title": "整理交付清单", "how": "列出产出文件、启动方式、遗留风险"}]
    return [{"title": "执行任务", "how": "按验收标准完成"}]


ENDPOINT_RE = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE)\s+(/api/[\w/{}\-.:]+)", re.I)
TABLE_HEAD_RE = re.compile(r"^\*\*\s*([A-Za-z_][\w]*)\s+([^*]{1,20}?表)\s*\*\*", re.M)


def _bigrams(text):
    text = re.sub(r"[\s，,。.;；:：()（）\[\]/、*_`|-]+", "", text or "")
    return {text[i:i + 2] for i in range(len(text) - 1)}


BOLD_HEAD_RE = re.compile(r"^\*\*\s*([^*]{1,30}?)\s*\*\*\s*$")


def _doc_profile(spec):
    """从规格正文里抽"能变成任务"的证据：接口、数据表、模块细节段。

    这是"节点会随文档变"的关键：没有接口的规格不会凭空长出接口任务；
    文档里写了几张表、几个接口，任务里就能看到对应的工作项。
    """
    body = spec.get("body") or ""
    endpoints = []
    recent = ""
    for line in body.splitlines():
        mb = BOLD_HEAD_RE.match(line.strip())
        if mb:
            recent = mb.group(1)
        for m in ENDPOINT_RE.finditer(line):
            path = m.group(2).rstrip(".,;")
            item = (m.group(1).upper(), path)
            if item not in [(e[0], e[1]) for e in endpoints]:
                endpoints.append(item + (recent or path.split("/")[-1],))
    tables = []
    for m in TABLE_HEAD_RE.finditer(body):
        tables.append((m.group(1), m.group(2)))
    sections = {}
    for level, head, lines in _doc_sections(body):
        sections[head] = "\n".join(lines)[:4000]
    return {"endpoints": endpoints[:12], "tables": tables[:10], "sections": sections}


def _doc_sections(text):
    """(level, title, lines) —— 只需要标题与紧随其后的正文。"""
    out, cur = [], None
    for line in text.splitlines():
        m = re.match(r"^(#{1,6})\s+(.*\S)\s*$", line)
        if m:
            cur = (len(m.group(1)), m.group(2).strip(), [])
            out.append(cur)
        elif cur is not None:
            cur[2].append(line)
    return out


def _owner_of(label, modules):
    """这个接口/表属于哪个模块：模块名命中权重 2，目标描述命中权重 1。"""
    if not modules:
        return None
    lb = _bigrams(label)
    best, score = None, 0
    for m in modules:
        sc = (2 * len(lb & _bigrams(m.get("name") or ""))
              + len(lb & _bigrams(m.get("goal") or "")))
        if sc > score:
            best, score = m, sc
    return best if score >= 1 else None


def _claim(label, modules, used):
    """一个接口/表只能归一个模块：已经认领过的不再重复分给别的模块。"""
    owner = _owner_of(label, modules)
    if owner is None or owner["id"] in used:
        return None
    used.add(owner["id"])
    return owner


def _endpoint_label(method, path):
    return "%s %s" % (method, path)


def _step_target_file(task, step, step_index):
    files = [str(p).replace("\\", "/").strip() for p in (task.get("files") or []) if p]
    explicit = []
    for key in ("file", "path"):
        if step.get(key):
            explicit.append(str(step[key]).replace("\\", "/").strip())
    explicit.extend(str(p).replace("\\", "/").strip() for p in (step.get("files") or []) if p)
    description = " ".join(str(step.get(k) or "") for k in ("title", "how"))
    explicit.extend(re.findall(r"(?:[\w.-]+/)*[\w.-]+\.(?:py|js|ts|tsx|jsx|html|css|json|md|sql)", description))
    for path in explicit:
        if path in files:
            return path
    if files:
        return files[min(step_index, len(files) - 1)]
    return ""


def _workspace_path(ws_dir, rel):
    """把任务里的相对路径解析到运行工作区内；越界或路径非法时返回空串（不判断文件是否存在）。"""
    if not ws_dir or not rel:
        return ""
    root = os.path.realpath(ws_dir)
    parts = [p for p in str(rel).replace("\\", "/").split("/") if p not in ("", ".")]
    if not parts or any(p == ".." for p in parts) or re.match(r"^[A-Za-z]:", parts[0]):
        return ""
    target = os.path.realpath(os.path.join(root, *parts))
    if target != root and not target.startswith(root + os.sep):
        return ""
    return target


def _preview_cache_file(run_id, task_id, step_index, target):
    """一条子步骤的生成结果缓存路径：同一次运行、同一个子步骤只生成一次。"""
    key = "%s__%s__%s" % (task_id, step_index, str(target).replace("/", "_"))
    key = re.sub(r"[^\w.\-]", "_", key)
    run_key = re.sub(r"[^\w.\-]", "_", str(run_id or "draft"))
    return os.path.join(PREVIEW_CACHE_DIR, run_key, key)


def _attach_symbols(payload, code, target):
    """给代码预览响应挂上符号索引：前端据此按符号查看，同时保留整文件。"""
    try:
        info = symbol_index.split_symbols(code, target)
        syms = info.get("symbols") or []
        payload["symbols"] = syms
        payload["strategy"] = info.get("strategy") or "none"
        payload["symbol_confidence"] = info.get("confidence") or "none"
        payload["symbol_note"] = info.get("note") or ""
        payload["granularity"] = "symbol" if syms else "file"
    except Exception as exc:            # 切分失败不能挡住看代码
        payload["symbols"] = []
        payload["strategy"] = "none"
        payload["symbol_confidence"] = "none"
        payload["symbol_note"] = "符号切分失败：%s" % exc
        payload["granularity"] = "file"
    return payload


def _generate_step_code(task, step, target, ai_id=None):
    """模拟运行没有落盘文件时，按任务+子步骤让模型生成一次文件内容。"""
    conf = SETTINGS.spec(ai_id) if ai_id else SETTINGS.data
    if not (conf.get("base_url") and conf.get("api_key")):
        raise llm_client.LLMNotConfigured(
            "查看代码需要先在页面右上角「模型」配好 base_url 与 API Key，然后重新点一次")
    doing = step.get("what") or step.get("title") or ""
    how = step.get("how") or ""
    done = step.get("done_when") or ""
    acceptance = "；".join(str(a) for a in (task.get("acceptance") or [])[:4])
    messages = [
        {"role": "system", "content": "你是资深工程师。只输出目标文件的完整源码，"
                                      "不要解释、不要 Markdown 围栏。"},
        {"role": "user", "content":
            "目标文件：%s\n任务：%s\n这一步：%s\n怎么做：%s\n验收：%s\n完成标准：%s\n\n"
            "请给出可直接写入 %s 的完整代码。"
            % (target, task.get("title") or "", doing, how, acceptance, done, target)},
    ]
    finish = ""
    saw_reasoning = False
    for attempt in range(2):
        if attempt:
            # 第一次空返回时用更短的提示词再试一次：有的模型会因为上下文太长而不吐内容
            messages = [
                {"role": "system", "content": "只输出完整源码，不要任何解释。"},
                {"role": "user", "content": "文件：%s\n要求：%s\n%s\n直接输出文件内容。"
                                            % (target, doing, how)},
            ]
        reply = LLM.chat(messages, timeout=120, temperature=0.2,
                         max_tokens=8000, ai_id=ai_id)
        finish = reply.get("finish") or ""
        if reply.get("reasoning"):
            saw_reasoning = True
        code = (reply.get("content") or "").strip()
        code = re.sub(r"^```[a-zA-Z0-9_+-]*\s*\n", "", code)
        code = re.sub(r"\n```\s*$", "", code).strip()
        if code:
            return code
    raise llm_client.LLMError(
        "E-FORMAT", "模型返回了空内容（finish_reason=%s）%s。可以把「模型」里的 max_tokens 调大，"
                    "或换一个非思考模型再点一次。"
        % (finish or "unknown",
           "，只返回了思考内容、没有正文" if saw_reasoning else ""))


def decompose_spec(spec):
    """设计规格 → (tasks, trace, stats)：三级结构（阶段 → 任务 → 子步骤）。

    分解规则（可解释、可复现）：
      P1 设计：工程骨架打头；P0 或涉及接口/数据模型的模块追加「方案与接口设计」
      P2 编码：每个模块一条「功能实现」，写明目标文件、技术路线、验收条款
      P3 测试：每个模块一条「单元测试与回归」，要求 pytest 用例矩阵
      P4 评审：每个模块一条「代码评审」，对照验收条款给结论
      P5 交付：集成联调 + 交付清单，收所有分支
    每条任务都带 what（做什么）/ how（怎么做）/ files（落在哪）/ steps（子步骤），
    `plan_from_tasks` 再把它们变成 DAG 节点。
    """
    modules = [dict(m) for m in (spec.get("modules") or [])]
    constraints = list(spec.get("constraints") or [])
    trace = []

    if not modules:
        modules = [{"id": "M1", "name": spec.get("summary") or spec.get("name") or "整体功能",
                    "goal": "", "priority": "P1", "acceptance": []}]
        trace.append("规格没有列模块，按整体功能当作单模块处理")
    trace.append("读取规格「%s」：%d 个模块、%d 条全局约束"
                 % (spec.get("name") or spec.get("id"), len(modules), len(constraints)))

    tasks = []
    seq = [0]
    spec_rev = int(spec.get("revision") or 1)
    spec_fingerprint = spec.get("content_hash") or _spec_hash(spec)

    def add(title, module, role, priority, estimate, deps, acceptance=None,
            kind="implementation", out_schema="code-result-v1", acceptance_sources=None,
            source_label=None, what="", how="", files=None, steps=None, level=2,
            part=""):
        seq[0] += 1
        module_id = module.get("id") if module else ""
        module_name = module.get("name") if module else ""
        spec_ref = ("%s/%s" % (spec["id"], module_id)) if module else spec["id"]
        criteria = list(acceptance or [])
        source_refs = list(acceptance_sources or [])
        if len(source_refs) < len(criteria):
            fallback = source_label or "%s/derived/%s" % (spec_ref, kind)
            source_refs.extend([fallback] * (len(criteria) - len(source_refs)))
        phase = PHASE_OF.get(kind, "编码")
        task = {
            "id": "T%d" % seq[0],
            "title": title,
            "phase": phase,
            "phase_id": "P%d" % (PHASES.index(phase) + 1),
            "level": level,
            "what": what or title,
            "how": how or "按验收标准实现",
            "files": list(files or []),
            "steps": list(steps or _steps_for(kind, module or {}, role, criteria)),
            "module": module_name,
            "module_id": module_id,
            "role": role,
            "priority": priority,
            "estimate": estimate,
            "depends_on": list(deps),
            "acceptance": criteria,
            "acceptance_sources": source_refs[:len(criteria)],
            "spec_ref": spec_ref,
            "spec_revision": spec_rev,
            "spec_hash": spec_fingerprint,
            "kind": kind,
            "out_schema": out_schema,
            # 任务分组：分解时显式给，没给就按标题/kind 归一
            "part": part or part_of_task({"title": title, "kind": kind, "what": what}),
            "source": {
                "spec_id": spec["id"],
                "revision": spec_rev,
                "module_id": module_id,
                "module_name": module_name,
            },
        }
        tasks.append(task)
        return task

    skeleton = add("工程骨架与环境搭建", None, "后端", "P0", 1, [],
                   ["构建脚本可一键起服务", "目录结构与模板、规格存放就位"],
                   kind="foundation", part="skeleton",
                   what="把项目骨架立起来：目录、入口、测试基线",
                   how="建 src/、tests/ 目录与 README；确保 python -m pytest -q 能跑",
                   files=["README.md", "src/__init__.py", "tests/__init__.py"],
                   source_label="%s/derived/engineering-skeleton" % spec["id"])

    # 文档证据 → 归属模块（按接口/表的标题与模块名做字面匹配）
    profile = _doc_profile(spec)
    doc_eps, doc_tbls = {}, {}
    claimed = set()
    for method, path, label in profile["endpoints"]:
        owner = _owner_of(label + " " + path.replace("/", " ") + " "
                          + path.split("/")[-1], modules)
        if owner:
            doc_eps[label or path] = (method, path, owner["id"])
            claimed.add(owner["id"])
    claimed_tbl = set()
    for name, label in profile["tables"]:
        owner = _claim("%s %s" % (name, label), modules, claimed_tbl)
        if owner:
            doc_tbls["%s %s" % (name, label)] = (name, owner["id"])
    if doc_eps or doc_tbls:
        trace.append("文档证据：%d 个接口、%d 张表，按名称归到相关模块"
                     % (len(doc_eps), len(doc_tbls)))

    design_count = 0
    data_count = 0
    api_count = 0
    review_of = {}
    for m in modules:
        priority = m.get("priority") or "P1"
        acc = list(m.get("acceptance") or [])
        role = _task_role((m.get("name") or "") + " " + (m.get("goal") or ""))
        estimate = TASK_PRIORITY_ESTIMATE.get(priority, 3) + (1 if len(acc) >= 3 else 0)
        module_ref = "%s/%s" % (spec["id"], m["id"])
        acc_sources = ["%s/acceptance/%d" % (module_ref, i + 1) for i in range(len(acc))]

        deps = [skeleton["id"]]
        if _needs_design(m):
            design = add("「%s」方案与接口设计" % m["name"], m, "架构师", priority, 2, deps,
                         ["输出接口与数据结构说明", "与规格验收条款逐条对齐"],
                         kind="design", part="skeleton", out_schema="design-v1",
                         what="给出「%s」的接口、数据结构与错误码约定" % m["name"],
                         how="写 docs/design-%s.md：接口表、字段表、异常分支" % _module_slug(m),
                         files=["docs/design-%s.md" % _module_slug(m)],
                         acceptance_sources=["%s/derived/design" % module_ref,
                                              "%s/acceptance" % module_ref])
            design_count += 1
            deps = [design["id"]]

        # 文档证据：这个模块自己的接口与数据表 → 单独成任务（文档没写就不会有）
        slug = _module_slug(m)
        mine_eps = [(method, path) for (method, path, oid) in doc_eps.values()
                    if oid == m["id"]]
        mine_tbls = [name for (name, oid) in doc_tbls.values() if oid == m["id"]]
        if mine_tbls:
            data = add("「%s」数据模型与迁移" % m["name"], m, "后端", priority,
                       1 + (1 if len(mine_tbls) > 2 else 0), deps,
                       ["表结构与规格一致（%s）" % "、".join(mine_tbls[:3]),
                        "字段类型/约束/索引可落地，迁移脚本可重复执行"],
                       kind="implementation", part="data", out_schema="code-result-v1",
                       what="按规格建「%s」的数据模型：%s" % (m["name"], "、".join(mine_tbls)),
                       how="写 models；建表/改表脚本放 migrations/，含唯一键与外键",
                       files=["src/models/%s.py" % slug,
                              "migrations/001_%s.sql" % slug],
                       steps=[{"title": "建表 %s" % t, "how": "按规格字段表定义类型、约束与索引"}
                              for t in mine_tbls[:4]]
                             + [{"title": "迁移可重复执行",
                                 "how": "写 up/down 脚本并跑两遍，确认幂等"}],
                       acceptance_sources=["%s/derived/data-model" % module_ref])
            deps = [data["id"]]
            data_count += 1
        if mine_eps:
            api = add("「%s」接口实现" % m["name"], m, "后端", priority,
                      1 + (1 if len(mine_eps) > 2 else 0), deps,
                      ["%s 可调用且返回统一结构" % _endpoint_label(*mine_eps[0])]
                      + ["错误码与规格一致"],
                      kind="implementation", part="api", out_schema="code-result-v1",
                      what="实现「%s」的 %d 个接口：%s"
                           % (m["name"], len(mine_eps),
                              "、".join(_endpoint_label(*e) for e in mine_eps[:4])),
                      how="按规格的请求/响应与错误码实现，入参校验 + 鉴权拦截",
                      files=["src/api/%s.py" % slug],
                      steps=[{"title": _endpoint_label(*e), "how": "请求校验 → 业务处理 → 统一返回体"}
                             for e in mine_eps[:4]]
                            + [{"title": "接口自测",
                                "how": "用 curl 或 unittest 打通成功、参数错误、未登录三条分支"}],
                      acceptance_sources=["%s/derived/api" % module_ref])
            deps = [api["id"]]
            api_count += 1

        impl = add("「%s」功能实现" % m["name"], m, role, priority, estimate, deps, acc,
                   kind="implementation", part="feature", out_schema="code-result-v1",
                   what=_impl_what(m, acc), how=_impl_how(m, role, acc),
                   files=_impl_files(m, role), acceptance_sources=acc_sources)

        test = add("「%s」单元测试与回归" % m["name"], m, "测试", priority,
                   1 + (1 if len(acc) >= 3 else 0), [impl["id"]],
                   acc or ["按规格正文逐条验证"],
                   kind="verification", part="test", out_schema="test-v1",
                   what="为「%s」补用例并跑通：正常 / 边界 / 异常" % m["name"],
                   how="pytest：tests/test_%s.py，命令 python -m pytest -q" % _module_slug(m),
                   files=["tests/test_%s.py" % _module_slug(m)],
                   acceptance_sources=acc_sources or ["%s/body" % module_ref])

        review = add("「%s」代码评审" % m["name"], m, "评审", priority, 1, [test["id"]],
                     acc or ["实现与验收条款一致"],
                     kind="review", part="review", out_schema="verify-v1",
                     what="对照验收条款评审「%s」的实现，给出问题清单与结论" % m["name"],
                     how="读实现与测试，检查重复逻辑/异常处理/硬编码/越界；阻塞问题必须列出",
                     files=[], acceptance_sources=acc_sources or ["%s/body" % module_ref])
        review_of[m["id"]] = review

    # 模块划分表里的依赖列 → 真正的任务依赖：被依赖模块的入口任务先做。
    # 加边前先查可达性，成环的边直接不加（并在 trace 里记账）。
    by_id = {t["id"]: t for t in tasks}
    entry = {}
    for t in tasks:
        if not t["module_id"]:
            continue
        if t["kind"] in ("design", "implementation"):
            prev = entry.get(t["module_id"])
            if prev is None or t["kind"] == "design":
                entry[t["module_id"]] = t

    def _reaches(src_id, dst_id, seen=None):
        seen = seen or set()
        if src_id == dst_id:
            return True
        if src_id in seen:
            return False
        seen.add(src_id)
        return any(_reaches(d, dst_id, seen) for d in by_id[src_id].get("depends_on", []) if d in by_id)

    edge_n, cycle_n = 0, 0
    for m in modules:
        tgt = entry.get(m.get("id"))
        if not tgt:
            continue
        for dep in (m.get("deps") or []):
            src = entry.get(dep)
            if not src or src["id"] == tgt["id"]:
                continue
            if src["id"] in tgt["depends_on"]:
                continue
            if _reaches(src["id"], tgt["id"]):
                cycle_n += 1      # 依赖反向成环：丢掉这条，不硬塞
                continue
            tgt["depends_on"].append(src["id"])
            edge_n += 1
    if edge_n:
        trace.append("模块依赖落成 %d 条任务先后关系%s"
                     % (edge_n, "（另有 %d 条成环已丢弃）" % cycle_n if cycle_n else ""))

    if design_count:
        trace.append("P0 或涉及接口的模块追加「方案与接口设计」：%d 条" % design_count)
    if data_count or api_count:
        trace.append("按文档证据追加：数据模型 %d 条、接口实现 %d 条（没写接口/表的规格不会有这些）"
                     % (data_count, api_count))
    trace.append("每个模块固定四段：功能实现 → 单元测试 → 代码评审 → 汇入集成")
    trace.append("实现任务按优先级估时：P0 5 人日 / P1 3 人日 / P2 2 人日，验收条款 ≥3 条再 +1")

    tails = [t["id"] for t in tasks
             if not any(t["id"] in other["depends_on"] for other in tasks)]
    integ_acc = ["端到端跑通主流程", "回归既有功能无退化"] + constraints
    integ_sources = [
        "%s/derived/integration-e2e" % spec["id"],
        "%s/derived/integration-regression" % spec["id"],
    ] + ["%s/constraints/%d" % (spec["id"], i + 1) for i in range(len(constraints))]
    integ = add("集成联调与整体验收", None, "测试", "P0", 2, tails, integ_acc,
                kind="integration", part="integrate", out_schema="test-v1",
                what="把所有模块装到一起，跑端到端主流程并回归",
                how="python -m pytest -q 跑全量用例；按约束逐条核对",
                files=["tests/test_integration.py"],
                acceptance_sources=integ_sources)
    add("交付清单与上线说明", None, "架构师", "P1", 1, [integ["id"]],
        ["启动方式、配置文件、遗留风险写清楚"],
        kind="delivery", part="delivery", out_schema="verify-v1",
        what="输出交付清单：怎么启动、怎么配置、还有哪些风险",
        how="写 README 的「运行方式」与「已知限制」两节",
        files=["README.md"],
        source_label="%s/derived/delivery" % spec["id"])
    if constraints:
        trace.append("%d 条全局约束并入整体验收清单" % len(constraints))

    phases = {}
    for t in tasks:
        phases.setdefault(t["phase"], []).append(t["id"])
    trace.append("阶段划分：" + "；".join("%s %d 条" % (p, len(phases.get(p, [])))
                                        for p in PHASES if phases.get(p)))

    stats = {
        "modules": len(modules),
        "tasks": len(tasks),
        "p0": len([t for t in tasks if t["priority"] == "P0"]),
        "estimate": sum(t["estimate"] for t in tasks),
        "depth": _task_depth(tasks),
        "phases": {p: len(phases.get(p, [])) for p in PHASES},
        "steps": sum(len(t["steps"]) for t in tasks),
        "endpoints": len(doc_eps),
        "tables": len(doc_tbls),
    }
    trace.append("共 %d 条任务 / %d 个子步骤，最长依赖链 %d 层，合计 %d 人日"
                 % (stats["tasks"], stats["steps"], stats["depth"], stats["estimate"]))
    return {"tasks": tasks, "trace": trace, "stats": stats}


class PMAgent:
    """PM 分解契约：实现可替换，但 tasks[] 的字段必须保持稳定。"""

    name = "rule"

    def decompose(self, spec, context=None):
        return decompose_spec(spec)


PM_AGENT = PMAgent()


# --------------------------------------------------------------------------
# 可替换的 Agent 执行器接口（B 任务）
#
# 编排器只依赖 `Agent.run(node, context) -> AgentResult` 这一个契约，不关心
# 具体实现是模拟器还是真实模型。MockAgent 用确定性模拟器产出可复现的结果，
# 保证 Mock 模式下全流程无需任何外部 API；LLMAgent 是可选的真实模型适配器，
# 通过环境变量/配置接入 OpenAI 兼容端点，失败时返回可分类、可重试的错误。
#
# 接入其它实现只需实现 run() 并注册到 AGENTS，编排器与前端都不用改。
# --------------------------------------------------------------------------
# 错误分类：code 是稳定错误码，category 是归类，retryable 决定是否自动重试。
AGENT_ERR = {
    "E-TIMEOUT": {"category": "timeout", "label": "执行超时", "retryable": True},
    "E-FORMAT":  {"category": "format",  "label": "格式门",   "retryable": True},
    "E-COMPILE": {"category": "compile", "label": "编译门",   "retryable": True},
    "E-REVIEW":  {"category": "review",  "label": "评审门",   "retryable": True},
    "E-NETWORK": {"category": "network", "label": "网络/服务", "retryable": True},
    "E-AGENT":   {"category": "agent",   "label": "执行器",   "retryable": False},
    # 进程崩溃/重启导致在途节点结果未知，只用于 _recover 标记，不走重试
    "E-INTERRUPT": {"category": "interrupt", "label": "运行中断", "retryable": False},
}

# Agent 节点未显式声明 timeout 时的兜底时限（秒）。执行在独立线程里进行，
# 超时由调度循环判定，线程本身无法被杀掉，其迟到结果按 token 丢弃。
EXEC_TIMEOUT_DEFAULT = 60.0


def make_error(code, reason, gate=None):
    meta = AGENT_ERR.get(code, {"category": "agent", "label": "执行器", "retryable": False})
    return {"code": code, "category": meta["category"], "label": meta["label"],
            "retryable": meta["retryable"], "reason": reason, "gate": gate or {}}


class AgentResult:
    def __init__(self, ok, output=None, error=None):
        self.ok = ok
        self.output = output or {}
        self.error = error

    @classmethod
    def success(cls, output):
        return cls(True, output=output)

    @classmethod
    def failure(cls, error):
        return cls(False, error=error)


# 交付物契约：声明了什么 schema，产物就必须长得像这个 schema。
# 之前格式门只把名字写进日志、从不校验内容，pm 节点产出 verify-v1 也照样"通过"。
OUTPUT_SCHEMAS = {
    "prd-v1": (("summary", str), ("requirements", list), ("tasks", list)),
    "design-v1": (("endpoints", list), ("entities", list)),
    "code-result-v1": (("loc", int),),          # 重构产物没有 files，只认代码量
    "scan-v1": (("score", int),),
    "test-v1": (("passed", int), ("total", int)),
    "verify-v1": (("merged", bool),),
}


def check_output_schema(schema, output):
    """返回 (是否合格, 原因)。未登记的自定义 schema 一律放行，避免卡住扩展。"""
    if not schema or schema not in OUTPUT_SCHEMAS:
        return True, ""
    if not isinstance(output, dict):
        return False, "产物不是 JSON 对象"
    artifact = output.get("artifact")
    if artifact != schema:
        return False, "产物声明的是 %r，节点要求 %r" % (artifact, schema)
    for key, expect in OUTPUT_SCHEMAS[schema]:
        if key not in output:
            return False, "缺少必填字段 %s" % key
        if not isinstance(output[key], expect):
            return False, "字段 %s 应为 %s，实际是 %s" % (
                key, expect.__name__, type(output[key]).__name__)
    return True, ""


class Agent:
    name = "base"

    def run(self, node, context):
        """执行一个 agent 节点，返回 AgentResult。context 由编排器注入。"""
        raise NotImplementedError


def _mock_output(n, context=None):
    """确定性模拟产物：**按节点声明的 out_schema 产出**，保证"声明什么就给什么"。

    旧版按节点 id（N4/N6/N7）分支，只对内置那套流程成立；一旦换成生成的图，
    N4 可能是"认证与账号实现"，却会拿到安全扫描的产物。改成契约驱动后，
    格式门校验才有意义（否则永远名不副实）。
    """
    context = context or {}
    role = n.get("role")
    name = n.get("name") or ""
    schema = n.get("out_schema") or {
        "pm": "prd-v1", "architect": "design-v1",
        "coder": "code-result-v1", "tester": "test-v1", "reviewer": "verify-v1",
        "qa": "verify-v1",
    }.get(role, "verify-v1")

    if schema == "prd-v1":
        tasks = context.get("tasks") or []
        if tasks:
            requirements = _clone(tasks)
        else:
            requirements = [{"id": "R1", "title": "梳理范围与关键交互"},
                            {"id": "R2", "title": "拆分模块与依赖"},
                            {"id": "R3", "title": "定义验收标准"}]
        return {"artifact": "prd-v1",
                "summary": (context.get("requirement") or name)[:80],
                "requirements": requirements,
                "tasks": _clone(tasks),
                "acceptance": ["每条任务有验收标准", "依赖关系完整、无环"]}
    if schema == "design-v1":
        return {"artifact": "design-v1", "endpoints": ["POST /api/login", "POST /api/logout"],
                "entities": ["user", "session"], "tables": 2, "planned_cases": 6}
    if schema == "code-result-v1":
        return {"artifact": "code-result-v1", "loc": 0,
                "summary": "模拟运行：未生成文件，也未在本机执行代码"}
    if schema == "scan-v1":
        return {"artifact": "scan-v1", "critical": 0, "high": 1, "score": 92,
                "findings": ["口令未加盐（中危）"]}
    if schema == "test-v1":
        criteria = list(n.get("acceptance") or [])
        results = [{
            "criterion": item,
            "status": "pass",
            "evidence": "mock://%s/acceptance/%d" % (n.get("task_id") or n.get("id"), i + 1),
        } for i, item in enumerate(criteria)]
        if not results:
            return {"artifact": "test-v1", "passed": 12, "total": 12,
                    "duration_ms": 1840, "acceptance_results": []}
        return {"artifact": "test-v1", "passed": len(results), "total": len(results),
                "duration_ms": 1840, "acceptance_results": results}
    if schema == "verify-v1":
        return {"artifact": "verify-v1", "merged": True, "regression": "pass", "coverage": 0.84}
    # 自定义 schema：至少把声明的名字回给格式门，具体结构由使用者自己约定
    return {"artifact": schema, "note": "模拟执行器产物", "node": n.get("id")}


class MockAgent(Agent):
    """确定性模拟执行器：无外部依赖，现场演示与测试的默认实现。"""

    name = "mock"

    def run(self, node, context):
        fault = context.get("fault")
        if fault:
            return AgentResult.failure(fault)
        return AgentResult.success(_mock_output(node, context))


def _llm_prompt(node, context):
    parts = [
        "你在 AutoFlow 工作流中扮演角色「%s」执行节点「%s」。" % (node.get("role"), node.get("name")),
        "原始需求：" + (context.get("requirement") or "（无）"),
        "请输出与该节点交付物（%s）一致的 JSON 对象，不要输出多余解释。" % node.get("out_schema", "default"),
    ]
    deps = context.get("deps") or []
    if deps:
        parts.append("上游节点：" + "、".join(deps))
    return "\n".join(parts)


def _parse_llm_output(content, schema):
    """把模型 content 解析为节点声明的 JSON 产物，而不是套 llm-v1 外壳。"""
    if isinstance(content, dict):
        content.setdefault("artifact", schema)
        return content
    text = (content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
        text = re.sub(r"\s*```$", "", text)
    try:
        value = json.loads(text)
    except Exception:
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("模型输出不是 JSON 对象")
        try:
            value = json.loads(text[start:end + 1])
        except Exception as exc:
            raise ValueError("模型输出 JSON 解析失败：%s" % exc)
    if not isinstance(value, dict):
        raise ValueError("模型输出必须是 JSON 对象")
    value.setdefault("artifact", schema)
    return value


class LLMAgent(Agent):
    """可选的真实模型适配器：调用 OpenAI 兼容 /chat/completions。

    需要端点与密钥（环境变量 AUTFLOW_LLM_URL / AUTFLOW_LLM_KEY，或通过
    /api/config 传入 agent=llm 之前配置好）。不配置时返回可分类的 E-AGENT 错误，
    绝不无限等待；超时、网络、服务端错误分别归类，供编排器决定是否重试。
    """

    name = "llm"

    def run(self, node, context):
        url = (context.get("llm_url") or os.environ.get("AUTFLOW_LLM_URL")
               or os.environ.get("AUTFLOW_LLM_BASE_URL") or "").rstrip("/")
        key = (context.get("llm_key") or os.environ.get("AUTFLOW_LLM_KEY")
               or os.environ.get("AUTFLOW_LLM_API_KEY") or "")
        model = context.get("llm_model") or os.environ.get("AUTFLOW_LLM_MODEL") or "gpt-4o-mini"
        if not url:
            return AgentResult.failure(make_error(
                "E-AGENT", "未配置 LLM 端点（设置 AUTFLOW_LLM_URL / AUTFLOW_LLM_KEY 后重试）"))
        timeout = int(node.get("timeout") or 60)
        payload = json.dumps({
            "model": model, "temperature": 0.2,
            "messages": [
                {"role": "system", "content": "你是 AutoFlow 工作流中的角色 Agent，只输出 JSON。"},
                {"role": "user", "content": _llm_prompt(node, context)},
            ],
        }).encode("utf-8")
        req = urllib.request.Request(url + "/chat/completions", data=payload, headers={
            "Content-Type": "application/json", "Authorization": "Bearer " + key})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read().decode("utf-8"))
            content = (((data.get("choices") or [{}])[0].get("message")) or {}).get("content", "")
            try:
                output = _parse_llm_output(content, node.get("out_schema") or "default")
            except ValueError as exc:
                return AgentResult.failure(make_error("E-FORMAT", str(exc)))
            return AgentResult.success(output)
        except socket.timeout:
            return AgentResult.failure(make_error("E-TIMEOUT", "调用模型超时（> %ss）" % timeout))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                return AgentResult.failure(make_error("E-NETWORK", "模型服务返回 HTTP %d" % e.code))
            return AgentResult.failure(make_error("E-AGENT", "模型服务拒绝请求（HTTP %d）" % e.code))
        except urllib.error.URLError as e:
            return AgentResult.failure(make_error("E-NETWORK", "无法连接模型服务：%s" % e.reason))
        except Exception as e:
            return AgentResult.failure(make_error("E-AGENT", "模型调用异常：%s" % e))


AGENTS = {"mock": MockAgent(), "llm": LLMAgent()}


# --------------------------------------------------------------------------
# 运行中状态持久化：崩溃/重启后据此恢复，而不是盲目重跑
# --------------------------------------------------------------------------
class RunStateStore:
    """把当前活动运行（含节点状态与事件）原子落盘，供重启恢复。"""

    def __init__(self, path):
        self.lock = threading.RLock()
        self.path = path

    def save(self, payload):
        with self.lock:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp, self.path)

    def load(self):
        with self.lock:
            try:
                with open(self.path, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None

    def clear(self):
        with self.lock:
            try:
                os.remove(self.path)
            except OSError:
                pass


# --------------------------------------------------------------------------
# 编排引擎
# --------------------------------------------------------------------------
class Engine:
    def __init__(self):
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.version = 0
        self.run = None
        self._thread = None
        self._stop = threading.Event()
        self._last_persist = 0.0
        # Agent 执行在独立线程池里进行：网络调用（LLM）不能占着引擎锁，
        # 否则一次慢请求就会把 /api/state、SSE 和编辑接口全部堵死。
        # 工作线程只把结果丢进队列，状态迁移仍然全部发生在调度线程内。
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="agent")
        self._results = queue.Queue()
        self._log_q = queue.Queue()          # 工作线程的执行日志，调度线程统一落盘
        self._exec_seq = 0
        # 运行配置在重启后保留（start/reset 不应丢配置）
        self.cfg = {"speed": 1.0, "demo": True, "qa_mode": "auto",
                    "agent": ("llm" if SETTINGS.public()["active"] else "mock")
                    if LOCAL_CODE_EXECUTION_ENABLED else "mock"}
        self.state_store = RunStateStore(RUN_STATE_PATH)
        # 当前运行所绑定的定义（运行时冻结，避免运行中编辑影响执行）
        self.wf = _clone(WORKFLOW)
        with self.lock:
            if not self._recover():
                self._new_run()

    # ---------------- 持久化与重启恢复 ----------------
    def persist(self):
        """把当前活动运行整体落盘（原子替换），供崩溃/重启后恢复。"""
        try:
            with self.lock:
                self.state_store.save({
                    "saved_at": time.time(),
                    "version": self.version,
                    "cfg": dict(self.cfg),
                    "wf": _clone(self.wf),
                    "run": _clone(self.run),
                })
        except Exception:
            pass

    def _normalize_run(self, run):
        run.setdefault("run_id", "RUN-recovered")
        run.setdefault("workflow_id", self.wf.get("workflow_id"))
        run.setdefault("workflow_name", self.wf.get("name"))
        run.setdefault("workflow_version", self.wf.get("version"))
        run.setdefault("requirement", self.wf.get("requirement", ""))
        run.setdefault("tasks", self.wf.get("tasks") or [])
        run.setdefault("spec_id", self.wf.get("spec_id"))
        run.setdefault("spec_revision", self.wf.get("spec_revision"))
        run.setdefault("spec_hash", self.wf.get("spec_hash"))
        run.setdefault("status", "IDLE")
        run.setdefault("nodes", {})
        run.setdefault("edges", [])
        run.setdefault("events", [])
        run.setdefault("seq", 0)
        run.setdefault("elapsed", 0.0)
        run.setdefault("started_at", None)
        run.setdefault("finished_at", None)
        run.setdefault("speed", self.cfg["speed"])
        run.setdefault("demo", self.cfg["demo"])
        run.setdefault("qa_mode", self.cfg["qa_mode"])
        run.setdefault("fail_mode", "off")
        run.setdefault("concurrency", 3)
        run.setdefault("stats", {"dispatched": 0, "retries": 0, "skipped": 0, "human_waits": 0})
        run.setdefault("demo_node", None)
        run.setdefault("agent", self.cfg.get("agent", "mock"))
        run.setdefault("mode", "llm" if SETTINGS.public()["active"] else "mock")
        run.setdefault("workspace", "")
        run.setdefault("artifacts", [])
        run.setdefault("recovered", False)
        run["_archived"] = False
        return run

    def _recover(self):
        """启动时尝试恢复上一次未结束的运行。

        原则：已到达终态（SUCCESS/SKIPPED/FAILED/CANCELLED）的节点保持原样；
        崩溃时仍在 RUNNING（在途、结果未知）的节点进入 UNCERTAIN 待核对状态，
        **不盲目重复执行**，等待人工确认/重试/跳过。
        """
        data = self.state_store.load()
        if not data:
            return False
        wf = data.get("wf")
        run = data.get("run")
        if not isinstance(wf, dict) or not isinstance(run, dict):
            return False
        status = run.get("status")
        if status in ("IDLE", "SUCCESS", "FAILED", "CANCELLED", None):
            return False
        self.wf = wf
        self.run = self._normalize_run(run)
        self.cfg.update(data.get("cfg") or {})
        self.version = int(data.get("version") or self.run.get("seq") or 0)

        uncertain = []
        for n in self.run["nodes"].values():
            if n.get("status") == "RUNNING":
                if n.get("attempts"):
                    n["attempts"][-1]["result"] = "unknown"
                    n["attempts"][-1]["error_code"] = "E-INTERRUPT"
                n["status"] = "UNCERTAIN"
                n["message"] = "进程中断，执行结果未知，需人工核对（不自动重复执行）"
                n["remaining"] = 0.0
                uncertain.append(n["id"])
            # 异步执行标记在进程重启后一律作废（没有工作线程可能再回结果）
            n["executing"] = False
            n.pop("exec_token", None)
            n.pop("exec_started", None)
        self.run["recovered"] = True
        # 崩溃时流程正卡在人工审批上：没有"结果未知"的节点，就保持原来的等待态，
        # 不应该把一次正常的审批等待变成需要额外 resume 的"待核对"。
        if uncertain:
            self.run["status"] = "RECONCILING"
        elif status == "WAITING_HUMAN":
            self.run["status"] = "WAITING_HUMAN"
        else:
            self.run["status"] = "RECONCILING"
        if uncertain:
            for nid in uncertain:
                self._emit("node.uncertain", nid, "进程中断，节点执行结果待核对")
            self._emit("run.recovered", None,
                       "检测到未完成运行，已恢复到核对状态（%d 个节点待核对）" % len(uncertain))
        elif status == "WAITING_HUMAN":
            self._emit("run.recovered", None, "检测到等待人工审批的运行，已保持等待态")
        else:
            self._emit("run.recovered", None, "检测到未完成运行，已恢复到核对状态")
        self.persist()
        return True

    def _spawn_loop(self):
        self._stop.set()          # 旧线程在下一轮循环自行退出
        ev = threading.Event()
        self._stop = ev
        self._thread = threading.Thread(target=self._loop, args=(ev,), daemon=True)
        self._thread.start()

    # ---------------- 运行生命周期 ----------------
    def _new_run(self):
        wf = self.wf
        nodes = {}
        for n in wf["nodes"]:
            nodes[n["id"]] = dict(
                n, status="PENDING", retry_count=0, attempts=[], gate={}, output=None,
                logs=[], duration=0.0, remaining=0.0, started_at=None, finished_at=None,
                branch_taken=None, message="等待前置依赖")
        for n in nodes.values():
            n.setdefault("max_retries", 2)
        self.run = {
            "run_id": "RUN-" + time.strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:4].upper(),
            "workflow_id": wf.get("workflow_id"),
            "workflow_name": wf.get("name"),
            "workflow_version": wf.get("version"),
            "requirement": wf.get("requirement", ""),
            "tasks": _clone(wf.get("tasks") or []),
            "spec_id": wf.get("spec_id"),
            "spec_revision": wf.get("spec_revision"),
            "spec_hash": wf.get("spec_hash"),
            "definition": _clone(wf),
            "status": "IDLE",
            "nodes": nodes,
            "edges": [dict(e, active=True) for e in wf["edges"]],
            "events": [],
            "seq": 0,
            "elapsed": 0.0,
            "started_at": None,
            "finished_at": None,
            "speed": self.cfg["speed"],
            "demo": self.cfg["demo"],
            "qa_mode": self.cfg["qa_mode"],
            "fail_mode": "off",          # off | single | persistent | timeout | timeout_persistent
            "concurrency": int(wf.get("concurrency_limit", 3) or 3),
            "stats": {"dispatched": 0, "retries": 0, "skipped": 0, "human_waits": 0},
            "agent": self.cfg.get("agent", "mock") if LOCAL_CODE_EXECUTION_ENABLED else "mock",
            "mode": ("llm" if SETTINGS.public()["active"] else "mock")
            if LOCAL_CODE_EXECUTION_ENABLED else "mock",
            "workspace": "",
            "artifacts": [],
            "recovered": False,
        }
        # 演示模式下"首个 Coder 节点首次失败"的落点（生成的工作流也适用）
        demo_node = None
        for n in wf["nodes"]:
            if n.get("type") == "agent" and n.get("role") == "coder":
                demo_node = n["id"]
                break
        if demo_node is None:
            for n in wf["nodes"]:
                if n.get("type") == "agent":
                    demo_node = n["id"]
                    break
        self.run["demo_node"] = demo_node
        self.run["_archived"] = False
        self.version += 1
        self.cond.notify_all()

    def reset(self):
        with self.lock:
            self._stop.set()
            self._new_run()
            self.state_store.clear()
            self.cond.notify_all()
            return True, "ok"

    def load(self, definition):
        """草稿被重建后同步运行时快照。

        没有这一步时会出现"画布是新图、右栏是旧图"的错位：画布画的是草稿，
        而 /api/state 仍然返回上一次发布的定义，点节点详情会看到别的节点。
        """
        with self.lock:
            if self.run["status"] in ("RUNNING", "PAUSED", "WAITING_HUMAN", "RECONCILING"):
                return False, "运行进行中，暂不能切换定义"
            self.wf = _clone(definition)
            annotate_parts(self.wf.get("nodes") or [])
            self._new_run()
            self._emit("workflow.loaded", None,
                       "已载入新草稿 · " + str(self.wf.get("name")))
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def start(self, definition=None):
        with self.lock:
            if self.run["status"] in ("RUNNING", "PAUSED", "WAITING_HUMAN", "RECONCILING"):
                return False, "运行已在进行中（如需放弃当前运行请先重置）"
            if definition is not None:
                self.wf = _clone(definition)     # 运行时冻结版本
                annotate_parts(self.wf.get("nodes") or [])
            self._new_run()
            self.run["status"] = "RUNNING"
            self.run["mode"] = ("llm" if SETTINGS.public()["active"] else "mock")
            if not LOCAL_CODE_EXECUTION_ENABLED:
                self.run["mode"] = "mock"
                self.run["agent"] = "mock"
            if LOCAL_CODE_EXECUTION_ENABLED and self.run["mode"] == "llm":
                self._prepare_workspace()
            self.run["started_at"] = time.time()
            self._emit("run.started", None, "运行已启动 · 工作流版本 " + str(self.wf.get("version")))
            if self.run["mode"] == "llm":
                self._emit("run.mode", None, "真实模型模式：%s / %s"
                           % (SETTINGS.data.get("provider"), SETTINGS.data.get("model")))
            self._spawn_loop()
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def _prepare_workspace(self):
        """每次运行一个隔离工作区：真实模型在这里落盘代码、跑测试。"""
        run_id = self.run.get("run_id") or "RUN"
        path = os.path.join(WORKSPACES_DIR, run_id)
        try:
            os.makedirs(path, exist_ok=True)
            self.run["workspace"] = path
        except OSError as exc:
            self.run["workspace"] = ""
            self.run["mode"] = "mock"
            self._emit("run.warning", None, "工作区创建失败，退回模拟模式：%s" % exc)

    def pause(self):
        with self.lock:
            if self.run["status"] != "RUNNING":
                return False, "当前状态不可暂停"
            self.run["status"] = "PAUSED"
            self._emit("run.paused", None, "人工暂停：停止派发新任务，在途任务继续收敛")
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def resume(self):
        with self.lock:
            status = self.run["status"]
            if status == "RECONCILING":
                uncertain = [n["id"] for n in self.run["nodes"].values()
                             if n["status"] == "UNCERTAIN"]
                if uncertain:
                    return False, ("仍有 %d 个节点待核对（%s），请先确认/重试/跳过"
                                   % (len(uncertain), "、".join(uncertain)))
                self.run["status"] = "RUNNING"
                self._emit("run.resumed", None, "恢复运行：已完成核对，继续调度")
                self._spawn_loop()
                self.persist()
                self.cond.notify_all()
                return True, "ok"
            if status != "PAUSED":
                return False, "当前状态不可恢复"
            self.run["status"] = "RUNNING"
            self._emit("run.resumed", None, "运行已恢复")
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def cancel(self):
        with self.lock:
            if self.run["status"] in ("IDLE", "SUCCESS", "FAILED", "CANCELLED"):
                return False, "当前状态不可取消"
            for nid, n in self.run["nodes"].items():
                # 取消时清掉异步执行标记，迟到结果会被 token 校验丢弃
                n["executing"] = False
                n.pop("exec_token", None)
                n.pop("exec_started", None)
                if n["status"] not in TERMINAL and n["status"] != "WAITING_HUMAN":
                    self._set_status(n, "CANCELLED", "运行被取消")
            self.run["status"] = "CANCELLED"
            self.run["finished_at"] = time.time()
            self._emit("run.cancelled", None, "运行已取消，未开始节点不再派发")
            self._stop.set()
            self.cond.notify_all()
            self._archive()
            return True, "ok"

    # ---------------- 配置与演示注入 ----------------
    def configure(self, speed=None, demo=None, qa_mode=None, fail_mode=None, agent=None):
        with self.lock:
            if agent is not None:
                if agent not in AGENTS:
                    return False, "未知执行器：%s（可用：%s）" % (agent, "、".join(AGENTS))
                self.cfg["agent"] = agent
                self.run["agent"] = agent
                self._emit("agent.switched", None, "执行器切换为 " + agent)
            if speed is not None:
                self.cfg["speed"] = max(0.25, min(8.0, float(speed)))
                self.run["speed"] = self.cfg["speed"]
            if demo is not None:
                self.cfg["demo"] = bool(demo)
                self.run["demo"] = self.cfg["demo"]
            if qa_mode in ("auto", "pass", "fail"):
                self.cfg["qa_mode"] = qa_mode
                self.run["qa_mode"] = qa_mode
            if fail_mode in ("off", "single", "persistent", "timeout", "timeout_persistent"):
                self.run["fail_mode"] = fail_mode
                if fail_mode != "off":
                    label = {
                        "single": "单次编译失败", "persistent": "持续编译失败",
                        "timeout": "单次超时", "timeout_persistent": "持续超时",
                    }.get(fail_mode, fail_mode)
                    self._emit("demo.inject", None, "已开启故障注入（%s）" % label)
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def inject_failure(self):
        with self.lock:
            self.run["fail_mode"] = "persistent"
            self._emit("demo.inject", None, "已开启持续故障注入：Coder 节点的编译门将持续失败")
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    # ---------------- 人工操作 ----------------
    def approve(self, node_id, decision, comment=""):
        with self.lock:
            n = self.run["nodes"].get(node_id)
            if not n or n["status"] != "WAITING_HUMAN":
                return False, "该节点不在待审批状态"
            if decision == "approve":
                n["gate"]["human"] = "pass"
                n["output"] = {"decision": "APPROVED", "approver": "值班工程师 · 张工",
                               "comment": comment or "同意上线", "signed_at": round(self.run["elapsed"], 2)}
                n["logs"].append("✓ 人工审批通过，签署人：张工")
                self._set_status(n, "SUCCESS", "人工审批通过")
                self.run["status"] = "RUNNING"
                self._emit("approval.resolved", node_id, "审批通过，流程继续向交付节点推进")
                self._ensure_loop()     # 重启后恢复的等待态没有在跑的调度线程
            else:
                n["gate"]["human"] = "fail"
                n["message"] = "审批被打回，等待修改后重试"
                n["logs"].append("✗ 人工审批打回：" + (comment or "需修改后重新提审"))
                self._emit("approval.resolved", node_id, "审批打回：" + (comment or "需修改后重新提审"))
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def retry_node(self, node_id):
        with self.lock:
            n = self.run["nodes"].get(node_id)
            if not n or n["status"] not in ("FAILED", "WAITING_HUMAN", "UNCERTAIN"):
                return False, "该节点当前不可重试"
            n["retry_count"] = 0
            n["gate"] = {}
            n["message"] = "人工触发重跑"
            n["logs"].append("↻ 人工触发重跑，尝试计数已重置")
            self._set_status(n, "READY", "人工重跑")
            # 恢复期的核对流程保持 RECONCILING，直到显式恢复；普通挂起则直接继续
            if self.run["status"] in ("PAUSED", "WAITING_HUMAN"):
                self.run["status"] = "RUNNING"
                self._ensure_loop()
            self._emit("node.retry", node_id, "人工触发重跑")
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def skip_node(self, node_id):
        with self.lock:
            n = self.run["nodes"].get(node_id)
            if not n or n["status"] not in ("FAILED", "WAITING_HUMAN", "UNCERTAIN"):
                return False, "该节点当前不可跳过"
            self._set_status(n, "SKIPPED", "人工跳过（操作者与理由已记录）")
            self._deactivate_outgoing(node_id)
            if self.run["status"] == "WAITING_HUMAN":
                self.run["status"] = "RUNNING"
                self._ensure_loop()
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    def confirm_node(self, node_id):
        """把恢复期 UNCERTAIN 节点核定为成功，**不重复执行**。"""
        with self.lock:
            n = self.run["nodes"].get(node_id)
            if not n or n["status"] != "UNCERTAIN":
                return False, "该节点不在待核对状态"
            if n.get("attempts"):
                n["attempts"][-1]["result"] = "confirmed"
            n["gate"] = n.get("gate") or {}
            n["output"] = n.get("output") or {}
            n["output"]["confirmed"] = True
            n["output"]["note"] = "人工核对确认，未重复执行"
            n["logs"].append("✓ 人工核对确认成功（未重复执行）")
            n["finished_at"] = round(self.run["elapsed"], 2)
            self._set_status(n, "SUCCESS", "人工核对确认成功")
            self._emit("node.confirmed", node_id, "人工核对确认成功，未重复执行")
            self.persist()
            self.cond.notify_all()
            return True, "ok"

    # ---------------- 主循环 ----------------
    def _ensure_loop(self):
        """确保调度线程在跑：从人工等待/暂停恢复时可能已经没有活着的循环了。"""
        if self._thread is None or not self._thread.is_alive():
            self._spawn_loop()

    def _loop(self, ev):
        last = time.time()
        last_ver = -1
        while not ev.is_set():
            time.sleep(0.05)
            now = time.time()
            dt = min(0.2, now - last)
            last = now
            with self.lock:
                if ev.is_set():
                    return
                if self.run["status"] not in ("RUNNING", "PAUSED", "WAITING_HUMAN"):
                    self._thread = None      # 明确标记循环已结束，便于 _ensure_loop 重新拉起
                    return
                if self.run["status"] == "RUNNING":
                    self.run["elapsed"] += dt
                # 先收执行结果（工作线程已完成的任务），再推进计时与派发
                self._drain_results()
                self._check_exec_timeouts()
                # 在途任务始终继续收敛（暂停只停止派发，见设计文档 3.2）
                self._advance(dt * self.run["speed"])
                # 等待人工期间也刷新依赖状态，让下游阻断可见
                if self.run["status"] in ("RUNNING", "WAITING_HUMAN"):
                    self._compute_ready()
                if self.run["status"] == "RUNNING":
                    self._dispatch()
                self._check_terminal()
                if self.version != last_ver:
                    last_ver = self.version
                    self.cond.notify_all()
                # 运行中状态持续落盘（限频），崩溃后可从最近一次快照恢复
                if now - self._last_persist >= 0.5:
                    self._last_persist = now
                    self.persist()

    def _advance(self, dt):
        for n in self.run["nodes"].values():
            if n["status"] != "RUNNING" or n.get("executing"):
                continue
            n["remaining"] -= dt
            n["duration"] += dt
            if n["remaining"] <= 0:
                if n["type"] in ("start", "end", "condition"):
                    self._finish(n)          # 纯本地判定，仍在调度线程内
                else:
                    self._begin_agent(n)     # 交给线程池，锁外执行

    def _compute_ready(self):
        for n in sorted(self.run["nodes"].values(), key=lambda x: x["id"]):
            # 已阻断的节点：上游恢复后重新参与调度，否则永久卡死
            if n["status"] == "BLOCKED":
                deps = [self.run["nodes"][d] for d in n.get("deps", [])]
                if any(d["status"] in BLOCKING for d in deps):
                    continue
                n["status"] = "PENDING"
            if n["status"] != "PENDING":
                continue
            if self._branch_excluded(n):
                self._set_status(n, "SKIPPED", "条件分支未命中，跳过")
                self.run["stats"]["skipped"] += 1
                self._deactivate_outgoing(n["id"])
                continue
            deps = [self.run["nodes"][d] for d in n.get("deps", [])]
            if not deps:
                self._set_status(n, "READY", "无前置依赖")
                continue
            if any(d["status"] in BLOCKING for d in deps):
                self._set_status(n, "BLOCKED", "上游未成功，下游阻断")
                continue
            if not all(d["status"] in ("SUCCESS", "SKIPPED") for d in deps):
                continue
            if n.get("join") == "any":
                if any(d["status"] == "SUCCESS" for d in deps):
                    self._set_status(n, "READY", "汇聚（any）：至少一条分支成功")
                continue
            if all(d["status"] == "SUCCESS" for d in deps):
                self._set_status(n, "READY", "依赖全部成功")
            elif all(d["status"] == "SKIPPED" for d in deps):
                self._set_status(n, "SKIPPED", "上游分支全部未命中")
                self._deactivate_outgoing(n["id"])

    def _dispatch(self):
        running = sum(1 for n in self.run["nodes"].values() if n["status"] == "RUNNING")
        ready = sorted([n for n in self.run["nodes"].values() if n["status"] == "READY"],
                       key=lambda n: n["id"])
        for n in ready:
            if running >= self.run["concurrency"]:
                break
            self._start_node(n)
            running += 1

    def _start_node(self, n):
        # 新一次尝试：清掉上一轮可能残留的异步执行标记，否则该节点永远不会再被推进
        n["executing"] = False
        n.pop("exec_token", None)
        n.pop("exec_started", None)
        n["attempts"].append({"attempt": len(n["attempts"]) + 1,
                              "started": round(self.run["elapsed"], 2), "result": "running"})
        self.run["stats"]["dispatched"] += 1
        if n["type"] == "approval":
            n["started_at"] = round(self.run["elapsed"], 2)
            self._set_status(n, "WAITING_HUMAN", "等待人工审批")
            self.run["stats"]["human_waits"] += 1
            self.run["status"] = "WAITING_HUMAN"
            n["logs"].append("⏸ 流程挂起：高风险节点，等待人工签署")
            self._emit("approval.requested", n["id"], "上线审批：等待值班工程师处理")
            return
        base = ROLE_META.get(n.get("role", "system"), ROLE_META["system"])["base_sec"]
        if n["type"] == "condition":
            base = 1.0
        jitter = 0.85 + (hash(n["id"] + str(len(n["attempts"]))) % 30) / 100.0
        n["remaining"] = max(0.6, base * jitter)
        n["started_at"] = round(self.run["elapsed"], 2)
        n["message"] = {
            "architect": "正在生成接口与数据模型", "coder": "正在生成代码与测试",
            "qa": "正在执行校验与评审", "system": "系统处理中",
        }.get(n.get("role"), "执行中")
        self._set_status(n, "RUNNING", n["message"])
        n["logs"].append("→ 装配上下文：" + ("、".join(n.get("deps", [])) or "用户原始需求"))
        n["logs"].append("→ 调用角色 Agent：" + ROLE_META.get(n.get("role", "system"), ROLE_META["system"])["label"])

    # ---------------- 完成与门禁 ----------------
    def _finish(self, n):
        """start / end / condition 三类节点在调度线程内直接结算。"""
        n["finished_at"] = round(self.run["elapsed"], 2)
        n["attempts"][-1]["ended"] = round(self.run["elapsed"], 2)

        if n["type"] == "start":
            n["output"] = {"input": self.run.get("requirement") or "为系统实现用户名密码登录",
                           "source": "user"}
            n["gate"] = {"format": "pass"}
            return self._succeed(n, "需求已接收")

        if n["type"] == "end":
            n["output"] = {"delivered": True, "artifacts": 4}
            n["gate"] = {}
            return self._succeed(n, "全部节点完成，交付物已归档")

        if n["type"] == "condition":
            return self._evaluate_condition(n)

        # 其它类型（agent 等）走异步路径；正常不会走到这里
        return self._begin_agent(n)

    # ---------------- Agent 异步执行 ----------------
    def _ai_for(self, n):
        """这个节点用哪个 AI：节点显式指定 > 分组默认 > 全局默认。"""
        known = {a.get("id") for a in (SETTINGS.data.get("ais") or [])}
        ai = (n.get("ai") or "").strip()
        if ai in known:
            return ai
        part = (n.get("part") or "").strip()
        mapping = SETTINGS.data.get("part_ais") or {}
        mapped = (mapping.get(part) or "").strip()
        return mapped if mapped in known else ""

    def _agent_kind(self, n):
        """决定这一次节点由谁执行。

        * 显式指定 agent=mock（默认）→ 本地确定性模拟，无密钥也能完整跑通；
        * 显式指定 agent=llm → 走真实模型（编码/设计/测试/评审用带工具的 ToolAgent）；
        * 用户保存配置并启用真实模型时，配置写入时会把 agent 切成 llm。
        """
        if not LOCAL_CODE_EXECUTION_ENABLED:
            return "mock"
        forced = self.cfg.get("agent") or "mock"
        if forced == "mock":
            # 例外：这个节点/分组指定了 AI，就按真实模型跑（用户显式指派，不能悄悄忽略）
            if self._ai_for(n):
                return "tool" if n.get("role") in ("coder", "architect", "tester",
                                                   "reviewer") else "llm"
            return "mock"
        if n.get("role") in ("coder", "architect", "tester", "reviewer"):
            return "tool"
        return "llm"

    def _task_of(self, n):
        tid = n.get("task_id")
        for t in (self.run.get("tasks") or []):
            if t.get("id") == tid:
                return _clone(t)
        return {}

    def _upstream_summaries(self, n):
        """把上游产物摘要带给下游：真实执行时下游需要知道前一步干了什么。"""
        out = []
        for dep in (n.get("deps") or [])[:6]:
            src = self.run["nodes"].get(dep)
            if not src or not src.get("output"):
                continue
            output = src["output"] or {}
            out.append({"name": src.get("name"),
                        "summary": output.get("summary") or output.get("design_notes")
                        or ("%s 个文件" % len(output.get("files") or []))})
        return out

    def _begin_agent(self, n):
        """把一次 Agent 执行交给线程池，结果由调度线程回收。

        调用方必须持有引擎锁；这里只做快照与提交。真正的 agent.run 在线程池里跑，
        不占用 self.lock —— 慢的 LLM 请求不会堵住 /api/state、SSE 和编辑接口。
        """
        self._exec_seq += 1
        token = self._exec_seq
        n["executing"] = True
        n["exec_token"] = token
        n["exec_started"] = self.run["elapsed"]
        spec = (self.run.get("definition") or {}).get("spec_snapshot") or {}
        ctx = {
            "node": _clone(n),
            "requirement": self.run.get("requirement", ""),
            "timeout": n.get("timeout"),
            "deps": list(n.get("deps") or []),
            "tasks": self.run.get("tasks") or [],
            "attempt": len(n.get("attempts") or []),
            # 故障注入有副作用（单次模式会自行关闭），每次尝试只求值一次
            "fault": self._injected_failure(n),
            "agent": self._agent_kind(n),
            "ai_id": self._ai_for(n),
            "task": self._task_of(n),
            "spec": spec,
            "workspace": self.run.get("workspace") or "",
            "upstream": self._upstream_summaries(n),
        }
        self._pool.submit(self._run_agent, n["id"], token, ctx)

    def _agent_log(self, node_id, message):
        """工作线程只往队列里塞日志，落盘统一在调度线程做（避免并发写节点状态）。"""
        self._log_q.put((node_id, message))

    def _run_tool_agent(self, ctx):
        node = ctx["node"]
        ws_dir = ctx.get("workspace") or ""
        if not ws_dir:
            return AgentResult.failure(make_error("E-AGENT", "没有可用的运行工作区"))
        ws = tool_agent.Workspace(ws_dir).ensure()
        agent = tool_agent.ToolAgent(LLM, SETTINGS.data, ws,
                                     ai_id=ctx.get("ai_id"),
                                     log=lambda m: self._agent_log(node["id"], m))
        try:
            raw = agent.run(node, ctx)
        except Exception as exc:
            return AgentResult.failure(make_error("E-AGENT", "执行器异常：%s" % exc))
        if raw.get("ok"):
            return AgentResult.success(raw.get("output") or {})
        err = raw.get("error") or {}
        return AgentResult.failure(make_error(err.get("code") or "E-AGENT",
                                              err.get("reason") or "执行失败"))

    def _run_agent(self, node_id, token, ctx):
        """线程池工作线程：只执行，不改引擎状态，结果丢进队列。"""
        try:
            if ctx.get("fault"):
                result = AgentResult.failure(ctx["fault"])
            elif ctx.get("agent") == "tool":
                result = self._run_tool_agent(ctx)
            else:
                agent = AGENTS.get(ctx.get("agent") or "mock") or AGENTS["mock"]
                result = agent.run(ctx["node"], ctx)
            if not isinstance(result, AgentResult):
                result = AgentResult.failure(
                    make_error("E-AGENT", "执行器返回了非法结果类型"))
        except Exception as exc:                    # 执行器异常不应拖垮调度线程
            result = AgentResult.failure(
                make_error("E-AGENT", "执行器异常：%s" % exc))
        self._results.put((node_id, token, result))

    def _drain_results(self):
        """调度线程回收结果；过期结果（已取消/超时/重试）按 token 丢弃。"""
        # 先把工作线程攒下的执行日志并入节点（工具调用的每一步都能在界面上看到）
        while True:
            try:
                node_id, message = self._log_q.get_nowait()
            except queue.Empty:
                break
            node = self.run["nodes"].get(node_id)
            if node is not None:
                node.setdefault("logs", []).append(message)
        while True:
            try:
                node_id, token, result = self._results.get_nowait()
            except queue.Empty:
                return
            n = self.run["nodes"].get(node_id)
            if not n or not n.get("executing") or n.get("exec_token") != token:
                continue
            n["executing"] = False
            n.pop("exec_token", None)
            n.pop("exec_started", None)
            if n["status"] != "RUNNING":
                continue
            n["duration"] = max(n["duration"], round(
                self.run["elapsed"] - (n.get("started_at") or 0.0), 2))
            n["finished_at"] = round(self.run["elapsed"], 2)
            n["attempts"][-1]["ended"] = round(self.run["elapsed"], 2)
            self._apply_agent_result(n, result)

    def _check_exec_timeouts(self):
        """按节点 timeout 判超时；线程杀不掉，但迟到结果会因 token 失效被丢弃。"""
        now = self.run["elapsed"]
        for n in list(self.run["nodes"].values()):
            if not n.get("executing"):
                continue
            limit = float(n.get("timeout") or EXEC_TIMEOUT_DEFAULT)
            if now - (n.get("exec_started") or 0.0) <= limit:
                continue
            n["executing"] = False
            n.pop("exec_token", None)
            n.pop("exec_started", None)
            n["duration"] = max(n["duration"], round(now - (n.get("started_at") or 0.0), 2))
            error = make_error("E-TIMEOUT", "节点执行超过 %s 秒时限" % int(limit),
                               gate={"format": "skip", "compile": "skip", "review": "skip"})
            n["attempts"][-1]["result"] = "failed"
            n["attempts"][-1]["error_code"] = "E-TIMEOUT"
            n["gate"] = dict(error["gate"])
            self._emit("gate.result", n["id"], "%s 未通过（%s）" % (error["label"], error["code"]))
            n["logs"].append("✗ 执行超时：超过 %s 秒" % int(limit))
            self._fail(n, error)

    def _record_artifacts(self, n, output):
        """把节点真正落盘的文件登记到运行产物表，供界面查看与验收。"""
        output = output or {}
        files = output.get("files") or []
        ws_dir = self.run.get("workspace") or ""
        known = {a["path"]: a for a in (self.run.get("artifacts") or [])}
        for rel in files[:40]:
            full = os.path.join(ws_dir, str(rel).replace("/", os.sep)) if ws_dir else ""
            size = 0
            try:
                size = os.path.getsize(full) if full else 0
            except OSError:
                size = 0
            known[str(rel)] = {"path": str(rel), "bytes": size,
                               "node": n["id"], "task_id": n.get("task_id")}
        self.run["artifacts"] = sorted(known.values(), key=lambda a: a["path"])
        if files:
            self._emit("artifact.saved", n["id"], "产出 %d 个文件：%s"
                       % (len(files), "、".join(str(f) for f in files[:4])))
        if isinstance(output.get("passed"), int) and isinstance(output.get("total"), int):
            self._emit("test.result", n["id"], "测试 %d/%d 通过（%s）"
                       % (output["passed"], output["total"],
                          output.get("command") or "pytest"))
            n["logs"].append("测试结果：%d/%d 通过 · %s"
                             % (output["passed"], output["total"],
                                output.get("command") or ""))
            if output.get("output_tail"):
                n["logs"].append("测试输出尾部：" + str(output["output_tail"])[-600:])

    def _apply_agent_result(self, n, result):
        """把执行器结果落成节点状态：schema 校验 → 门禁 → 成功/失败迁移。"""
        role = n.get("role")
        if result.ok:
            passed, why = check_output_schema(n.get("out_schema"), result.output)
            if not passed:
                result = AgentResult.failure(make_error(
                    "E-FORMAT", why,
                    gate={"format": "fail", "compile": "skip", "review": "skip", "human": "skip"}))

        if result.ok:
            n["gate"] = {"format": "pass", "compile": "pass" if role == "coder" else "skip",
                         "review": "pass" if role in ("qa", "tester", "reviewer") else "skip",
                         "human": "skip"}
            n["output"] = result.output
            n["attempts"][-1]["result"] = "success"
            n["logs"].append("✓ 输出通过 schema 校验（%s）" % n.get("out_schema", "default"))
            self._record_artifacts(n, result.output)
            self._emit("gate.result", n["id"], "格式门 / 编译门 / 评审门 全部通过")
            return self._succeed(n, "完成并通过质量门")

        error = result.error or make_error("E-AGENT", "执行器返回未知错误")
        n["attempts"][-1]["result"] = "failed"
        n["attempts"][-1]["error_code"] = error["code"]
        n["gate"] = dict(error.get("gate") or {})
        self._emit("gate.result", n["id"], "%s 未通过（%s）" % (error["label"], error["code"]))
        n["logs"].append("✗ %s 未通过：%s" % (error["label"], error["reason"]))
        return self._fail(n, error)

    def _injected_failure(self, n):
        """演示/测试用故障注入：返回分类错误或 None。真实 Agent 错误由 Agent 自行返回。"""
        role = n.get("role")
        mode = self.run.get("fail_mode", "off")
        if mode in ("timeout", "timeout_persistent") and role == "coder":
            if mode == "timeout":
                self.run["fail_mode"] = "off"
            timeout = n.get("timeout")
            return make_error("E-TIMEOUT",
                              "节点执行超过 %s 秒时限，模拟超时" % (timeout if timeout else "默认"),
                              gate={"format": "skip", "compile": "skip", "review": "skip"})
        if mode != "off" and role == "coder":
            if mode == "single":
                self.run["fail_mode"] = "off"
            return make_error("E-COMPILE", "src/login.py 第 42 行 NameError，依赖未解析",
                              gate={"compile": "fail"})
        # 演示模式只对模拟执行器生效：真实模型跑任务时不该被"故意失败一次"干扰
        if (self.run["demo"] and self.run.get("agent", "mock") == "mock"
                and n["id"] == self.run.get("demo_node")
                and len(n.get("attempts") or []) == 1):
            return make_error("E-FORMAT", "输出缺少必填字段 files[].sha256",
                              gate={"format": "fail"})
        return None

    def _evaluate_condition(self, n):
        if self.run["qa_mode"] == "pass":
            score = 88
        elif self.run["qa_mode"] == "fail":
            score = 72
        else:
            score = 72 if len(n["attempts"]) == 1 else 88
        result = score >= 80
        n["branch_taken"] = "true" if result else "false"
        n["gate"] = {"review": "pass" if result else "fail"}
        n["output"] = {
            "qa_score": score,
            "expression": n.get("expression", "qa_score >= 80"),
            "result": result,
            "blocking": [] if result else ["覆盖率 61% 低于 80% 阈值"],
            "rubric_version": "v2",
        }
        n["attempts"][-1]["result"] = "success"
        n["logs"].append("→ 执行受限表达式：%s" % n.get("expression"))
        n["logs"].append("→ 评分 qa_score = %d → %s 分支" % (score, "true" if result else "false"))
        for e in self.run["edges"]:
            if e["source"] == n["id"]:
                e["active"] = (e.get("label") == n["branch_taken"])
        self._emit("branch.selected", n["id"],
                   "质量门评分 %d，命中 %s 分支" % (score, "TRUE" if result else "FALSE"))
        self._set_status(n, "SUCCESS", "判定完成 → %s 分支" % ("TRUE" if result else "FALSE"))
        return

    def _succeed(self, n, msg):
        self._set_status(n, "SUCCESS", msg)
        n["logs"].append("✓ 节点完成，耗时 %.1fs" % n["duration"])

    def _fail(self, n, error):
        code = error.get("code", "E-AGENT")
        label = error.get("label", code)
        reason = error.get("reason", "")
        n["message"] = "%s：%s" % (label, reason)
        self._set_status(n, "FAILED", n["message"])

        # 不可重试错误：不消耗重试次数，直接转人工（人工可重试/跳过）
        if not error.get("retryable", True):
            self._set_status(n, "WAITING_HUMAN", "不可重试错误，转人工处理")
            self.run["status"] = "WAITING_HUMAN"
            self._emit("human.escalated", n["id"], "不可重试错误（%s），流程暂停等待人工处理" % code)
            return

        n["retry_count"] += 1
        if n["retry_count"] <= int(n.get("max_retries", 2)):
            self.run["stats"]["retries"] += 1
            self._emit("retry.scheduled", n["id"],
                       "第 %d/%d 次重试，携带校验反馈" % (n["retry_count"], n.get("max_retries", 2)))
            n["logs"].append("↻ 进入重试队列（%d/%d），退避 2s" % (n["retry_count"], n.get("max_retries", 2)))
            self._set_status(n, "READY", "等待重试（%d/%d）" % (n["retry_count"], n.get("max_retries", 2)))
        else:
            self._set_status(n, "WAITING_HUMAN", "重试次数耗尽，转人工处理")
            self.run["status"] = "WAITING_HUMAN"
            self._emit("human.escalated", n["id"], "重试次数耗尽，流程暂停等待人工处理")

    # ---------------- 终止判定 ----------------
    def _check_terminal(self):
        nodes = list(self.run["nodes"].values())
        if any(n["status"] == "WAITING_HUMAN" for n in nodes):
            if self.run["status"] == "RUNNING":
                self.run["status"] = "WAITING_HUMAN"
            return
        if any(n["status"] in ("RUNNING", "READY", "PENDING", "UNCERTAIN") for n in nodes):
            return
        if self.run["status"] in ("SUCCESS", "FAILED", "CANCELLED"):
            return
        ok = all(n["status"] in ("SUCCESS", "SKIPPED") for n in nodes)
        self.run["status"] = "SUCCESS" if ok else "FAILED"
        self.run["finished_at"] = time.time()
        self._emit("run.finished", None,
                   ("运行成功结束，用时 %.1fs" % self.run["elapsed"]) if ok
                   else ("运行失败结束，用时 %.1fs" % self.run["elapsed"]))
        self._archive()

    def _archive(self):
        """运行进入终态时归档一份快照，供历史回看，并清理运行中状态文件。"""
        if self.run.get("_archived"):
            return
        self.run["_archived"] = True
        try:
            ARCHIVE.add(self.snapshot(include_workflow=True))
        except Exception:
            traceback.print_exc()
        self.state_store.clear()

    # ---------------- 工具方法 ----------------
    def _branch_excluded(self, n):
        ins = [e for e in self.run["edges"] if e["target"] == n["id"]]
        return bool(ins) and all(not e["active"] for e in ins)

    def _deactivate_outgoing(self, node_id):
        for e in self.run["edges"]:
            if e["source"] == node_id:
                e["active"] = False

    def _set_status(self, n, status, message):
        if n["status"] == status:
            if message:
                n["message"] = message
            return
        old = n["status"]
        n["status"] = status
        n["message"] = message or n["message"]
        # 跳过/等人工不是"执行成功"，但同样该在日志里留一行，别让日志空白
        if status in ("SKIPPED", "WAITING_HUMAN") and message:
            logs = n.setdefault("logs", [])
            line = ("跳过：%s" % message) if status == "SKIPPED" else ("等待人工：%s" % message)
            if line not in logs:
                logs.append(line)
        self._emit("node.status", n["id"], "%s → %s（%s）" % (old, status, message or ""))

    def _emit(self, etype, node_id, message):
        self.run["seq"] += 1
        self.run["events"].append({
            "seq": self.run["seq"], "ts": round(self.run["elapsed"], 2),
            "type": etype, "node_id": node_id, "message": message,
        })
        self.version += 1
        if len(self.run["events"]) > 800:
            self.run["events"] = self.run["events"][-800:]

    def _slim_nodes(self):
        out = []
        for n in sorted(self.run["nodes"].values(), key=lambda x: x["id"]):
            out.append({k: n[k] for k in NODE_FIELDS if k in n})
        return out

    def snapshot(self, include_workflow=False):
        with self.lock:
            snap = {
                "version": self.version,
                "run": {k: self.run.get(k) for k in
                        ("run_id", "workflow_id", "workflow_name", "workflow_version", "status",
                         "requirement", "elapsed", "started_at", "finished_at", "speed",
                         "demo", "qa_mode", "fail_mode", "concurrency", "stats", "seq",
                         "agent", "recovered", "tasks", "spec_id", "spec_revision", "spec_hash",
                         "mode", "workspace", "artifacts")},
                "nodes": self._slim_nodes(),
                "edges": [dict(e) for e in self.run["edges"]],
                "events": self.run["events"][-200:],
                "roles": ROLE_META,
                "agents": {"current": self.cfg.get("agent", "mock"),
                           "available": sorted(AGENTS.keys())},
                "settings": SETTINGS.public(),
            }
            if include_workflow:
                snap["workflow"] = _clone(self.run.get("definition") or self.wf)
            return snap


ENGINE = Engine()


# --------------------------------------------------------------------------
# HTTP 服务
# --------------------------------------------------------------------------
class AutoFlowServer(ThreadingHTTPServer):
    """Windows 下必须关掉 SO_REUSEADDR：否则第二个实例能静默绑定同一端口，
    造成"看起来启动了、打开却是别人的响应"的假象。"""
    daemon_threads = True
    allow_reuse_address = (os.name != "nt")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "AutoFlow/1.0"

    def log_message(self, fmt, *args):
        pass

    def handle_one_request(self):
        """浏览器取消/刷新会直接断开连接，不必在控制台打印堆栈。"""
        try:
            BaseHTTPRequestHandler.handle_one_request(self)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            self.close_connection = True

    # ---------- 工具 ----------
    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path):
        if not os.path.isfile(path):
            return self._json({"error": "not found", "path": path}, 404)
        ext = os.path.splitext(path)[1].lower()
        ctype = {".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
                 ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml",
                 ".json": "application/json; charset=utf-8"}.get(ext, "application/octet-stream")
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b"{}"
            return json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return {}

    # ---------- 运行工作区（真实落盘的代码） ----------
    def _workspace_of(self, run_id):
        """当前运行直接用它自己的工作区；历史运行按约定目录找（工作区不删除）。"""
        if ENGINE.run.get("run_id") == run_id and ENGINE.run.get("workspace"):
            return ENGINE.run["workspace"]
        return os.path.join(WORKSPACES_DIR, run_id)

    def _run_files(self, run_id):
        ws_dir = self._workspace_of(run_id)
        live = ENGINE.run if isinstance(ENGINE.run, dict) else {}
        if live.get("run_id") == run_id:
            artifacts = live.get("artifacts") or []
        else:
            artifacts = (ARCHIVE.get(run_id) or {}).get("artifacts") or []
        files = []
        if os.path.isdir(ws_dir):
            try:
                files = tool_agent.Workspace(ws_dir).list("", depth=6)
            except Exception:
                files = []
        return self._json({"run_id": run_id,
                           "workspace": ws_dir if os.path.isdir(ws_dir) else "",
                           "files": files,
                           "artifacts": artifacts})

    def _run_file(self, run_id):
        qs = parse_qs(urlparse(self.path).query)
        rel = (qs.get("path") or [""])[0]
        ws_dir = self._workspace_of(run_id)
        if not os.path.isdir(ws_dir):
            return self._json({"error": "该运行没有工作区"}, 404)
        try:
            data = tool_agent.Workspace(ws_dir).read(rel, limit=200000)
        except FileNotFoundError:
            return self._json({"error": "文件不存在：" + rel}, 404)
        except Exception as exc:
            return self._json({"error": str(exc)}, 400)
        return self._json({"ok": True, **data})

    # ---------- 路由 ----------
    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            return self._file(os.path.join(STATIC_DIR, "index.html"))
        if path.startswith("/static/"):
            return self._file(os.path.join(STATIC_DIR, path[len("/static/"):]))
        if path == "/api/state":
            return self._json(ENGINE.snapshot(include_workflow=True))
        if path == "/api/editor":
            return self._json(self._editor_payload())
        if path == "/api/workflows":
            return self._json({"workflows": STORE.list()})
        if path == "/api/templates":
            return self._json({"templates": list_templates()})
        if path == "/api/specs":
            return self._json({"specs": SPECS.list()})
        if path == "/api/runs":
            return self._json({"runs": ARCHIVE.list()})
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "runs" and parts[3] == "files":
            return self._run_files(parts[2])
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "runs" and parts[3] == "file":
            return self._run_file(parts[2])
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "runs":
            item = ARCHIVE.get(parts[2])
            if not item:
                return self._json({"error": "run not found"}, 404)
            return self._json({"run": item})
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "specs":
            spec = SPECS.get(parts[2])
            if not spec:
                return self._json({"error": "spec not found"}, 404)
            return self._json({"spec": spec})
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "plan":
            session = PLANNER.get(parts[2])
            if not session:
                return self._json({"ok": False, "message": "规划会话不存在"}, 404)
            return self._json({"ok": True, "session": session})
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "workflows":
            wf = STORE.get(parts[2])
            if not wf:
                return self._json({"error": "workflow not found"}, 404)
            return self._json({"workflow": wf})
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "workflows" and parts[3] == "versions":
            wf = STORE.get(parts[2])
            if not wf:
                return self._json({"error": "workflow not found"}, 404)
            return self._json({"versions": [
                {"version": v["version"], "created_at": v["created_at"], "note": v["note"],
                 "nodes": len(v["definition"].get("nodes", [])),
                 "edges": len(v["definition"].get("edges", []))}
                for v in wf["versions"]]})
        if path == "/api/workflow":
            wf = _clone(ENGINE.wf)
            ok, errors, warnings, stats = validate_workflow(wf)
            return self._json({"workflow": wf, "valid": ok, "errors": errors,
                               "warnings": warnings, "stats": stats})
        if path == "/api/stream":
            return self._stream()
        if path == "/api/agents":
            current = ENGINE.cfg.get("agent", "mock") if LOCAL_CODE_EXECUTION_ENABLED else "mock"
            return self._json({"current": current,
                               "available": sorted(AGENTS.keys()),
                               "mode": ENGINE.run.get("mode"),
                               "local_code_execution": LOCAL_CODE_EXECUTION_ENABLED,
                               "settings": SETTINGS.public()})
        if path == "/api/settings":
            return self._json({"settings": SETTINGS.public()})
        if path == "/api/ais":
            pub = SETTINGS.public()
            return self._json({"ok": True, "ais": pub["ais"],
                               "role_ais": pub["role_ais"],
                               "part_ais": pub["part_ais"],
                               "primary": pub["primary"],
                               "presets": pub["presets"],
                               "parts": [{"id": x["id"], "label": x["label"],
                                          "color": x["color"]} for x in PARTS]})
        if path == "/api/health":
            return self._json({"ok": True, "ts": time.time(),
                               "mode": ENGINE.run.get("mode")})
        return self._json({"error": "not found"}, 404)

    def do_PUT(self):
        path = urlparse(self.path).path
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "workflows":
            return self._save_definition(parts[2], self._body())
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "specs":
            return self._save_spec(parts[2], self._body())
        return self._json({"error": "not found"}, 404)

    # ---------- 定义编辑 ----------
    def _editor_payload(self):
        wid = STORE.default_id()
        wf = STORE.get(wid) if wid else None
        if not wf:
            return {"ok": False, "message": "仓库中没有工作流"}
        return {
            "ok": True,
            "workflow_id": wf["id"],
            "name": wf["name"],
            "requirement": wf.get("requirement", ""),
            "draft": wf["draft"],
            "versions": [{"version": v["version"], "created_at": v["created_at"],
                          "note": v["note"],
                          "nodes": len(v["definition"].get("nodes", [])),
                          "edges": len(v["definition"].get("edges", []))}
                         for v in wf["versions"]],
            "run": {"status": ENGINE.run["status"], "version": ENGINE.wf.get("version")},
            "roles": ROLE_META,
        }

    def _save_definition(self, wf_id, body):
        definition = body.get("definition")
        if not isinstance(definition, dict):
            return self._json({"ok": False, "message": "请求缺少 definition"}, 400)
        ok, errors, warnings, stats = validate_workflow(definition)
        if not ok:
            return self._json({"ok": False, "message": "校验未通过，草稿未保存",
                               "errors": errors, "warnings": warnings}, 400)
        draft, err = STORE.save_draft(wf_id, _clone(definition), body.get("name"))
        if err:
            return self._json({"ok": False, "message": err}, 404)
        return self._json({"ok": True, "message": "草稿已保存", "draft": draft,
                           "warnings": warnings, "stats": stats})

    # ---------- 设计规格 ----------
    def _create_spec(self, body):
        try:
            spec = SPECS.create(body)
        except SpecError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        return self._json({"ok": True, "message": "规格已创建", "spec": spec})

    def _save_spec(self, spec_id, body):
        try:
            spec, err = SPECS.update(spec_id, body)
        except SpecError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        if err:
            return self._json({"ok": False, "message": err}, 404)
        return self._json({"ok": True, "message": "规格已保存", "spec": spec})

    def _decompose_spec(self, spec_id):
        """规格 → 任务清单。只读：分解结果由前端暂存，正式编排在 T4 落库。"""
        spec = SPECS.get(spec_id)
        if not spec:
            return self._json({"ok": False, "message": "规格不存在"}, 404)
        result = PM_AGENT.decompose(spec)
        return self._json({
            "ok": True,
            "message": "已拆出 %d 条任务" % len(result["tasks"]),
            "spec": {"id": spec["id"], "name": spec["name"],
                     "revision": spec.get("revision"), "content_hash": spec.get("content_hash")},
            "tasks": result["tasks"],
            "trace": result["trace"],
            "stats": result["stats"],
        })

    def _plan_spec(self, spec_id, body):
        """规格 → 任务清单 → DAG：编排后直接落库为当前草稿，画布随即显示。"""
        spec = SPECS.get(spec_id)
        if not spec:
            return self._json({"ok": False, "message": "规格不存在"}, 404)
        if ENGINE.run["status"] not in ("IDLE", "SUCCESS", "FAILED", "CANCELLED"):
            return self._json({"ok": False, "message": "运行进行中，暂不能重建草稿"}, 409)
        wid = STORE.default_id()
        wf = STORE.get(wid) if wid else None
        if not wf:
            return self._json({"ok": False, "message": "仓库中没有工作流"}, 404)

        tasks = PM_AGENT.decompose(spec)["tasks"]
        try:
            definition, meta = plan_from_tasks(spec, tasks,
                                               body.get("template") or "default")
        except TemplateError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        ok, errors, warnings, stats = validate_workflow(definition)
        if not ok:
            # 编排器自检失败：不写入仓库，把问题原样抛回
            return self._json({"ok": False, "message": "编排出的图未通过校验",
                               "errors": errors}, 500)

        draft, err = STORE.save_draft(wid, definition, name=definition["name"],
                                      requirement=definition["requirement"], dirty=False)
        if err:
            return self._json({"ok": False, "message": err}, 404)
        ENGINE.load(definition)      # 画布与运行时快照保持一致
        return self._json({"ok": True,
                           "message": "已按规格编排 %d 条任务" % len(tasks),
                           "draft": draft, "tasks": tasks, "trace": meta["trace"],
                           "stats": stats, "warnings": warnings,
                           "template": meta["template"],
                           "spec_revision": definition.get("spec_revision"),
                           "spec_hash": definition.get("spec_hash")})

    def _plan_discuss(self, spec_id, body):
        """开一场多智能体规划讨论：角色各自发言 → 汇总成任务清单。

        讨论本身不动仓库、不动运行，只产出一份"待确认的任务清单"；
        真正生成工作流要再调 `POST /api/plan/{sid}`（apply），
        这样"讨论"可以随便重开，不会污染当前草稿。
        """
        spec = SPECS.get(spec_id)
        if not spec:
            return self._json({"ok": False, "message": "规格不存在"}, 404)
        pub = SETTINGS.public()
        if not pub.get("any_active"):
            return self._json({"ok": False, "code": "E-AGENT",
                               "message": "多智能体讨论需要真实模型：点右上角「模拟」"
                                          "填入 Base URL 与 API Key 并勾选「使用真实模型执行」"}, 400)
        modules = spec.get("modules") or []
        if not modules and not (spec.get("body") or "").strip():
            return self._json({"ok": False,
                               "message": "规格里既没有模块也没有正文，没什么可讨论的"}, 400)
        role_ais = dict(SETTINGS.data.get("role_ais") or {})
        role_ais.update({str(k): str(v) for k, v in (body.get("role_ais") or {}).items()
                         if v})
        session = PLANNER.start(spec, body.get("template") or "default",
                                {"role_ais": role_ais})
        return self._json({"ok": True, "session_id": session["id"],
                           "session": PLANNER.get(session["id"])})

    def _plan_apply(self, sid, body):
        """把讨论产出的任务清单编排成 DAG 并落库为当前草稿。"""
        session = PLANNER.get(sid)
        if not session:
            return self._json({"ok": False, "message": "规划会话不存在"}, 404)
        if session["status"] == "running":
            return self._json({"ok": False, "message": "讨论还没结束"}, 409)
        if session["status"] != "done" or not session["tasks"]:
            err = (session.get("error") or {}).get("message") or session.get("stage")
            return self._json({"ok": False,
                               "message": "这次讨论没有产出可用任务：%s" % (err or "未知原因")},
                              409)
        consensus = session.get("consensus") or {}
        if consensus.get("status") == "blocked" and not session.get("human_decision"):
            return self._json({"ok": False, "code": "E-CONSENSUS",
                               "consensus": consensus,
                               "message": "讨论没达成共识（%s）。请先「接受当前清单」或让相关角色继续讨论"
                                          % "；".join(consensus.get("unresolved") or [])[:160]},
                              409)
        if ENGINE.run["status"] not in ("IDLE", "SUCCESS", "FAILED", "CANCELLED"):
            return self._json({"ok": False, "message": "运行进行中，暂不能重建草稿"}, 409)
        spec = SPECS.get(session["spec_id"]) or {}
        if not spec:
            return self._json({"ok": False, "message": "规格已被删除"}, 404)
        wid = STORE.default_id()
        wf = STORE.get(wid) if wid else None
        if not wf:
            return self._json({"ok": False, "message": "仓库中没有工作流"}, 404)

        template = body.get("template") or session.get("template") or "default"
        tasks = session["tasks"]
        try:
            definition, meta = plan_from_tasks(spec, tasks, template)
        except TemplateError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        ok, errors, warnings, stats = validate_workflow(definition)
        if not ok:
            return self._json({"ok": False, "message": "讨论产出的任务编排成的图未通过校验",
                               "errors": errors}, 500)

        draft, err = STORE.save_draft(wid, definition, name=definition["name"],
                                      requirement=definition["requirement"], dirty=False)
        if err:
            return self._json({"ok": False, "message": err}, 404)
        ENGINE.load(definition)
        return self._json({"ok": True,
                           "message": "已按 AI 讨论结果编排 %d 条任务" % len(tasks),
                           "draft": draft, "tasks": tasks,
                           "trace": meta["trace"] + session.get("trace", []),
                           "stats": stats, "warnings": warnings,
                           "template": meta["template"],
                           "discussion": {"session_id": sid,
                                          "minutes": len(session["minutes"]),
                                          "fixes": session.get("fixes") or [],
                                          "usage": session.get("usage") or {}},
                           "spec_revision": definition.get("spec_revision"),
                           "spec_hash": definition.get("spec_hash")})

    def _preview_spec(self, spec_id, body):
        """只读预演：把规格编排成 DAG 交给前端画在画布上，不写仓库、不动草稿与版本。

        与 _plan_spec 的区别只有一句：**不落库**。所以「分解预览」可以随便点，
        退出时画布自然回到原来的定义，不需要回滚任何状态。
        """
        spec = SPECS.get(spec_id)
        if not spec:
            return self._json({"ok": False, "message": "规格不存在"}, 404)
        if ENGINE.run["status"] not in ("IDLE", "SUCCESS", "FAILED", "CANCELLED"):
            return self._json({"ok": False,
                               "message": "运行进行中，画布正在跟随运行状态，稍后再预览"}, 409)
        result = PM_AGENT.decompose(spec)
        tasks = result["tasks"]
        try:
            definition, meta = plan_from_tasks(spec, tasks,
                                               body.get("template") or "default")
        except TemplateError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        ok, errors, warnings, stats = validate_workflow(definition)
        if not ok:
            return self._json({"ok": False, "message": "预演出的图未通过校验",
                               "errors": errors}, 500)
        definition = dict(definition, preview=True, spec_id=spec["id"],
                          spec_revision=spec.get("revision"))
        return self._json({
            "ok": True, "preview": True,
            "message": "预演：%d 条任务 → %d 个节点 / %d 条边（未保存）"
                       % (len(tasks), stats["nodes"], stats["edges"]),
            "spec": {"id": spec["id"], "name": spec["name"],
                     "revision": spec.get("revision"), "content_hash": spec.get("content_hash")},
            "definition": definition, "tasks": tasks, "trace": result["trace"],
            # 任务清单用 compose 的统计（模块/任务/人日/最长链），图规模单独给 graph
            "stats": result["stats"],
            "graph": {"nodes": stats["nodes"], "edges": stats["edges"]},
            "warnings": warnings, "template": meta["template"],
        })

    def _generate(self, body):
        """需求 → 草稿：生成后直接落库为当前草稿，前端随即展示可编辑的 DAG。"""
        text = (body.get("requirement") or "").strip()
        if not text:
            return self._json({"ok": False, "message": "请先填写需求描述"}, 400)
        if len(text) < 6:
            return self._json({"ok": False, "message": "需求太短（至少 6 个字），再多描述一点"}, 400)
        if ENGINE.run["status"] not in ("IDLE", "SUCCESS", "FAILED", "CANCELLED"):
            return self._json({"ok": False, "message": "运行进行中，暂不能重建草稿"}, 409)

        wid = STORE.default_id()
        wf = STORE.get(wid) if wid else None
        if not wf:
            return self._json({"ok": False, "message": "仓库中没有工作流"}, 404)

        try:
            definition, meta = generate_workflow(text, body.get("name"),
                                                 body.get("template") or "default")
        except TemplateError as exc:
            return self._json({"ok": False, "message": str(exc)}, 400)
        ok, errors, warnings, stats = validate_workflow(definition)
        if not ok:
            # 生成器自检失败：不写入仓库，把问题原样抛回，便于定位
            return self._json({"ok": False, "message": "生成的图未通过校验", "errors": errors}, 500)

        draft, err = STORE.save_draft(wid, definition, name=definition["name"],
                                      requirement=text, dirty=False)
        if err:
            return self._json({"ok": False, "message": err}, 404)
        ENGINE.load(definition)      # 画布与运行时快照保持一致
        return self._json({"ok": True, "message": "已生成工作流草稿",
                           "draft": draft, "trace": meta["trace"],
                           "matched": meta["matched"], "risk": meta["risk"],
                           "stats": stats, "warnings": warnings})

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._body()
        parts = [p for p in path.split("/") if p]

        if path == "/api/specs":
            return self._create_spec(body)
        if path == "/api/specs/import":
            try:
                parsed = spec_import.parse_any(body.get("text") or "", body.get("name") or "")
            except Exception as exc:
                return self._json({"ok": False, "message": "解析失败：%s" % exc}, 400)
            return self._json({"ok": True, "spec": parsed})
        if path == "/api/settings/clear-key":
            SETTINGS.clear_key(body.get("ai_id"))
            return self._json({"ok": True, "cleared": True,
                               "settings": SETTINGS.public()})
        if path == "/api/tasks/preview-code":
            task_id = str(body.get("task_id") or "")
            try:
                step_index = int(body.get("step_index"))
            except (TypeError, ValueError):
                return self._json({"ok": False, "message": "子步骤编号无效"}, 400)
            if not task_id or step_index < 0:
                return self._json({"ok": False, "message": "任务或子步骤编号无效"}, 400)
            run_id = str(body.get("run_id") or "")
            with ENGINE.lock:
                live = ENGINE.run if isinstance(ENGINE.run, dict) else {}
                if not run_id or run_id == live.get("run_id"):
                    run_id = live.get("run_id") or ""
                    definition = (live.get("definition")
                                  if live.get("status") != "IDLE" else ENGINE.wf)
                    ws_dir = live.get("workspace") or (
                        os.path.join(WORKSPACES_DIR, run_id) if run_id
                        else os.path.join(WORKSPACES_DIR, "_draft"))
                else:
                    snap = ARCHIVE.get(run_id) or {}
                    definition = snap.get("definition") or ENGINE.wf
                    ws_dir = os.path.join(WORKSPACES_DIR, run_id)
                definition = _clone(definition or ENGINE.wf)
            task = next((t for t in (definition.get("tasks") or [])
                         if str(t.get("id")) == task_id), None)
            if not task:
                return self._json({"ok": False, "message": "当前工作流中找不到该任务"}, 404)
            steps = task.get("steps") or []
            if step_index >= len(steps):
                return self._json({"ok": False, "message": "找不到该子步骤"}, 404)
            step = steps[step_index]
            if not isinstance(step, dict):
                return self._json({"ok": False, "message": "子步骤格式无效"}, 400)
            target = _step_target_file(task, step, step_index)
            if not target:
                return self._json({"ok": False, "message": "该子步骤没有关联的目标文件"}, 400)
            file_path = _workspace_path(ws_dir, target)
            if not file_path:
                return self._json({"ok": False, "path": target,
                                   "message": "路径不在运行工作区内：%s" % target}, 400)
            if os.path.isfile(file_path):
                try:
                    with open(file_path, "rb") as f:
                        data = f.read(200001)
                except OSError as exc:
                    return self._json({"ok": False, "path": target,
                                       "message": "读取工作区文件失败：%s" % exc}, 500)
                code = data[:200000].decode("utf-8", "replace")
                payload = {"ok": True, "source": "workspace", "run_id": run_id,
                           "task_id": task_id, "step_index": step_index,
                           "path": target, "code": code,
                           "truncated": len(data) > 200000,
                           "saved": False, "executed": False}
                return self._json(_attach_symbols(payload, code, target))
            cache_path = _preview_cache_file(run_id, task_id, step_index, target)
            if os.path.isfile(cache_path):
                try:
                    with open(cache_path, "rb") as f:
                        data = f.read(200001)
                except OSError as exc:
                    return self._json({"ok": False, "path": target,
                                       "message": "读取生成缓存失败：%s" % exc}, 500)
                code = data[:200000].decode("utf-8", "replace")
                payload = {"ok": True, "source": "cache", "run_id": run_id,
                           "task_id": task_id, "step_index": step_index,
                           "path": target, "code": code,
                           "truncated": len(data) > 200000,
                           "cached": True, "saved": False, "executed": False}
                return self._json(_attach_symbols(payload, code, target))
            # 模拟运行没写文件：按需生成一次并缓存，之后点同一子步骤直接读缓存，不再调模型
            node = next((n for n in (definition.get("nodes") or [])
                         if str(n.get("task_id")) == task_id), None)
            ai_id = ENGINE._ai_for(node) if node else ""
            try:
                code = _generate_step_code(task, step, target, ai_id or None)
            except llm_client.LLMNotConfigured as exc:
                return self._json({"ok": False, "path": target, "message": str(exc)}, 400)
            except llm_client.LLMError as exc:
                return self._json({"ok": False, "path": target,
                                   "code": getattr(exc, "code", "E-AGENT"),
                                   "message": str(exc)}, 502)
            cached = True
            try:
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, "wb") as f:
                    f.write(code.encode("utf-8"))
            except OSError:
                cached = False
            payload = {"ok": True, "source": "generated", "run_id": run_id,
                       "task_id": task_id, "step_index": step_index,
                       "path": target, "code": code, "truncated": False,
                       "cached": cached, "saved": False, "executed": False}
            return self._json(_attach_symbols(payload, code, target))
        if path == "/api/ais":
            res = SETTINGS.upsert_ai(body or {})
            return self._json({"ok": True, "ai": res, "settings": SETTINGS.public()})
        if path == "/api/assign":
            mapping = {}
            for scope in ("role_ais", "part_ais"):
                if isinstance(body.get(scope), dict):
                    merged = dict(SETTINGS.data.get(scope) or {})
                    for k, v in body[scope].items():
                        if v:
                            merged[str(k)] = str(v)
                        else:
                            merged.pop(str(k), None)
                    mapping[scope] = merged
            if mapping:
                SETTINGS.save(mapping)
            return self._json({"ok": True, "settings": SETTINGS.public()})
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "ais"
                and parts[3] == "delete"):
            SETTINGS.delete_ai(parts[2])
            return self._json({"ok": True, "settings": SETTINGS.public()})
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "ais"
                and parts[3] == "probe"):
            try:
                res = LLM.probe(parts[2])
            except Exception as exc:
                return self._json({"ok": False,
                                   "code": getattr(exc, "code", "E-AGENT"),
                                   "message": str(exc)}, 400)
            return self._json({"ok": True, "probe": res, "settings": SETTINGS.public()})
        if path == "/api/settings":
            patch = dict(body or {})
            if patch.pop("clear_key", False):
                SETTINGS.clear_key(patch.pop("ai_id", None))
                return self._json({"ok": True, "cleared": True,
                                   "settings": SETTINGS.public()})
            # 空字符串 = "不改密钥"，避免前端回填掩码时把真 key 覆盖掉
            elif "api_key" in patch and not str(patch.get("api_key") or "").strip():
                patch.pop("api_key")
            SETTINGS.save(patch)
            active = SETTINGS.public()["active"]
            ENGINE.configure(agent="llm" if LOCAL_CODE_EXECUTION_ENABLED and active else "mock")
            return self._json({"ok": True, "settings": SETTINGS.public()})
        if path == "/api/settings/probe":
            try:
                res = LLM.probe()
            except Exception as exc:
                # 探测失败是"诊断结果"，不是请求错误：回 200 + ok:false，
                # 前端照样能显示原因，浏览器控制台也不会多一条 400。
                return self._json({"ok": False, "code": getattr(exc, "code", "E-AGENT"),
                                   "message": str(exc),
                                   "settings": SETTINGS.public()})
            return self._json({"ok": True, "probe": res, "settings": SETTINGS.public()})
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "specs"
                and parts[3] == "decompose"):
            return self._decompose_spec(parts[2])
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "specs"
                and parts[3] == "preview"):
            return self._preview_spec(parts[2], body)
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "specs"
                and parts[3] == "plan"):
            return self._plan_spec(parts[2], body)
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "specs"
                and parts[3] == "discuss"):
            return self._plan_discuss(parts[2], body)
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "plan"
                and parts[3] == "say"):
            res = PLANNER.say(parts[2], body.get("text") or "", body.get("to") or "")
            return self._json(res, 200 if res.get("ok") else 404)
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "plan"
                and parts[3] == "decide"):
            res = PLANNER.decide(parts[2], body.get("action") or "accept",
                                 body.get("note") or "")
            return self._json(res, 200 if res.get("ok") else 400)
        if (len(parts) == 4 and parts[0] == "api" and parts[1] == "plan"
                and parts[3] == "cancel"):
            ok = PLANNER.cancel(parts[2])
            return self._json({"ok": ok, "session": PLANNER.get(parts[2])},
                              200 if ok else 404)
        if (len(parts) == 3 and parts[0] == "api" and parts[1] == "plan"
                and parts[2]):
            return self._plan_apply(parts[2], body)
        if path == "/api/workflows/generate":
            return self._generate(body)
        if path == "/api/run/start":
            if body.get("speed") is not None:
                ENGINE.configure(speed=body["speed"])
            if body.get("demo") is not None:
                ENGINE.configure(demo=body["demo"])
            if body.get("qa_mode"):
                ENGINE.configure(qa_mode=body["qa_mode"])
            wid = STORE.default_id()
            wf = STORE.get(wid) if wid else None
            definition = wf["draft"]["definition"] if wf else None
            if definition:
                valid, errors, _, _ = validate_workflow(definition)
                if not valid:
                    return self._json({"ok": False, "message": "定义校验未通过，拒绝启动",
                                       "errors": errors}, 400)
            ok, msg = ENGINE.start(definition)
            return self._json({"ok": ok, "message": msg,
                               "run_id": ENGINE.run["run_id"],
                               "version": ENGINE.run["workflow_version"]}, 200 if ok else 409)
        if path == "/api/run/pause":
            ok, msg = ENGINE.pause()
            return self._json({"ok": ok, "message": msg}, 200 if ok else 409)
        if path == "/api/run/resume":
            ok, msg = ENGINE.resume()
            return self._json({"ok": ok, "message": msg}, 200 if ok else 409)
        if path == "/api/run/cancel":
            ok, msg = ENGINE.cancel()
            return self._json({"ok": ok, "message": msg}, 200 if ok else 409)
        if path == "/api/run/reset":
            ok, msg = ENGINE.reset()
            return self._json({"ok": ok, "message": msg})
        if path == "/api/config":
            ok, msg = ENGINE.configure(speed=body.get("speed"), demo=body.get("demo"),
                                       qa_mode=body.get("qa_mode"),
                                       fail_mode=body.get("fail_mode"),
                                       agent=body.get("agent"))
            return self._json({"ok": ok, "message": msg})
        if path == "/api/demo/inject-failure":
            ok, msg = ENGINE.inject_failure()
            return self._json({"ok": ok, "message": msg})
        if path == "/api/validate":
            wf = body.get("workflow")
            if not wf:
                wid = STORE.default_id()
                stored = STORE.get(wid) if wid else None
                wf = stored["draft"]["definition"] if stored else WORKFLOW
            ok, errors, warnings, stats = validate_workflow(wf)
            return self._json({"valid": ok, "errors": errors, "warnings": warnings, "stats": stats})

        # /api/workflows/{id}/publish  与  /api/workflows/{id}/runs
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "workflows":
            wf_id, action = parts[2], parts[3]
            wf = STORE.get(wf_id)
            if not wf:
                return self._json({"ok": False, "message": "工作流不存在"}, 404)
            if action == "publish":
                valid, errors, warnings, stats = validate_workflow(wf["draft"]["definition"])
                if not valid:
                    return self._json({"ok": False, "message": "校验未通过，不能发布",
                                       "errors": errors}, 400)
                res, err = STORE.publish(wf_id, body.get("note", ""))
                if err:
                    return self._json({"ok": False, "message": err}, 404)
                return self._json({"ok": True, "message": "已发布版本 " + res["version"],
                                   "version": res["version"], "warnings": warnings})
            if action == "runs":
                label = body.get("version")
                definition = STORE.version(wf_id, label) if label else wf["draft"]["definition"]
                if not definition:
                    return self._json({"ok": False, "message": "版本不存在：%s" % label}, 404)
                valid, errors, _, _ = validate_workflow(definition)
                if not valid:
                    return self._json({"ok": False, "message": "定义校验未通过，拒绝启动",
                                       "errors": errors}, 400)
                if body.get("speed") is not None:
                    ENGINE.configure(speed=body["speed"])
                if body.get("demo") is not None:
                    ENGINE.configure(demo=body["demo"])
                if body.get("qa_mode"):
                    ENGINE.configure(qa_mode=body["qa_mode"])
                ok, msg = ENGINE.start(definition)
                return self._json({"ok": ok, "message": msg,
                                   "run_id": ENGINE.run["run_id"],
                                   "version": definition.get("version")}, 200 if ok else 409)

        # /api/nodes/{id}/{action}
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "nodes":
            node_id, action = parts[2], parts[3]
            if action == "approval":
                ok, msg = ENGINE.approve(node_id, body.get("decision", "approve"),
                                         body.get("comment", ""))
            elif action == "retry":
                ok, msg = ENGINE.retry_node(node_id)
            elif action == "skip":
                ok, msg = ENGINE.skip_node(node_id)
            elif action == "confirm":
                ok, msg = ENGINE.confirm_node(node_id)
            else:
                return self._json({"ok": False, "message": "unknown action"}, 404)
            return self._json({"ok": ok, "message": msg}, 200 if ok else 409)

        return self._json({"ok": False, "message": "not found"}, 404)

    # ---------- SSE ----------
    def _stream(self):
        self.protocol_version = "HTTP/1.0"
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
        except Exception:
            return

        # 断线续接：优先 Last-Event-ID 头（浏览器自动携带），其次 ?last_seq=N
        last_seq = 0
        try:
            qs = parse_qs(urlparse(self.path).query)
            last_seq = int((qs.get("last_seq") or ["0"])[0] or 0)
        except Exception:
            last_seq = 0
        header_last = self.headers.get("Last-Event-ID")
        if header_last:
            try:
                last_seq = max(last_seq, int(header_last))
            except (TypeError, ValueError):
                pass

        last_ver = -1
        try:
            self.wfile.write(b"retry: 1500\n\n")
            self.wfile.flush()
            while True:
                with ENGINE.lock:
                    if ENGINE.version == last_ver:
                        ENGINE.cond.wait(1.5)
                    ver = ENGINE.version
                    events = [e for e in ENGINE.run["events"] if e["seq"] > last_seq]
                    snap = {
                        "version": ver,
                        "run": {k: ENGINE.run.get(k) for k in
                                ("run_id", "workflow_id", "workflow_version", "requirement",
                                 "status", "elapsed", "speed", "demo", "qa_mode", "fail_mode",
                                 "concurrency", "stats", "seq", "agent", "recovered")},
                        "nodes": ENGINE._slim_nodes(),
                        "edges": [dict(e) for e in ENGINE.run["edges"]],
                        "events": events[-40:],
                    }
                if ver != last_ver or events:
                    if events:
                        last_seq = events[-1]["seq"]
                    payload = json.dumps(snap, ensure_ascii=False)
                    self.wfile.write(("id: %d\n" % last_seq).encode())
                    self.wfile.write(("event: patch\n").encode())
                    self.wfile.write(("data: " + payload + "\n\n").encode())
                    self.wfile.flush()
                    last_ver = ver
                else:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8010)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    if not os.path.isdir(STATIC_DIR):
        print("缺少 static 目录：", STATIC_DIR)
        sys.exit(1)

    try:
        srv = AutoFlowServer((args.host, args.port), Handler)
    except OSError as e:
        msg = str(e).lower()
        if "10048" in msg or "address already in use" in msg or getattr(e, "errno", None) in (48, 98, 10048):
            print("端口 %d 已被占用，服务很可能已经在运行：" % args.port)
            print("   http://%s:%d" % (args.host, args.port))
            print("如果打不开，先结束占用该端口的进程：netstat -ano | findstr :%d" % args.port)
            sys.exit(0)
        raise
    srv.daemon_threads = True
    wid = STORE.default_id()
    wf = STORE.get(wid) if wid else None
    print("=" * 62)
    print(" AutoFlow 编排器原型已启动")
    print("   http://%s:%d" % (args.host, args.port))
    if wf:
        print(" 工作流：%s  已发布 %d 个版本（最新 %s）"
              % (wf["name"], len(wf["versions"]),
                 wf["versions"][-1]["version"] if wf["versions"] else "—"))
        print(" 定义库：%s" % STORE_PATH)
    print(" 按 Ctrl+C 停止")
    print("=" * 62)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
