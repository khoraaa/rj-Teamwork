#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""关键审核关口验收：到点暂停、下游不执行、通过继续、退回指定节点重跑。"""
import copy
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SERVER = os.path.join(ROOT, "server.py")
BASE = ""
PASS, FAIL = [], []


def check(cond, label):
    (PASS if cond else FAIL).append(label)
    print(("  ✓ " if cond else "  ✗ ") + label)
    return bool(cond)


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def start_server():
    port = free_port()
    data_dir = tempfile.mkdtemp(prefix="autflow-approval-")
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    for key in ("AUTFLOW_LLM_BASE_URL", "AUTFLOW_LLM_API_KEY", "AUTFLOW_LLM_MODE"):
        env.pop(key, None)
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port


def call(path, body=None, method=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"},
        method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {"ok": False, "message": raw}
        return exc.code, payload


def state():
    return call("/api/state")[1]


def by_id(snapshot):
    return {node["id"]: node for node in snapshot.get("nodes", [])}


def wait_until(pred, timeout=40, tick=0.15):
    end = time.time() + timeout
    snap = state()
    while time.time() < end:
        snap = state()
        if pred(snap):
            return snap
        time.sleep(tick)
    return snap


def wait_health(timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(BASE + "/api/health", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def generate(requirement, template="default"):
    code, result = call("/api/workflows/generate",
                        {"requirement": requirement, "template": template})
    check(code == 200 and result.get("ok"),
          "%s 模板生成成功" % template)
    return result.get("draft", {}).get("definition", {})


def approval_names(definition):
    return [n.get("name") for n in definition.get("nodes", [])
            if n.get("type") == "approval"]


def main():
    global BASE
    proc, BASE = start_server()
    try:
        check(wait_health(), "隔离测试服务已启动")
        requirement = "做一个登录与订单管理后台，包含测试、评审和交付上线"

        print("\n[1] 三套模板都有两个通用审核关口")
        definitions = {}
        for template in ("default", "lean", "enterprise"):
            definition = generate(requirement, template)
            definitions[template] = definition
            names = approval_names(definition)
            check("需求与任务确认" in names, "%s 含需求与任务确认" % template)
            check("交付审核" in names, "%s 含交付审核" % template)
            approvals = [n for n in definition.get("nodes", []) if n.get("type") == "approval"]
            common = [n for n in approvals if n.get("name") in ("需求与任务确认", "交付审核")]
            check(all(n.get("return_to") for n in common),
                  "%s 的两个通用审核都配置了 return_to" % template)
            code, valid = call("/api/validate", {"workflow": definition})
            check(code == 200 and valid.get("valid"),
                  "%s 模板审核链路通过 DAG 校验" % template)

        print("\n[2] 非法退回目标被校验器拒绝")
        bad = copy.deepcopy(definitions["default"])
        first = next(n for n in bad["nodes"] if n.get("name") == "需求与任务确认")
        first["return_to"] = first["id"]
        _, invalid = call("/api/validate", {"workflow": bad})
        check(not invalid.get("valid"), "审核退回自身被拒绝")
        check(any(e.get("code") == "E-APPROVAL-RETURN"
                  for e in invalid.get("errors", [])),
              "错误码为 E-APPROVAL-RETURN")

        print("\n[3] 任务确认暂停，下游没有执行")
        definition = generate(requirement, "default")
        start = call("/api/run/start",
                     {"speed": 8, "demo": False, "qa_mode": "pass"})[1]
        check(start.get("ok"), "生成的工作流启动成功")
        snap = wait_until(lambda s: s.get("run", {}).get("status") == "WAITING_HUMAN")
        nodes = by_id(snap)
        waiting = [n for n in snap.get("nodes", [])
                   if n.get("status") == "WAITING_HUMAN" and n.get("type") == "approval"]
        check(len(waiting) == 1, "停在第一个审核节点")
        first = waiting[0]
        pm_id = first.get("return_to")
        check(first.get("name") == "需求与任务确认",
              "第一个审核是需求与任务确认（实际 %s）" % first.get("name"))
        check(pm_id in nodes, "return_to 指向真实节点 %s" % pm_id)
        check(nodes[pm_id].get("role") == "pm", "退回目标是任务拆解 PM 节点")
        downstream_active = [
            n["id"] for n in snap.get("nodes", [])
            if n["id"] not in (first["id"], pm_id)
            and n.get("status") in ("READY", "RUNNING", "SUCCESS")
            and n.get("type") != "start"
        ]
        check(not downstream_active,
              "审核期间下游不执行（异常活动：%s）" % (downstream_active[:4] or "无"))
        pm_attempts = len(nodes[pm_id].get("attempts") or [])
        start_attempts = len(nodes["N1"].get("attempts") or [])

        print("\n[4] 退回修改：回到指定节点，无关上游不重跑")
        code, rejected = call(
            "/api/nodes/%s/approval" % first["id"],
            {"decision": "reject", "comment": "验收标准不完整，补充分页和权限边界"})
        check(code == 200 and rejected.get("ok"), "退回修改请求成功")
        again = wait_until(
            lambda s: any(n.get("id") == first["id"]
                          and n.get("status") == "WAITING_HUMAN"
                          for n in s.get("nodes", [])),
            timeout=30)
        again_nodes = by_id(again)
        check(len(again_nodes[pm_id].get("attempts") or []) > pm_attempts,
              "指定 PM 节点重新执行（%d → %d）"
              % (pm_attempts, len(again_nodes[pm_id].get("attempts") or [])))
        check(len(again_nodes["N1"].get("attempts") or []) == start_attempts,
              "已成功的起始节点没有重复执行")
        check(again_nodes[first["id"]].get("status") == "WAITING_HUMAN",
              "退回重跑后再次等待人工审核")
        check(again_nodes[first["id"]].get("return_count") == 1,
              "审核节点记录已退回 1 轮")

        print("\n[5] 两次审核通过后完成，并保留审核历史")
        code, approved = call(
            "/api/nodes/%s/approval" % first["id"],
            {"decision": "approve", "comment": "范围与验收标准确认"})
        check(code == 200 and approved.get("ok"), "任务确认审核通过")
        second = wait_until(
            lambda s: any(n.get("status") == "WAITING_HUMAN"
                          and n.get("type") == "approval"
                          and n.get("id") != first["id"]
                          for n in s.get("nodes", [])),
            timeout=60)
        second_wait = [n for n in second.get("nodes", [])
                       if n.get("status") == "WAITING_HUMAN"
                       and n.get("type") == "approval"
                       and n.get("id") != first["id"]]
        check(len(second_wait) == 1, "走到第二个审核节点")
        delivery = second_wait[0] if second_wait else {}
        check(delivery.get("name") == "交付审核",
              "第二个审核是交付审核（实际 %s）" % delivery.get("name"))
        second_nodes = by_id(second)
        check(all(second_nodes[d].get("status") in ("SUCCESS", "SKIPPED")
                  for d in delivery.get("deps") or [] if d in second_nodes),
              "测试与评审链路完成后才进入交付审核")
        code, approved = call(
            "/api/nodes/%s/approval" % delivery["id"],
            {"decision": "approve", "comment": "测试与交付内容确认"})
        check(code == 200 and approved.get("ok"), "交付审核通过")
        final = wait_until(
            lambda s: s.get("run", {}).get("status") in ("SUCCESS", "FAILED"),
            timeout=60)
        check(final.get("run", {}).get("status") == "SUCCESS",
              "审核通过后运行成功结束")
        history = final.get("run", {}).get("approval_history") or []
        check(len(history) == 3, "运行记录保留 3 次审核操作（实际 %d）" % len(history))
        check(any(h.get("decision") == "REJECTED" and h.get("return_to") == pm_id
                  for h in history), "审核历史记录退回目标和意见")
        check(any(e.get("type") == "approval.returned"
                  for e in final.get("events", [])),
              "时间线记录审核退回事件")

        print("\n结果:", "PASS" if not FAIL else "FAIL",
              "(%d/%d)" % (len(PASS), len(PASS) + len(FAIL)))
        return 0 if not FAIL else 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
