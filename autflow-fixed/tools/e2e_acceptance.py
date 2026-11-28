#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""C 任务 · 需求到历史归档的完整 API 端到端验收。

覆盖：需求生成 → 编辑 DAG → 非法图拦截 → 保存 → 发布 → 按版本运行 →
运行中状态 → 审批打回/重试/通过 → 终态 → 历史列表与详情。
"""
import copy
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"
ARTIFACT_DIR = sys.argv[2] if len(sys.argv) > 2 else ""
OK = []
EVIDENCE = {}


def call(path, body=None, method=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, headers={"Content-Type": "application/json"},
        method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=20) as res:
            return res.status, json.loads(res.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def check(condition, message):
    OK.append(bool(condition))
    print(("  ✓ " if condition else "  ✗ ") + message)
    return condition


def wait_state(predicate, timeout=90, tick=0.15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, last = call("/api/state")
        if code == 200 and predicate(last):
            return last
        time.sleep(tick)
    return last


def by_id(state):
    return {node["id"]: node for node in state.get("nodes", [])}


def read_sse(last_seq, use_header=False):
    path = "/api/stream?last_seq=%d" % max(0, last_seq)
    headers = {}
    if use_header:
        headers["Last-Event-ID"] = str(max(0, last_seq))
    req = urllib.request.Request(BASE + path, headers=headers)
    event_id = None
    payload = None
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            for _ in range(20):
                line = res.readline().decode("utf-8").strip()
                if line.startswith("id:"):
                    event_id = int(line.split(":", 1)[1].strip())
                elif line.startswith("data:"):
                    payload = json.loads(line.split(":", 1)[1].strip())
                if payload is not None:
                    break
    except (TimeoutError, urllib.error.URLError):
        return None, None
    return event_id, payload


def save_evidence():
    if not ARTIFACT_DIR:
        return
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    EVIDENCE["result"] = "PASS" if all(OK) else "FAIL"
    EVIDENCE["passed"] = sum(OK)
    EVIDENCE["total"] = len(OK)
    with open(os.path.join(ARTIFACT_DIR, "api-e2e-evidence.json"), "w",
              encoding="utf-8") as stream:
        json.dump(EVIDENCE, stream, ensure_ascii=False, indent=2)


def main():
    print("[1] 服务与编辑器基线")
    code, health = call("/api/health")
    check(code == 200 and health.get("ok"), "健康检查正常")
    code, agents = call("/api/agents")
    check(code == 200 and "mock" in agents.get("available", []),
          "执行器接口可用，默认 mock")
    call("/api/run/reset", {})
    code, editor = call("/api/editor")
    check(code == 200 and editor.get("ok"), "编辑器初始化正常")
    workflow_id = editor["workflow_id"]
    version_count = len(editor.get("versions", []))

    print("\n[2] 输入需求并生成 DAG")
    requirement = ("验收：做一个内部管理后台，支持用户名密码登录、订单支付结算、"
                   "报表统计导出，并要求权限审计")
    code, generated = call("/api/workflows/generate", {"requirement": requirement})
    check(code == 200 and generated.get("ok"), "需求生成成功")
    check(len(generated.get("trace", [])) >= 4, "返回生成依据 trace")
    check("认证与账号" in generated.get("matched", []), "识别登录认证能力域")
    check("交易与支付" in generated.get("matched", []), "识别支付交易能力域")
    check(bool(generated.get("risk")), "识别风险词并追加安全评审")
    draft = generated["draft"]["definition"]
    ids = {node["id"] for node in draft["nodes"]}
    check(len(draft["nodes"]) >= 10, "生成 %d 个节点" % len(draft["nodes"]))
    check(all(edge["source"] in ids and edge["target"] in ids
              for edge in draft["edges"]), "生成图所有边端点有效")
    code, validated = call("/api/validate", {"workflow": draft})
    check(code == 200 and validated.get("valid"), "生成图通过服务端 DAG 校验")
    check(not validated.get("warnings"), "生成图无孤立节点告警")
    EVIDENCE["requirement"] = requirement
    EVIDENCE["generated_nodes"] = len(draft["nodes"])

    print("\n[3] 画布编辑结果保存与非法图拦截")
    edited = copy.deepcopy(draft)
    approval = next(node for node in edited["nodes"] if node["type"] == "approval")
    predecessor = approval["deps"][0]
    inserted_id = "N90"
    edited["nodes"].append({
        "id": inserted_id, "type": "agent", "name": "验收补充复核",
        "role": "qa", "deps": [predecessor], "max_retries": 1,
        "out_schema": "verify-v1", "pos": {"x": 620, "y": 760},
    })
    edited["edges"] = [
        edge for edge in edited["edges"]
        if not (edge["source"] == predecessor and edge["target"] == approval["id"])
    ]
    edited["edges"].extend([
        {"source": predecessor, "target": inserted_id},
        {"source": inserted_id, "target": approval["id"]},
    ])
    approval["deps"] = [inserted_id]

    bad = copy.deepcopy(edited)
    start = next(node for node in bad["nodes"] if node["type"] == "start")
    end = next(node for node in bad["nodes"] if node["type"] == "end")
    start["deps"].append(end["id"])
    bad["edges"].append({"source": end["id"], "target": start["id"]})
    code, rejected = call("/api/workflows/" + workflow_id,
                          {"definition": bad}, "PUT")
    check(code == 400 and not rejected.get("ok"), "环依赖草稿被拒绝")
    check(any(error.get("code") == "E-CYCLE"
              for error in rejected.get("errors", [])), "错误码为 E-CYCLE")

    code, saved = call("/api/workflows/" + workflow_id,
                       {"definition": edited}, "PUT")
    check(code == 200 and saved.get("ok"), "合法编辑草稿保存成功")
    saved_ids = {node["id"] for node in saved["draft"]["definition"]["nodes"]}
    check(inserted_id in saved_ids, "新增复核节点已落库")
    code, published = call("/api/workflows/%s/publish" % workflow_id,
                           {"note": "C 验收发布"})
    check(code == 200 and published.get("ok"), "发布不可变版本")
    run_version = published.get("version")
    check(isinstance(run_version, str) and run_version.startswith("v"),
          "发布版本号有效：%s" % run_version)
    code, versions = call("/api/workflows/%s/versions" % workflow_id)
    check(len(versions.get("versions", [])) == version_count + 1,
          "版本数量从 %d 增至 %d" % (
              version_count, len(versions.get("versions", []))))
    EVIDENCE["published_version"] = run_version

    print("\n[4] 按发布版本运行并观察状态")
    code, started = call("/api/workflows/%s/runs" % workflow_id, {
        "version": run_version, "speed": 8, "demo": False, "qa_mode": "pass",
    })
    check(code == 200 and started.get("ok"), "按发布版本启动运行")
    run_id = started.get("run_id")
    check(bool(run_id), "返回 Run ID：%s" % run_id)
    running = wait_state(
        lambda state: state["run"]["status"] in ("RUNNING", "WAITING_HUMAN", "SUCCESS"))
    check(running and running["run"]["workflow_version"] == run_version,
          "运行快照绑定发布版本")
    check(running and inserted_id in by_id(running),
          "运行使用人工插入的复核节点")

    waiting = wait_state(
        lambda state: state["run"]["status"] == "WAITING_HUMAN", timeout=90)
    check(waiting is not None and waiting["run"]["status"] == "WAITING_HUMAN",
          "流程停在人工审批")
    waits = [node for node in waiting.get("nodes", [])
             if node["status"] == "WAITING_HUMAN" and node["type"] == "approval"]
    check(len(waits) == 1, "恰有一个审批节点等待处理")
    approval_id = waits[0]["id"] if waits else approval["id"]

    print("\n[5] 任务确认退回指定节点、重跑、再通过，并处理交付审核")
    approval_id = waits[0]["id"] if waits else approval["id"]
    before_reject = by_id(waiting)
    first_approval = before_reject[approval_id]
    return_target = first_approval.get("return_to")
    check(first_approval.get("name") == "需求与任务确认",
          "第一个审核节点是需求与任务确认")
    check(return_target in before_reject,
          "任务确认配置了有效退回目标：%s" % return_target)
    upstream = {approval_id}
    stack = list(first_approval.get("deps") or [])
    while stack:
        current = stack.pop()
        if current in upstream:
            continue
        upstream.add(current)
        current_node = before_reject.get(current) or {}
        stack.extend(current_node.get("deps") or [])
    downstream_running = [
        node["id"] for node in waiting["nodes"]
        if node["id"] not in upstream
        and node["status"] in ("READY", "RUNNING", "SUCCESS")
    ]
    check(not downstream_running,
          "审核期间下游没有执行（异常：%s）" % (downstream_running[:4] or "无"))
    target_attempts = len(before_reject[return_target].get("attempts") or [])
    start_id = next(node["id"] for node in waiting["nodes"] if node["type"] == "start")
    start_attempts = len(before_reject[start_id].get("attempts") or [])
    code, rejected_approval = call(
        "/api/nodes/%s/approval" % approval_id,
        {"decision": "reject", "comment": "C 验收：补充回滚方案"})
    check(code == 200 and rejected_approval.get("ok"), "审核退回请求成功")
    waiting_again = wait_state(
        lambda state: (
            by_id(state).get(return_target, {}).get("attempts")
            and len(by_id(state)[return_target]["attempts"]) > target_attempts
            and any(node["id"] == approval_id
                    and node["status"] == "WAITING_HUMAN"
                    for node in state.get("nodes", []))
        ), timeout=45)
    again = by_id(waiting_again or {})
    check(len(again.get(return_target, {}).get("attempts") or []) > target_attempts,
          "退回目标节点已重新执行（%d → %d）"
          % (target_attempts,
             len(again.get(return_target, {}).get("attempts") or [])))
    check(len(again.get(start_id, {}).get("attempts") or []) == start_attempts,
          "返回点之前的成功节点没有重复执行")
    check(again.get(approval_id, {}).get("status") == "WAITING_HUMAN",
          "退回重跑后再次等待任务确认")
    check(any(event.get("type") == "approval.returned"
              for event in (waiting_again or {}).get("events", [])),
          "时间线记录审核退回事件")

    approval_comment = "C 验收：范围与验收标准确认"
    code, approved = call(
        "/api/nodes/%s/approval" % approval_id,
        {"decision": "approve", "comment": approval_comment})
    check(code == 200 and approved.get("ok"), "任务确认审核通过")
    delivery_wait = wait_state(
        lambda state: any(node["type"] == "approval"
                          and node["id"] != approval_id
                          and node["status"] == "WAITING_HUMAN"
                          for node in state.get("nodes", [])),
        timeout=90)
    delivery = next((node for node in (delivery_wait or {}).get("nodes", [])
                     if node["type"] == "approval"
                     and node["id"] != approval_id
                     and node["status"] == "WAITING_HUMAN"), None)
    check(delivery is not None and delivery.get("name") == "交付审核",
          "测试与评审后进入交付审核")
    delivery_comment = "C 验收：测试结果与交付内容确认"
    code, approved_delivery = call(
        "/api/nodes/%s/approval" % delivery["id"],
        {"decision": "approve", "comment": delivery_comment})
    check(code == 200 and approved_delivery.get("ok"), "交付审核通过")
    final = wait_state(
        lambda state: state["run"]["status"] in ("SUCCESS", "FAILED", "CANCELLED"))
    check(final and final["run"]["status"] == "SUCCESS",
          "两次审核后运行成功结束（实际 %s）" % (
              final and final["run"]["status"]))
    final_map = by_id(final or {})
    check(final_map.get(inserted_id, {}).get("status") == "SUCCESS",
          "新增复核节点成功执行")
    check(final_map.get(approval_id, {}).get("output", {}).get("comment")
          == approval_comment, "审批意见写入节点产物")
    check(final_map.get(delivery["id"], {}).get("output", {}).get("comment")
          == delivery_comment, "交付审核意见写入节点产物")
    check(len((final or {}).get("run", {}).get("approval_history") or []) == 3,
          "运行记录保留三次审核操作")
    check(final and final["run"].get("recovered") is False,
          "正常运行未误标为崩溃恢复")
    EVIDENCE["run_id"] = run_id

    print("\n[6] SSE 断点续接与历史回看")
    seq = final["run"].get("seq", 0)
    expected_last = max(0, seq - 3)
    event_id, patch = read_sse(expected_last)
    check(event_id is not None and event_id >= expected_last,
          "SSE ?last_seq 从断点续推（id=%s）" % event_id)
    check(patch is not None and patch.get("events")
          and min(event["seq"] for event in patch["events"]) > expected_last,
          "SSE 补丁只包含断点之后的事件")
    event_id2, patch2 = read_sse(expected_last, use_header=True)
    check(event_id2 is not None and event_id2 >= expected_last,
          "SSE Last-Event-ID 请求头可续推")
    check(patch2 is not None and patch2.get("run", {}).get("status") == "SUCCESS",
          "SSE 快照与最终服务端状态一致")

    code, history = call("/api/runs")
    check(code == 200, "历史列表接口正常")
    top = (history.get("runs") or [None])[0]
    check(top and top.get("run_id") == run_id, "最新历史为刚才的运行")
    check(top and top.get("status") == "SUCCESS", "历史状态为 SUCCESS")
    check(top and top.get("requirement") == requirement, "历史保留需求原文")
    code, detail = call("/api/runs/" + run_id)
    check(code == 200 and detail.get("run", {}).get("run_id") == run_id,
          "单次历史详情可读取")
    archived = detail.get("run", {})
    check(len(archived.get("definition", {}).get("nodes", [])) == len(edited["nodes"]),
          "历史含当时完整定义快照")
    check(any(node["id"] == inserted_id for node in archived.get("nodes", [])),
          "历史含人工编辑节点")
    check(len(archived.get("events", [])) >= 20, "历史事件流完整")
    EVIDENCE["history_events"] = len(archived.get("events", []))

    save_evidence()
    total = len(OK)
    passed = sum(OK)
    print("\n结果: %s (%d/%d)" % ("PASS" if passed == total else "FAIL",
                                   passed, total))
    return 0 if passed == total else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        save_evidence()
