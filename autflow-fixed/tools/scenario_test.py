#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""场景测试：分支走向、审批打回、暂停恢复、DAG 校验。

用法: python tools/scenario_test.py [base_url]
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"
PASSED = []


def call(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode() or "{}")


def state():
    return call("/api/state")


def wait_until(pred, timeout=40, tick=0.2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = state()
        if pred(s):
            return s
        time.sleep(tick)
    return state()


def check(cond, msg):
    PASSED.append(bool(cond))
    print(("  ✓ " if cond else "  ✗ ") + msg)
    return cond


def by_id(s):
    return {n["id"]: n for n in s["nodes"]}


# ---------------------------------------------------------------- 场景 1
def scenario_branch():
    print("\n[场景 1] 顺利模式：应命中 TRUE 分支，FALSE 分支被跳过")
    call("/api/run/reset", {})
    call("/api/run/start", {"speed": 6, "demo": False, "qa_mode": "pass"})
    s = wait_until(lambda s: s["run"]["status"] in ("WAITING_HUMAN", "SUCCESS", "FAILED"))
    if s["run"]["status"] == "WAITING_HUMAN":
        n = [x for x in s["nodes"] if x["status"] == "WAITING_HUMAN"][0]
        call("/api/nodes/%s/approval" % n["id"], {"decision": "approve"})
        s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
    b = by_id(s)
    check(s["run"]["status"] == "SUCCESS", "运行成功结束")
    check(b["N5"]["branch_taken"] == "true", "条件节点命中 TRUE")
    check(b["N7"]["status"] == "SKIPPED", "FALSE 分支 N7 被跳过")
    check(b["N6"]["status"] == "SUCCESS", "TRUE 分支 N6 正常执行")
    check(len(b["N3"]["attempts"]) == 1, "顺利模式无格式错误（N3 一次通过）")


# ---------------------------------------------------------------- 场景 2
def scenario_reject():
    print("\n[场景 2] 审批打回 → 人工重试 → 再次审批")
    call("/api/run/reset", {})
    call("/api/run/start", {"speed": 6, "demo": False, "qa_mode": "pass"})
    s = wait_until(lambda s: s["run"]["status"] == "WAITING_HUMAN")
    check(any(n["status"] == "WAITING_HUMAN" and n["type"] == "approval" for n in s["nodes"]),
          "流程在审批节点挂起")

    call("/api/nodes/N9/approval", {"decision": "reject", "comment": "缺少回滚方案"})
    time.sleep(0.4)
    s = state()
    n9 = by_id(s)["N9"]
    check(s["run"]["status"] == "WAITING_HUMAN", "打回后流程保持挂起，不误报完成")
    check(n9["gate"].get("human") == "fail", "人工门记录为 fail")
    check("打回" in (n9.get("message") or "") or "打回" in "".join(n9.get("logs") or []),
          "打回意见已留痕")

    r = call("/api/nodes/N9/retry", {})
    check(r.get("ok"), "人工重试被接受")
    s = wait_until(lambda s: any(n["status"] == "WAITING_HUMAN" for n in s["nodes"]), timeout=20)
    check(any(n["status"] == "WAITING_HUMAN" for n in s["nodes"]), "重试后再次进入待审批")

    call("/api/nodes/N9/approval", {"decision": "approve", "comment": "补充回滚方案后同意"})
    s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
    check(s["run"]["status"] == "SUCCESS", "二次审批通过后运行完成")
    check(by_id(s)["N9"]["output"].get("comment") == "补充回滚方案后同意", "审批意见写入产物")


# ---------------------------------------------------------------- 场景 3
def scenario_pause():
    print("\n[场景 3] 暂停：应停止派发新任务，恢复后继续")
    call("/api/run/reset", {})
    call("/api/run/start", {"speed": 1, "demo": False, "qa_mode": "pass"})
    time.sleep(1.2)
    r = call("/api/run/pause", {})
    check(r.get("ok"), "暂停被接受")
    s1 = state()
    time.sleep(2.0)
    s2 = state()
    disp1 = s1["run"]["stats"]["dispatched"]
    disp2 = s2["run"]["stats"]["dispatched"]
    check(disp2 <= disp1 + 1, "暂停期间派发计数基本停止（%d → %d）" % (disp1, disp2))
    check(s2["run"]["status"] == "PAUSED", "运行状态保持 PAUSED")
    call("/api/run/resume", {})
    s = wait_until(lambda s: s["run"]["status"] in ("WAITING_HUMAN", "SUCCESS", "FAILED"), timeout=60)
    if s["run"]["status"] == "WAITING_HUMAN":
        call("/api/nodes/N9/approval", {"decision": "approve"})
        s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
    check(s["run"]["status"] == "SUCCESS", "恢复后运行至完成")


# ---------------------------------------------------------------- 场景 4
def scenario_validate():
    print("\n[场景 4] DAG 校验：正常图通过，环依赖被拒绝")
    wf = call("/api/workflow")["workflow"]
    r = call("/api/validate", {"workflow": wf})
    check(r["valid"], "正常图校验通过（%d 节点 / %d 边 / 关键路径 %d 层）"
          % (r["stats"]["nodes"], r["stats"]["edges"], len(r["stats"]["critical_path"])))

    bad = json.loads(json.dumps(wf))
    bad["nodes"][1]["deps"].append("N10")
    bad["edges"].append({"source": "N10", "target": "N2"})
    rb = call("/api/validate", {"workflow": bad})
    codes = [e["code"] for e in rb["errors"]]
    check(not rb["valid"], "环依赖图被拒绝")
    check("E-CYCLE" in codes, "错误码包含 E-CYCLE：" + "；".join(e["msg"] for e in rb["errors"])[:90])

    bad2 = json.loads(json.dumps(wf))
    bad2["nodes"][2]["deps"].append("N99")
    rb2 = call("/api/validate", {"workflow": bad2})
    check(any(e["code"] == "E-DANGLING" for e in rb2["errors"]), "悬空依赖被检出（E-DANGLING）")


def scenario_escalate():
    print("\n[场景 5] 重试耗尽 → 转人工 → 下游阻断 → 恢复")
    call("/api/run/reset", {})
    call("/api/run/start", {"speed": 2, "demo": False, "qa_mode": "pass"})
    call("/api/config", {"fail_mode": "persistent"})
    s = wait_until(lambda s: s["run"]["status"] == "WAITING_HUMAN", timeout=45)
    b = by_id(s)
    check(b["N3"]["status"] == "WAITING_HUMAN", "N3 重试耗尽后转人工（WAITING_HUMAN）")
    check(len(b["N3"]["attempts"]) == 4, "N3 共尝试 4 次（1 次执行 + 3 次重试），实际 %d"
          % len(b["N3"]["attempts"]))
    s = wait_until(lambda s: by_id(s)["N5"]["status"] == "BLOCKED", timeout=8)
    check(by_id(s)["N5"]["status"] == "BLOCKED", "下游 N5 被标记 BLOCKED")
    check(s["run"]["status"] == "WAITING_HUMAN", "运行整体挂起，不误报完成")

    call("/api/config", {"fail_mode": "off"})
    r = call("/api/nodes/N3/retry", {})
    check(r.get("ok"), "排除故障后人工重试被接受")
    s = wait_until(lambda s: by_id(s)["N8"]["status"] in ("SUCCESS", "SKIPPED"), timeout=45)
    b = by_id(s)
    check(b["N3"]["status"] == "SUCCESS", "N3 恢复成功")
    check(b["N5"]["status"] == "SUCCESS", "被阻断的 N5 自动解阻并执行")
    s = wait_until(lambda s: s["run"]["status"] == "WAITING_HUMAN", timeout=20)
    call("/api/nodes/N9/approval", {"decision": "approve"})
    s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
    check(s["run"]["status"] == "SUCCESS", "恢复后整条流程跑完")


def main():
    scenario_branch()
    scenario_reject()
    scenario_pause()
    scenario_validate()
    scenario_escalate()
    print("\n结果:", "PASS" if all(PASSED) else "FAIL", "(%d/%d)" % (sum(PASSED), len(PASSED)))
    return 0 if all(PASSED) else 1


if __name__ == "__main__":
    sys.exit(main())