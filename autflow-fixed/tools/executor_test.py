#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""B 任务 · 执行器接口 / 超时 / 错误分类 / 取消 接口测试。

覆盖：
  - 可替换 Agent 接口：GET /api/agents 暴露 mock / llm，默认 mock；
  - 超时注入：E-TIMEOUT 为可重试错误，单次超时自动重试后成功；
  - 持续超时：耗尽重试后转人工，下游阻断；
  - 错误分类：不可重试错误（E-AGENT）不消耗重试次数，直接转人工；
  - 取消：运行中取消后状态正确并归档。

用法: python tools/executor_test.py [base_url]
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"
OK = []


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def check(cond, msg):
    OK.append(bool(cond))
    print(("  ✓ " if cond else "  ✗ ") + msg)
    return cond


def state():
    return call("/api/state")[1]


def by_id(s):
    return {n["id"]: n for n in s["nodes"]}


def wait_until(pred, timeout=60, tick=0.2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = state()
        if pred(s):
            return s
        time.sleep(tick)
    return state()


print("[1] 可替换 Agent 接口")
code, r = call("/api/agents")
check(code == 200, "GET /api/agents 正常")
check(set(r.get("available", [])) >= {"mock", "llm"}, "已注册 mock / llm 两个执行器")
check(r.get("current") == "mock", "默认执行器为 mock（无需外部 API）")
s = state()
check(s["run"].get("agent") == "mock", "运行快照携带当前执行器字段")

print("\n[2] 单次超时 → 可重试 → 自动恢复")
call("/api/run/reset", {})
call("/api/run/start", {"speed": 2, "demo": False, "qa_mode": "pass"})
call("/api/config", {"fail_mode": "timeout"})
s = wait_until(lambda s: by_id(s)["N3"]["status"] in ("SUCCESS", "WAITING_HUMAN"))
b = by_id(s)
check(b["N3"]["status"] == "SUCCESS", "单次超时后 N3 自动重试并成功")
check(len(b["N3"]["attempts"]) >= 2, "N3 至少尝试 2 次（超时 + 重试），实际 %d" % len(b["N3"]["attempts"]))
check(b["N3"]["attempts"][0].get("error_code") == "E-TIMEOUT", "首次尝试记录 E-TIMEOUT")
check(b["N3"]["retry_count"] >= 1, "超时触发自动重试计数（retry=%d）" % b["N3"]["retry_count"])
call("/api/run/cancel", {})

print("\n[3] 持续超时 → 耗尽重试 → 转人工 → 下游阻断")
call("/api/run/reset", {})
call("/api/run/start", {"speed": 4, "demo": False, "qa_mode": "pass"})
call("/api/config", {"fail_mode": "timeout_persistent"})
s = wait_until(lambda s: by_id(s)["N3"]["status"] == "WAITING_HUMAN", timeout=60)
b = by_id(s)
check(b["N3"]["status"] == "WAITING_HUMAN", "持续超时耗尽重试后转人工")
check(len(b["N3"]["attempts"]) == 4, "N3 共 4 次尝试（1 次执行 + 3 次重试），实际 %d"
      % len(b["N3"]["attempts"]))
check(all(a.get("error_code") == "E-TIMEOUT" for a in b["N3"]["attempts"]),
      "每次尝试的错误码都是 E-TIMEOUT")
check(s["run"]["status"] == "WAITING_HUMAN", "运行整体挂起，不误报完成")
call("/api/config", {"fail_mode": "off"})
call("/api/nodes/N3/retry", {})
s = wait_until(lambda s: s["run"]["status"] in ("WAITING_HUMAN", "SUCCESS"), timeout=60)
call("/api/nodes/N9/approval", {"decision": "approve"})
s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
check(s["run"]["status"] == "SUCCESS", "排除超时后流程恢复并跑完")

print("\n[4] 错误分类：不可重试错误（E-AGENT）直接转人工，不消耗重试")
call("/api/run/reset", {})
call("/api/run/start", {"speed": 2, "demo": False, "qa_mode": "pass"})
call("/api/config", {"agent": "llm"})   # 未配置 LLM 端点 → E-AGENT 不可重试
s = wait_until(lambda s: any(n["status"] == "WAITING_HUMAN" for n in s["nodes"]), timeout=30)
waits = [n for n in s["nodes"] if n["status"] == "WAITING_HUMAN"]
check(len(waits) == 1, "恰有一个节点因不可重试错误转人工（%s）" % (waits[0]["id"] if waits else "—"))
if waits:
    n = waits[0]
    check(len(n.get("attempts") or []) == 1, "不可重试错误只尝试 1 次（不自动重试）")
    check(n["attempts"][-1].get("error_code") == "E-AGENT", "错误码为 E-AGENT")
    check(n["retry_count"] == 0, "不可重试错误不消耗重试计数")
call("/api/config", {"agent": "mock"})
call("/api/nodes/%s/retry" % waits[0]["id"], {})
s = wait_until(lambda s: s["run"]["status"] in ("WAITING_HUMAN", "SUCCESS"), timeout=60)
call("/api/nodes/N9/approval", {"decision": "approve"})
s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"))
check(s["run"]["status"] == "SUCCESS", "切回 mock 并人工重试后跑完")

print("\n[5] 运行中取消")
call("/api/run/reset", {})
call("/api/run/start", {"speed": 1, "demo": False, "qa_mode": "pass"})
time.sleep(0.4)
code, r = call("/api/run/cancel", {})
check(code == 200 and r.get("ok"), "取消被接受")
s = state()
check(s["run"]["status"] == "CANCELLED", "运行状态为 CANCELLED")
check(not any(n["status"] == "RUNNING" for n in s["nodes"]), "无节点残留 RUNNING")
code, hl = call("/api/runs")
check(any(x["status"] == "CANCELLED" for x in hl.get("runs", [])), "取消的运行已归档到历史")

total = len(OK)
passed = sum(OK)
print("\n结果: %s (%d/%d)" % ("PASS" if passed == total else "FAIL", passed, total))
sys.exit(0 if passed == total else 1)
