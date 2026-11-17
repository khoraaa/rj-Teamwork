#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""对运行中的原型做一次端到端冒烟测试（不依赖第三方库）。

用法: python tools/smoke_test.py [base_url] [--speed 4] [--demo]
"""
import json
import sys
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"
SPEED = 4
DEMO = True


def call(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def state():
    return call("/api/state")


def main():
    print("→ 重置并启动，speed=%s demo=%s" % (SPEED, DEMO))
    call("/api/run/reset", {})
    r = call("/api/run/start", {"speed": SPEED, "demo": DEMO, "qa_mode": "auto" if DEMO else "pass"})
    assert r["ok"], r

    seen, t0, approved = {}, time.time(), False
    while time.time() - t0 < 120:
        s = state()
        for n in s["nodes"]:
            seen.setdefault(n["id"], set()).add(n["status"])
        run = s["run"]["status"]
        if run == "WAITING_HUMAN" and not approved:
            waiting = [n for n in s["nodes"] if n["status"] == "WAITING_HUMAN"]
            print("   ⏸ 等待人工：", [(w["id"], w["name"]) for w in waiting])
            node = waiting[0]
            if node["type"] == "approval":
                a = call("/api/nodes/%s/approval" % node["id"], {"decision": "approve"})
            else:
                a = call("/api/nodes/%s/retry" % node["id"], {})
            print("   → 人工操作:", a)
            approved = True
            continue
        if run in ("SUCCESS", "FAILED", "CANCELLED"):
            break
        time.sleep(0.25)

    s = state()
    print("\n=== 最终运行状态:", s["run"]["status"], " 用时 %.1fs" % s["run"]["elapsed"])
    print("=== 节点结果 ===")
    for n in s["nodes"]:
        print("  %-4s %-10s %-14s attempts=%d retry=%d branch=%s" % (
            n["id"], n["status"], n["name"], len(n.get("attempts") or []),
            n.get("retry_count", 0), n.get("branch_taken")))
    print("\n=== 关键事件（后 18 条）===")
    for e in s["events"][-18:]:
        print("  [%6.2fs] %-22s %-4s %s" % (e["ts"], e["type"], e["node_id"] or "", e["message"]))

    by = {n["id"]: n for n in s["nodes"]}
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("  ✓ " if cond else "  ✗ ") + msg)
        ok = ok and cond

    print("\n=== 断言 ===")
    check(s["run"]["status"] == "SUCCESS", "运行最终成功")
    check(len(by["N3"]["attempts"]) >= 2, "N3 发生重试后成功（格式门拦截）")
    check(by["N5"].get("branch_taken") in ("true", "false"), "N5 条件判定产生分支")
    if DEMO:
        check(by["N5"]["branch_taken"] == "false", "演示模式命中 FALSE 分支")
        check(by["N6"]["status"] == "SKIPPED", "未命中的 N6 被标记 SKIPPED")
        check(by["N7"]["status"] == "SUCCESS", "命中分支 N7 正常执行")
    else:
        check(by["N5"]["branch_taken"] == "true", "顺利模式命中 TRUE 分支")
        check(by["N7"]["status"] == "SKIPPED", "未命中的 N7 被标记 SKIPPED")
    check(by["N8"]["status"] == "SUCCESS", "汇聚节点 N8 在 any 策略下就绪")
    check(by["N9"]["status"] == "SUCCESS" and by["N9"]["output"], "N9 审批留痕")
    check(by["N10"]["status"] == "SUCCESS", "N10 交付完成")
    check(len(s["events"]) > 20, "事件流已记录（%d 条）" % len(s["events"]))
    print("\n结果:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())