#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""FR-09 / 需求→DAG 生成 + 运行历史 接口测试。

需要干净仓库（先删 data/workflows.json 与 data/runs.json 再启动服务）。
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
    print(("  \u2713 " if cond else "  \u2717 ") + msg)
    return cond


def wait_status(want, limit=40.0):
    end = time.time() + limit
    st = None
    while time.time() < end:
        _, s = call("/api/state")
        st = s["run"]["status"]
        if st in want:
            return st
        time.sleep(0.25)
    return st


print("[1] 需求输入")
code, ed = call("/api/editor")
check(code == 200 and ed.get("ok"), "GET /api/editor 正常")
check(bool(ed.get("requirement")), "编辑器返回当前需求文本")
wid = ed["workflow_id"]

code, r = call("/api/workflows/generate", {"requirement": ""})
check(code == 400 and not r.get("ok"), "空需求被拒绝（400）")
code, r = call("/api/workflows/generate", {"requirement": "做登录"})
check(code == 400, "过短需求被拒绝（400）")

print("\n[2] 生成 DAG 草稿")
req = "做一个内部管理后台：支持用户名密码登录与短信验证码，订单支付结算，报表统计导出"
code, r = call("/api/workflows/generate", {"requirement": req})
check(code == 200 and r.get("ok"), "生成成功")
check(len(r.get("trace", [])) >= 4, "返回生成依据（trace %d 条）" % len(r.get("trace", [])))
check("认证与账号" in (r.get("matched") or []), "识别出能力域：认证与账号")
check("交易与支付" in (r.get("matched") or []), "识别出能力域：交易与支付")
check("数据与报表" in (r.get("matched") or []), "识别出能力域：数据与报表")
check(bool(r.get("risk")), "识别出高风险词（%s）" % "、".join(r.get("risk") or []))

draft = r["draft"]["definition"]
ids = [n["id"] for n in draft["nodes"]]
names = [n["name"] for n in draft["nodes"]]
check(len(draft["nodes"]) >= 10, "生成 %d 个节点" % len(draft["nodes"]))
check(draft.get("requirement") == req, "需求文本写入草稿")
check(any(n["type"] == "condition" for n in draft["nodes"]), "含质量门条件节点")
check(any(n["type"] == "approval" for n in draft["nodes"]), "含人工审批节点")
check(any("安全" in n["name"] for n in draft["nodes"]), "高风险需求追加安全评审")
check(draft["edges"] and all(e["source"] in ids and e["target"] in ids
                             for e in draft["edges"]), "所有边端点都存在于节点集合")
check(r["draft"]["dirty"] is False, "生成后草稿处于已保存状态")

code, v = call("/api/validate", {"workflow": draft})
check(code == 200 and v.get("valid"), "生成的图通过校验（0 错误）")
check(not v.get("warnings"), "无孤立节点告警")

print("\n[3] 无关键词需求 → 通用骨架")
code, r2 = call("/api/workflows/generate", {"requirement": "帮我做一个东西，尽快上线"})
check(code == 200 and r2.get("ok"), "无关键词也能生成")
names2 = [n["name"] for n in r2["draft"]["definition"]["nodes"]]
check(any("核心功能实现" in n for n in names2), "回退到通用实现节点")
check(any("安全" in n for n in names2), "命中“上线”时仍追加安全评审")

print("\n[4] 编辑 → 校验 → 保存 → 发布")
wid = ed["workflow_id"]
d3 = json.loads(json.dumps(draft))
target = next(n for n in d3["nodes"] if n["type"] == "agent" and n["role"] == "coder")
target["name"] = target["name"] + "（已人工修改）"
d3["nodes"].append({"id": "N90", "type": "agent", "name": "人工补充节点", "role": "qa",
                    "deps": [target["id"]], "out_schema": "verify-v1", "max_retries": 1})
d3["edges"].append({"source": target["id"], "target": "N90"})
code, r3 = call("/api/workflows/" + wid, {"definition": d3}, method="PUT")
check(code == 200 and r3.get("ok"), "保存人工编辑后的草稿")
check(any(n["name"].endswith("（已人工修改）") for n in r3["draft"]["definition"]["nodes"]),
      "编辑内容已落库")
check(any(n["id"] == "N90" for n in r3["draft"]["definition"]["nodes"]), "新增节点已落库")

code, r4 = call("/api/workflows/%s/publish" % wid, {"note": "需求生成 v2"})
check(code == 200 and r4.get("ok"), "发布新版本（%s）" % r4.get("version"))

print("\n[5] 启动 → 两个关键审核 → 完成 → 归档")
code, r5 = call("/api/workflows/%s/runs" % wid, {"speed": 12, "demo": False, "qa_mode": "pass"})
check(code == 200 and r5.get("ok"), "按草稿启动运行")
run_id = r5.get("run_id")
seen_approvals = []
deadline = time.time() + 90
st = None
while time.time() < deadline:
    _, s = call("/api/state")
    st = s["run"]["status"]
    waits = [n for n in s["nodes"]
             if n["status"] == "WAITING_HUMAN" and n["type"] == "approval"]
    if waits:
        for node in waits:
            if node["id"] not in seen_approvals:
                seen_approvals.append(node["id"])
            code, ra = call("/api/nodes/%s/approval" % node["id"],
                            {"decision": "approve",
                             "comment": "自动验收：" + (node.get("name") or node["id"])})
            check(code == 200 and ra.get("ok"),
                  "审核通过：%s" % (node.get("name") or node["id"]))
    if st in ("SUCCESS", "FAILED", "CANCELLED"):
        break
    time.sleep(0.25)
check(len(seen_approvals) == 2,
      "两次关键审核都到点暂停（实际 %d）" % len(seen_approvals))
check(st == "SUCCESS", "两次审核后运行成功结束（当前 %s）" % st)

print("\n[6] 历史查看")
code, hl = call("/api/runs")
check(code == 200, "GET /api/runs 正常")
runs = hl.get("runs") or []
check(len(runs) >= 1, "历史里有 %d 条记录" % len(runs))
top = runs[0]
check(top["run_id"] == run_id, "最新记录就是刚才那次运行")
check(top["status"] == "SUCCESS", "记录状态为 SUCCESS")
check(top["stats"]["dispatched"] + top["stats"]["skipped"] == len(d3["nodes"]),
      "派发 + 跳过 = 全部节点（%d + %d = %d）"
      % (top["stats"]["dispatched"], top["stats"]["skipped"], len(d3["nodes"])))
code, one = call("/api/runs/" + run_id)
check(code == 200 and one["run"]["run_id"] == run_id, "GET /api/runs/{id} 可取单次详情")
check(len(one["run"]["nodes"]) == len(d3["nodes"]), "详情含全部节点（含人工补充节点）")
check(len(one["run"]["events"]) > 10, "详情含事件流（%d 条）" % len(one["run"]["events"]))
check(bool(one["run"]["definition"].get("nodes")), "详情含当时的定义快照")
code, miss = call("/api/runs/RUN-NOT-EXIST")
check(code == 404, "不存在的运行返回 404")

print("\n[7] 运行中禁止重建草稿")
call("/api/run/reset")
_, ed2 = call("/api/editor")
code, r6 = call("/api/workflows/generate", {"requirement": "生成一个报表系统，含导出"})
check(code == 200 and r6.get("ok"), "空闲态可再次生成")
call("/api/workflows/%s/runs" % wid, {"speed": 12})
time.sleep(0.3)
code, r7 = call("/api/workflows/generate", {"requirement": "再做一次支付流程"})
check(code == 409, "运行中重建草稿被拒绝（409）")
call("/api/run/cancel")

total = len(OK)
passed = sum(OK)
print("\n结果: %s (%d/%d)" % ("PASS" if passed == total else "FAIL", passed, total))
sys.exit(0 if passed == total else 1)
