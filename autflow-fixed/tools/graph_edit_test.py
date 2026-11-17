#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""结构改图端到端：删节点、加节点、改连线 → 保存 → 发布 → 按版本跑完。

验证 FR-01 的编辑结果不只是能存 JSON，而是真的可被编排器执行；
同时验证破坏性编辑（删掉条件节点的唯一分支）会被校验器拦下。
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


call("/api/run/reset", {})
ed = call("/api/editor")[1]
wid = ed["workflow_id"]
base = json.loads(json.dumps(ed["draft"]["definition"]))

print("[1] 破坏性编辑应被拦下：删掉 N7 会让条件节点 N5 只剩单分支")
bad = json.loads(json.dumps(base))
bad["nodes"] = [n for n in bad["nodes"] if n["id"] != "N7"]
bad["edges"] = [e for e in bad["edges"] if e["source"] != "N7" and e["target"] != "N7"]
code, r = call("/api/workflows/" + wid, {"definition": bad}, method="PUT")
check(code == 400 and not r.get("ok"), "单分支定义被拒绝（HTTP %d）" % code)
check(any("true / false" in (e.get("msg") or "") or "分支" in (e.get("msg") or "")
          for e in r.get("errors", [])), "错误信息指出分支缺失")

print("\n[2] 合法改图：删 N4（安全扫描），在 N6 之后插入 N11 复核节点")
d = json.loads(json.dumps(base))
d["nodes"] = [n for n in d["nodes"] if n["id"] != "N4"]
d["edges"] = [e for e in d["edges"] if e["source"] != "N4" and e["target"] != "N4"]
n5 = next(n for n in d["nodes"] if n["id"] == "N5")
n5["deps"] = [x for x in n5["deps"] if x != "N4"]
d["edges"] = [e for e in d["edges"] if not (e["source"] == "N6" and e["target"] == "N8")]
d["nodes"].append({"id": "N11", "type": "agent", "role": "qa", "name": "交付前复核",
                   "deps": ["N6"], "max_retries": 1, "pos": {"x": 520, "y": 640}})
d["edges"].append({"source": "N6", "target": "N11"})
d["edges"].append({"source": "N11", "target": "N8"})
n8 = next(n for n in d["nodes"] if n["id"] == "N8")
n8["deps"] = [x for x in n8["deps"] if x != "N6"] + ["N11"]
check(len(d["nodes"]) == 10, "节点数 10（原 10，删 1 加 1）")

code, r = call("/api/validate", {"workflow": d})
check(r["valid"], "改图后校验通过（%d 节点 / %d 边）" % (r["stats"]["nodes"], r["stats"]["edges"]))
code, r = call("/api/workflows/" + wid, {"definition": d}, method="PUT")
check(code == 200 and r["ok"], "保存为草稿")
code, r = call("/api/workflows/%s/publish" % wid, {"note": "删 N4、插入 N11"})
check(code == 200 and r["ok"], "发布新版本 %s" % r.get("version"))
label = r.get("version")

print("\n[3] 按新版本运行，遇审批自动批准，直到结束")
code, r = call("/api/workflows/%s/runs" % wid,
               {"version": label, "speed": 6, "demo": False, "qa_mode": "pass"})
check(code == 200 and r["ok"], "启动成功，绑定版本 %s" % r.get("version"))

deadline = time.time() + 90
final = None
approved = False
while time.time() < deadline:
    st = call("/api/state")[1]
    nodes = {n["id"]: n for n in st["nodes"]}
    if st["run"]["status"] == "WAITING_HUMAN" and not approved:
        waiting = [n for n in st["nodes"] if n["status"] == "WAITING_HUMAN"]
        if waiting and waiting[0].get("type") == "approval":
            call("/api/nodes/%s/approval" % waiting[0]["id"], {"decision": "approve"})
            approved = True
    if st["run"]["status"] in ("SUCCESS", "FAILED", "CANCELLED"):
        final = st
        break
    time.sleep(0.5)

check(final is not None, "运行在 90s 内结束")
if final:
    ids = {n["id"]: n for n in final["nodes"]}
    check(final["run"]["status"] == "SUCCESS", "运行结果 SUCCESS（实际 %s）" % final["run"]["status"])
    check("N4" not in ids, "已删除的 N4 未出现在运行里")
    check("N11" in ids and ids["N11"]["status"] == "SUCCESS", "新插入的 N11 执行成功")
    check(ids.get("N10", {}).get("status") == "SUCCESS", "N10 交付完成")
    order = [e.get("node_id") for e in final["events"]
             if e.get("type") == "node.status" and "→ SUCCESS" in (e.get("message") or "")]
    if all(x in order for x in ("N6", "N11", "N8")):
        check(order.index("N6") < order.index("N11") < order.index("N8"),
              "执行顺序 N6 → N11 → N8（新连线生效）")
    else:
        check(False, "事件里缺少 N6/N11/N8 的成功记录")
    check(final["run"]["workflow_version"] == label,
          "运行记录绑定版本 %s（实际 %s）" % (label, final["run"]["workflow_version"]))

print("\n结果:", "PASS" if all(OK) else "FAIL", "(%d/%d)" % (sum(OK), len(OK)))
sys.exit(0 if all(OK) else 1)