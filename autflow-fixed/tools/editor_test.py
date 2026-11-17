#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""FR-01 / 版本管理接口测试：草稿保存、校验拒绝、发布、按版本运行、运行期隔离。"""
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
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def check(cond, msg):
    OK.append(bool(cond))
    print(("  ✓ " if cond else "  ✗ ") + msg)
    return cond


print("[1] 读取编辑器状态")
code, ed = call("/api/editor")
wid = ed.get("workflow_id")
check(code == 200 and ed.get("ok"), "GET /api/editor 正常")
check(any(v["version"] == "v1" for v in ed["versions"]), "存在初始发布版本 v1")
draft = ed["draft"]["definition"]
check(len(draft["nodes"]) == 10, "草稿含 10 个节点")

print("\n[2] 修改并保存草稿（改名 + 记录坐标）")
d2 = json.loads(json.dumps(draft))
node = next(n for n in d2["nodes"] if n["id"] == "N3")
node["name"] = "登录接口实现（v2 草稿）"
node["pos"] = {"x": 320, "y": 480}
code, r = call("/api/workflows/" + wid, {"definition": d2}, method="PUT")
check(code == 200 and r.get("ok"), "PUT /api/workflows/{id} 保存成功")
check(r["draft"]["dirty"] is True, "草稿标记为 dirty")
check(r["draft"]["base_version"] == "v1", "草稿基于 v1")

print("\n[3] 非法定义应被拒绝且不落库")
bad = json.loads(json.dumps(d2))
bad["nodes"][1]["deps"].append("N10")
bad["edges"].append({"source": "N10", "target": "N2"})
code, r = call("/api/workflows/" + wid, {"definition": bad}, method="PUT")
check(code == 400 and not r.get("ok"), "环依赖定义被拒绝（HTTP %d）" % code)
check(any(e["code"] == "E-CYCLE" for e in r.get("errors", [])), "错误定位到 E-CYCLE")
code, ed2 = call("/api/editor")
check(next(n for n in ed2["draft"]["definition"]["nodes"] if n["id"] == "N3")["name"]
      == "登录接口实现（v2 草稿）", "拒绝后草稿保持不变")

print("\n[4] 发布版本")
code, r = call("/api/workflows/%s/publish" % wid, {"note": "改节点名与坐标"})
check(code == 200 and r.get("version") == "v2", "发布得到 v2")
check(r["ok"], "发布返回 ok")
code, vs = call("/api/workflows/%s/versions" % wid)
check([v["version"] for v in vs["versions"]] == ["v1", "v2"], "版本列表 = v1, v2")
v1 = call("/api/workflows/" + wid)[1]["workflow"]["versions"][0]["definition"]
v2 = call("/api/workflows/" + wid)[1]["workflow"]["versions"][1]["definition"]
check(next(n for n in v1["nodes"] if n["id"] == "N3")["name"] == "登录接口实现",
      "v1 仍是旧名字（版本不可变）")
check(next(n for n in v2["nodes"] if n["id"] == "N3")["name"] == "登录接口实现（v2 草稿）",
      "v2 含新名字")

print("\n[5] 按指定版本启动运行")
call("/api/run/reset", {})
code, r = call("/api/workflows/%s/runs" % wid, {"version": "v1", "speed": 8, "demo": False,
                                                "qa_mode": "pass"})
check(code == 200 and r.get("ok"), "POST /api/workflows/{id}/runs 启动成功")
check(r.get("version") == "v1", "运行绑定 v1")
time.sleep(0.6)
code, st = call("/api/state")
check(st["run"]["workflow_version"] == "v1", "运行中快照版本 = v1")
check(next(n for n in st["nodes"] if n["id"] == "N3")["name"] == "登录接口实现",
      "运行中的节点名来自 v1（而不是草稿）")

print("\n[6] 运行期间改草稿，不影响在跑的版本")
d3 = json.loads(json.dumps(v2))
next(n for n in d3["nodes"] if n["id"] == "N3")["name"] = "运行期改的名字"
code, r = call("/api/workflows/" + wid, {"definition": d3}, method="PUT")
check(code == 200, "运行中仍可保存草稿")
time.sleep(0.4)
code, st2 = call("/api/state")
check(next(n for n in st2["nodes"] if n["id"] == "N3")["name"] == "登录接口实现",
      "在跑的运行未受草稿修改影响（版本隔离）")

print("\n[7] 校验接口对草稿生效")
code, r = call("/api/validate", {})
check(r["valid"], "当前草稿校验通过")
call("/api/run/cancel", {})

print("\n结果:", "PASS" if all(OK) else "FAIL", "(%d/%d)" % (sum(OK), len(OK)))
sys.exit(0 if all(OK) else 1)