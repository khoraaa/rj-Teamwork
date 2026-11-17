#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""B 任务 · 运行中重启恢复 端到端测试。

流程：在隔离数据目录里启动服务 → 慢速启动运行，捕获到“已有完成节点 + 有在途节点”
时杀掉进程 → 用同一数据目录重启 → 断言：
  1. 运行进入 RECONCILING（待核对），历史与已完成节点仍可见；
  2. 崩溃时在途的节点进入 UNCERTAIN，不盲目重复执行；
  3. 核对（确认成功/重试/跳过）后可恢复并跑完，且归档进历史。

用法: python tools/recovery_test.py
"""
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


PORT = int(sys.argv[1]) if len(sys.argv) > 1 else free_port()
BASE = "http://127.0.0.1:%d" % PORT
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


def state():
    try:
        return call("/api/state")[1]
    except Exception:
        return None


def by_id(s):
    return {n["id"]: n for n in s["nodes"]}


def disk_state(data_dir):
    """直接读取落盘快照（run_state.json）里的运行，用于确认持久化已追平。"""
    try:
        with open(os.path.join(data_dir, "run_state.json"), encoding="utf-8") as f:
            return json.load(f).get("run")
    except Exception:
        return None


def disk_nodes(data_dir):
    r = disk_state(data_dir)
    return list((r or {}).get("nodes", {}).values())


def start_server(data_dir):
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    for attempt in range(10):
        proc = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(PORT)],
            cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(40):
            try:
                if call("/api/health")[0] == 200:
                    return proc
            except Exception:
                pass
            time.sleep(0.25)
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
        time.sleep(0.5)
    raise RuntimeError("无法在端口 %d 启动服务" % PORT)


def wait_until(pred, timeout=60, tick=0.1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = state()
        if s and pred(s):
            return s
        time.sleep(tick)
    return state()


def main():
    data_dir = tempfile.mkdtemp(prefix="autflow_recover_")
    proc = None
    print("数据目录:", data_dir)
    try:
        print("\n[1] 首次启动并慢速运行，制造“有完成节点 + 有在途节点”的中断点")
        proc = start_server(data_dir)
        call("/api/run/reset", {})
        call("/api/run/start", {"speed": 0.5, "demo": False, "qa_mode": "pass"})
        s = wait_until(lambda s: any(n["status"] == "SUCCESS" for n in s["nodes"])
                       and any(n["status"] == "RUNNING" for n in s["nodes"]), timeout=40)
        check(s is not None and any(n["status"] == "RUNNING" for n in s["nodes"]),
              "捕获到在途节点（RUNNING）")
        check(any(n["status"] == "SUCCESS" for n in s["nodes"]), "已有完成节点（SUCCESS）")
        running_before = [n["id"] for n in s["nodes"] if n["status"] == "RUNNING"]

        # 持久化按 0.5s 限频，等待落盘快照也追平“有完成节点 + 有在途节点”，
        # 这样崩溃恢复后才能看到已完成节点（而非仅看到上一次 RUNNING 的旧快照）。
        t0 = time.time()
        while time.time() - t0 < 10:
            dn = disk_nodes(data_dir)
            if dn and any(n["status"] == "SUCCESS" for n in dn) \
                    and any(n["status"] == "RUNNING" for n in dn):
                break
            time.sleep(0.1)
        dn = disk_nodes(data_dir)
        check(dn and any(n["status"] == "SUCCESS" for n in dn),
              "完成节点已落盘（崩溃后仍可见）")
        check(dn and any(n["status"] == "RUNNING" for n in dn),
              "在途节点已落盘（崩溃后进入待核对）")
        running_before = [n["id"] for n in dn if n["status"] == "RUNNING"]

        print("    模拟崩溃：终止服务进程")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
            proc.wait()

        print("\n[2] 用同一数据目录重启，检查恢复状态")
        proc = start_server(data_dir)
        s = wait_until(lambda s: s["run"]["status"] == "RECONCILING", timeout=20)
        b = by_id(s)
        check(s["run"]["status"] == "RECONCILING", "重启后运行进入 RECONCILING（待核对）")
        uncertain = [nid for nid, n in b.items() if n["status"] == "UNCERTAIN"]
        check(bool(uncertain), "在途节点进入 UNCERTAIN（待核对）：%s" % "、".join(uncertain))
        check(any(nid in uncertain for nid in running_before),
              "崩溃时 RUNNING 的节点现在 UNCERTAIN")
        check(any(n["status"] == "SUCCESS" for n in s["nodes"]), "已完成节点在重启后仍可见")
        check(s["run"].get("recovered") is True, "快照标记为已恢复")

        print("\n[3] 核对不确定节点（确认成功，不重复执行），恢复并跑完")
        for nid in uncertain:
            code, r = call("/api/nodes/%s/confirm" % nid, {})
            check(code == 200 and r.get("ok"), "确认 %s 成功（未重复执行）" % nid)
        s = state()
        check(not any(n["status"] == "UNCERTAIN" for n in s["nodes"]), "核对后无残留待核对节点")

        code, r = call("/api/run/resume", {})
        check(code == 200 and r.get("ok"), "恢复运行被接受")
        s = wait_until(lambda s: s["run"]["status"] in ("WAITING_HUMAN", "SUCCESS", "FAILED"), timeout=60)
        if s["run"]["status"] == "WAITING_HUMAN":
            waiting = [n for n in s["nodes"] if n["status"] == "WAITING_HUMAN"]
            if waiting and waiting[0].get("type") == "approval":
                call("/api/nodes/%s/approval" % waiting[0]["id"], {"decision": "approve"})
            s = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED"), timeout=60)
        check(s["run"]["status"] == "SUCCESS",
              "核对恢复后运行成功结束（实际 %s）" % s["run"]["status"])

        print("\n[4] 历史归档")
        code, hl = call("/api/runs")
        runs = hl.get("runs", [])
        check(any(x["status"] == "SUCCESS" for x in runs), "恢复的运行已归档进历史")
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
                proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)

    total = len(OK)
    passed = sum(OK)
    print("\n结果: %s (%d/%d)" % ("PASS" if passed == total else "FAIL", passed, total))
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
