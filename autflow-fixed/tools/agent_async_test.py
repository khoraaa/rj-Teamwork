#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""B 任务 · 慢执行器不阻塞接口与调度（异步执行回归测试）。

背景：Agent 调用曾经发生在持有引擎锁的调度循环里。MockAgent 是瞬时的，掩盖了问题；
一旦切换到真实模型（一次请求几秒），`/api/state`、SSE、保存草稿等所有请求都会被锁死。

本用例用一个**故意慢 2 秒**的假 LLM 端点验证：
  1. 慢调用进行中，`/api/state` 依然秒回（锁没有被执行器占住）；
  2. 慢调用进行中，`run.elapsed` 持续推进（调度线程没有被堵）；
  3. 超时后的错误分类仍然是可重试的 `E-TIMEOUT`，节点会重试；
  4. 运行可被取消并进入 CANCELLED。

自带服务与假模型端点，不需要外部依赖，也不会污染演示数据。

用法: python tools/agent_async_test.py
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server.py")
LLM_DELAY = 2.0
OK = []


def check(cond, msg):
    OK.append(bool(cond))
    print(("  ✓ " if cond else "  ✗ ") + msg)
    return cond


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------- 假模型端点
class SlowLLM(BaseHTTPRequestHandler):
    hits = 0

    # 产物形状要匹配节点声明的 schema，否则会被格式门拦下（那是另一个测试的事）
    SHAPES = {
        "prd-v1": {"summary": "登录功能", "requirements": ["账号登录"], "tasks": []},
        "design-v1": {"endpoints": ["POST /login"], "entities": ["User"]},
        "code-result-v1": {"loc": 120, "files": []},
        "scan-v1": {"score": 92},
        "test-v1": {"passed": 10, "total": 10},
        "verify-v1": {"merged": True},
    }

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        SlowLLM.hits += 1
        schema = "code-result-v1"
        text = raw.decode("utf-8", "replace")
        for name in self.SHAPES:
            if name in text:
                schema = name
                break
        time.sleep(LLM_DELAY)                    # 故意慢：模拟真实模型的响应时间
        body = json.dumps({"choices": [{"message": {"content": json.dumps(
            self.SHAPES[schema], ensure_ascii=False)}}]}).encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


class QuietServer(ThreadingHTTPServer):
    """执行器超时会主动断开连接，假模型端点的写回自然会失败；不让它污染退出码。"""
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, OSError)):
            return
        super().handle_error(request, client_address)


def start_llm():
    port = free_port()
    srv = QuietServer(("127.0.0.1", port), SlowLLM)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d" % port


# ---------------------------------------------------------------- 被测服务
def start_server(llm_url):
    port = free_port()
    data_dir = tempfile.mkdtemp(prefix="autflow-async-")
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    env["AUTFLOW_LLM_BASE_URL"] = llm_url
    env["AUTFLOW_LLM_API_KEY"] = "test-key"
    env["AUTFLOW_LLM_MODEL"] = "fake-model"
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen([sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
                            cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port, data_dir


BASE = ""


def call(path, body=None, method=None, timeout=15):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def timed_state():
    t0 = time.time()
    code, payload = call("/api/state")
    return code, payload, time.time() - t0


def by_id(s):
    return {n["id"]: n for n in s["nodes"]}


def wait_until(pred, timeout=40, tick=0.15):
    t0 = time.time()
    s = None
    while time.time() - t0 < timeout:
        s = call("/api/state")[1]
        if pred(s):
            return s
        time.sleep(tick)
    return s


def wait_health(base, timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(base + "/api/health", timeout=2):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def main():
    global BASE
    llm_srv, llm_url = start_llm()
    proc, BASE, data_dir = start_server(llm_url)
    try:
        print("[1] 环境就绪")
        check(wait_health(BASE), "服务已启动")
        code, payload = call("/api/agents")
        check("llm" in payload.get("available", []), "llm 执行器已注册")

        # 让首个 coder 节点带 1 秒时限，便于验证超时仍被归为可重试错误
        code, detail = call("/api/workflows/wf_login_001")
        draft = detail["workflow"]["draft"]["definition"]
        coder = next(n for n in draft["nodes"] if n.get("role") == "coder")
        coder["timeout"] = 1
        code, saved = call("/api/workflows/wf_login_001", {"definition": draft}, method="PUT")
        check(code == 200, "草稿已保存（%s 增加 timeout=1）" % coder["id"])
        call("/api/config", {"agent": "llm"})
        check(call("/api/agents")[1].get("current") == "llm", "已切换到 llm 执行器")

        print("\n[2] 慢执行期间接口不能被锁住")
        code, started = call("/api/run/start", {"speed": 5, "demo": False, "qa_mode": "pass"})
        check(code == 200, "运行已启动")

        worst, first_elapsed, last_elapsed, samples = 0.0, None, None, 0
        t0 = time.time()
        while time.time() - t0 < 4.0:
            code, snap, cost = timed_state()
            worst = max(worst, cost)
            if snap["run"]["status"] == "RUNNING":
                samples += 1
                if first_elapsed is None:
                    first_elapsed = snap["run"]["elapsed"]
                last_elapsed = snap["run"]["elapsed"]
            time.sleep(0.15)

        check(SlowLLM.hits > 0, "假模型端点已收到调用（%d 次）" % SlowLLM.hits)
        check(worst < 0.6, "慢调用期间 /api/state 最大延迟 %.3fs < 0.6s" % worst)
        check(samples >= 5, "采样到 %d 次 RUNNING 状态" % samples)
        check(first_elapsed is not None and last_elapsed - first_elapsed > 0.5,
              "忙碌期间 elapsed 持续推进（%.2fs → %.2fs）"
              % (first_elapsed or 0, last_elapsed or 0))

        print("\n[3] 超时仍按可重试错误处理")
        snap = wait_until(
            lambda s: by_id(s)[coder["id"]]["attempts"]
            and by_id(s)[coder["id"]]["attempts"][-1].get("error_code") == "E-TIMEOUT",
            timeout=40)
        node = by_id(snap)[coder["id"]]
        check(node["attempts"][-1].get("error_code") == "E-TIMEOUT",
              "%s 的失败被归类为 E-TIMEOUT" % coder["id"])
        check(len(node["attempts"]) >= 1, "记录到 %d 次尝试" % len(node["attempts"]))
        check(node["status"] in ("READY", "RUNNING", "WAITING_HUMAN", "FAILED"),
              "超时后节点状态可继续推进（%s）" % node["status"])

        print("\n[4] 运行可取消")
        code, r = call("/api/run/cancel", {})
        check(code == 200, "取消请求被接受")
        snap = wait_until(lambda s: s["run"]["status"] == "CANCELLED", timeout=10)
        check(snap["run"]["status"] == "CANCELLED", "运行进入 CANCELLED")
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        llm_srv.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)

    passed = sum(1 for x in OK if x)
    print("\n结果: %s (%d/%d)" % ("PASS" if passed == len(OK) else "FAIL", passed, len(OK)))
    return 0 if passed == len(OK) else 1


if __name__ == "__main__":
    sys.exit(main())