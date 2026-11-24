#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""群体讨论 / 人工引导 API 回归测试。用法: python tools/discussion_test.py"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server.py")
OK = []
LLM_PROMPTS = []


def check(condition, message):
    OK.append(bool(condition))
    print(("[PASS] " if condition else "[FAIL] ") + message)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class FakeLLM(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        request = json.loads(self.rfile.read(length).decode("utf-8"))
        LLM_PROMPTS.append(request["messages"][-1]["content"])
        content = json.dumps({"message": "模拟模型结合当前讨论给出建议"}, ensure_ascii=False)
        body = json.dumps({"choices": [{"message": {"content": content}}]}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def call(base, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method="POST" if data is not None else "GET",
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8") or "{}")


def wait_discussion(base, discussion_id, expected, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, state = call(base, "/api/state")
        discussion = next((d for d in state.get("discussions", [])
                           if d.get("id") == discussion_id), None)
        if discussion and discussion.get("status") == expected:
            return discussion
        if discussion and discussion.get("status") == "FAILED":
            return discussion
        time.sleep(0.1)
    return discussion


def main():
    port = free_port()
    llm_port = free_port()
    while llm_port == port:
        llm_port = free_port()
    llm_server = ThreadingHTTPServer(("127.0.0.1", llm_port), FakeLLM)
    threading.Thread(target=llm_server.serve_forever, daemon=True).start()
    data_dir = tempfile.mkdtemp(prefix="autflow-discussion-test-")
    env = dict(os.environ, AUTFLOW_DATA_DIR=data_dir, PYTHONIOENCODING="utf-8",
               AUTFLOW_LLM_BASE_URL="http://127.0.0.1:%d" % llm_port,
               AUTFLOW_LLM_API_KEY="test-key", AUTFLOW_LLM_MODEL="fake-model")
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = "http://127.0.0.1:%d" % port
    try:
        for _ in range(100):
            try:
                call(base, "/api/health")
                break
            except Exception:
                time.sleep(0.1)
        code, result = call(base, "/api/discussions", {"node_id": "N2", "mode": "mock"})
        discussion = result.get("discussion") or {}
        check(code == 200 and result.get("ok"), "Agent 节点可发起讨论")
        check(discussion.get("status") == "WAITING_GUIDANCE", "首轮结束后等待人工引导")
        check(len(discussion.get("messages", [])) == 3, "首轮包含三个不同角色的发言")

        code, result = call(base, "/api/discussions/%s/guide" % discussion.get("id"),
                            {"message": "优先复用现有模块，不新增依赖"})
        guided = result.get("discussion") or {}
        check(code == 200 and result.get("ok"), "人工意见提交成功")
        check(guided.get("status") == "COMPLETE", "引导后讨论形成结论")
        check(len(guided.get("messages", [])) == 7, "消息包含人工意见和三条后续回应")
        check("不新增依赖" in guided.get("conclusion", ""), "最终结论包含人工约束")

        code, state = call(base, "/api/state")
        stored = next((d for d in state.get("discussions", [])
                       if d.get("id") == discussion.get("id")), None)
        check(code == 200 and stored and stored.get("status") == "COMPLETE",
              "讨论记录进入运行快照")

        code, result = call(base, "/api/discussions", {"node_id": "N3", "mode": "llm"})
        llm_id = (result.get("discussion") or {}).get("id")
        llm_discussion = wait_discussion(base, llm_id, "WAITING_GUIDANCE")
        check(code == 200 and llm_discussion and llm_discussion.get("status") == "WAITING_GUIDANCE",
              "LLM 模式异步生成首轮发言")
        check(len((llm_discussion or {}).get("messages", [])) == 3, "LLM 首轮包含三个模型角色")
        code, result = call(base, "/api/discussions/%s/guide" % llm_id,
                            {"message": "必须兼容旧版接口"})
        llm_discussion = wait_discussion(base, llm_id, "COMPLETE")
        check(code == 200 and (llm_discussion or {}).get("status") == "COMPLETE",
              "LLM 模式接受人工意见并形成总结")
        check(len((llm_discussion or {}).get("messages", [])) == 8,
              "LLM 总结包含二轮发言和主持人结论")
        check(any("必须兼容旧版接口" in prompt for prompt in LLM_PROMPTS),
              "人工意见进入后续模型提示词")

        code, started = call(base, "/api/run/start", {"demo": False})
        check(code == 200 and started.get("ok"), "完成讨论后可以启动工作流")
        code, state = call(base, "/api/state")
        carried = next((d for d in state.get("discussions", [])
                        if d.get("id") == discussion.get("id")), None)
        check(code == 200 and carried and carried.get("status") == "COMPLETE",
              "启动工作流后讨论结论仍绑定到对应节点")
        call(base, "/api/run/cancel", {})

        code, invalid = call(base, "/api/discussions", {"node_id": "N9", "mode": "mock"})
        check(code == 400 and not invalid.get("ok"), "非 Agent 节点会被拒绝")
        code, invalid = call(base, "/api/discussions/%s/guide" % discussion.get("id"),
                            {"message": " "})
        check(code == 400 and not invalid.get("ok"), "空引导意见会被拒绝")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)
        llm_server.shutdown()
        llm_server.server_close()

    passed = sum(1 for result in OK if result)
    print("\n结果: %s (%d/%d)" % ("PASS" if passed == len(OK) else "FAIL", passed, len(OK)))
    return 0 if passed == len(OK) else 1


if __name__ == "__main__":
    sys.exit(main())
