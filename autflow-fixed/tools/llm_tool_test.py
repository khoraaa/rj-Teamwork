"""D · 真实模型链路验收：模型配置、规格文档导入、带工具的执行器（真写文件 / 真跑测试）。

这里**不连外网**：起一个假的 OpenAI 兼容端点，让它在会话里真的下发
`write_file` / `run_command` / `finish` 工具调用，于是：

  * 工作区里出现真的源码与真的测试文件（不是模型自报的 JSON）；
  * 测试节点跑的是真的 `python -m pytest -q`，通过数来自 pytest 的输出；
  * `/api/runs/{id}/files` 与 `?path=` 能把落盘内容读回来。

同时覆盖：密钥只回掩码、设计规格文档（Markdown / JSON）解析、路径越界与命令白名单。
"""
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SERVER = os.path.join(ROOT, "server.py")
sys.path.insert(0, ROOT)

PASS, FAIL = [], []
BASE = ""

SERVICE_PY = '''"""加法服务：最小可运行实现。"""


def add(a, b):
    """返回两数之和。"""
    return a + b
'''

TEST_PY = '''import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "app"))

from service import add


class TestAdd(unittest.TestCase):
    def test_add(self):
        self.assertEqual(add(1, 2), 3)

    def test_add_negative(self):
        self.assertEqual(add(-1, -2), -3)


if __name__ == "__main__":
    unittest.main()
'''


def check(cond, label):
    (PASS if cond else FAIL).append(label)
    print(("  ✓ " if cond else "  ✗ ") + label)


# ------------------------------------------------------------------ 假模型端点
class FakeLLM(BaseHTTPRequestHandler):
    calls = 0
    lock = threading.Lock()

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {}
        messages = payload.get("messages") or []
        system = next((m.get("content") or "" for m in messages
                       if m.get("role") == "system"), "")
        kind = "implementation"
        m = re.search(r"任务类型：(\w+)", system)
        if m:
            kind = m.group(1)
        tool_rounds = len([x for x in messages if x.get("role") == "tool"])
        with FakeLLM.lock:
            FakeLLM.calls += 1

        if kind in ("verification", "review", "integration"):
            body = {"content": "测试与评审由执行器实跑，见 pytest 输出。"}
            return self._send(body)

        if tool_rounds == 0:
            return self._tool("write_file", {"path": "src/app/service.py", "content": SERVICE_PY})
        if tool_rounds == 1:
            return self._tool("write_file", {"path": "tests/test_service.py", "content": TEST_PY})
        if tool_rounds == 2:
            return self._tool("run_command", {"command": "python -m pytest -q"})
        return self._tool("finish", {"summary": "实现 add() 并通过 2 条用例",
                                     "files": ["src/app/service.py", "tests/test_service.py"]})

    def _tool(self, name, args):
        self._send({"content": "", "tool_calls": [{
            "id": "call_" + name, "type": "function",
            "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)}}]})

    def _send(self, message):
        message.setdefault("role", "assistant")
        body = json.dumps({"choices": [{"message": message, "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 12, "completion_tokens": 6}}).encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, OSError)):
            return
        super().handle_error(request, client_address)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_llm():
    port = free_port()
    srv = QuietServer(("127.0.0.1", port), FakeLLM)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/v1" % port


def start_server(llm_url):
    port = free_port()
    data_dir = tempfile.mkdtemp(prefix="autflow-llm-")
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    env["AUTFLOW_LLM_BASE_URL"] = llm_url
    env["AUTFLOW_LLM_API_KEY"] = "sk-test-1234567890abcdef"
    env["AUTFLOW_LLM_MODEL"] = "fake-coder"
    env["AUTFLOW_LLM_MODE"] = "llm"
    env["AUTFLOW_ENABLE_LOCAL_CODE_EXECUTION"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen([sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
                            cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port, data_dir


def call(path, body=None, method=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode() or "{}")
        except Exception:
            return e.code, {}


def wait_health(timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(BASE + "/api/health", timeout=2):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def wait_until(pred, timeout=60, tick=0.3):
    t0 = time.time()
    snap = None
    while time.time() - t0 < timeout:
        code, snap = call("/api/state")
        if code == 200 and pred(snap):
            return snap
        time.sleep(tick)
    return snap


def by_id(snap):
    return {n["id"]: n for n in snap["nodes"]}


# ------------------------------------------------------------------ 单元检查
def module_checks():
    print("[0] 执行器内核：文件沙箱、命令白名单、测试解析、安全扫描")
    import tool_agent

    ws = tool_agent.Workspace(tempfile.mkdtemp(prefix="ws-")).ensure()
    info = ws.write("src/a.py", "print(1)\nprint(2)\n")
    check(info["bytes"] > 0 and info["path"] == "src/a.py", "写入相对路径并返回字节数")
    check(ws.read("src/a.py")["content"].count("print") == 2, "读回落盘内容一致")
    check("src/a.py" in [f["path"] for f in ws.list("")], "文件列表包含新写入的文件")

    for bad in ("../escape.py", "C:/Windows/x.py", "/etc/passwd", "a/../../b.py"):
        try:
            ws.write(bad, "x")
            check(False, "越界路径应被拒绝：%s" % bad)
        except ValueError:
            check(True, "越界路径被拒绝：%s" % bad)

    try:
        ws.run("rm -rf /")
        check(False, "非白名单命令应被拒绝")
    except ValueError as exc:
        check("白名单" in str(exc), "非白名单命令被拒绝（rm）")
    try:
        ws.run("python -c 'print(1)' && echo x")
        check(False, "shell 连接符应被拒绝")
    except ValueError as exc:
        check("连接符" in str(exc), "shell 连接符被拒绝（&&）")

    ws.write("tests/test_ok.py",
             "import unittest\n\n\nclass T(unittest.TestCase):\n"
             "    def test_a(self):\n        self.assertTrue(True)\n")
    cmd = tool_agent.detect_test_command(ws)
    res = ws.run(cmd, timeout=120)
    check(res["ok"], "工作区内真的能跑通测试（exit 0）：%s" % cmd)
    parsed = tool_agent.parse_test_output(res["output"])
    check(parsed["found"] and parsed["passed"] >= 1,
          "从测试输出解析出通过数（%d/%d）" % (parsed["passed"], parsed["total"]))

    for text, want in (("1 passed, 0 failed", (1, 1)), ("12 passed in 0.3s", (12, 12)),
                       ("Tests: 3 passed, 3 total", (3, 3)), ("# pass 5\n# fail 1", (5, 6)),
                       ("Ran 4 tests in 0.02s\n\nOK", (4, 4)),
                       ("Ran 5 tests in 0.1s\n\nFAILED (failures=2)", (3, 5))):
        got = tool_agent.parse_test_output(text)
        check((got["passed"], got["total"]) == want,
              "解析 %r → %d/%d" % (text, want[0], want[1]))

    check(tool_agent.detect_test_command(ws) != "", "能识别出测试命令")
    empty = tool_agent.Workspace(tempfile.mkdtemp(prefix="ws-empty-")).ensure()
    empty.write("src/b.py", "print(1)\n")
    check(tool_agent.detect_test_command(empty) != "", "仅在 Python 源码里也能推断出 pytest")

    ws.write("src/danger.py", "import os\nos.system('ls')\npassword = 'hunter2'\n")
    findings = tool_agent.scan_workspace(ws)
    check(len(findings) >= 2, "安全扫描能发现 os.system 与硬编码口令（%d 处）" % len(findings))
    check(0 <= tool_agent.security_score(findings) < 100, "安全分按发现项扣分而不是恒等于满分")


def spec_import_checks():
    print("\n[1] 设计规格文档导入")
    md = """# 内部管理后台 v2

面向运营同学的内部后台，需要登录、订单与报表三块能力。

## 模块

### 登录：账号密码 + 短信验证码
账号密码登录，连续错误 5 次锁定。
验收：错误提示明确
验收：锁定 5 次后可自助解锁

### 订单列表
支持按状态筛选、导出 CSV。
- [ ] 导出 1 万行不超时
验收：分页每页 20 条

## 全局约束
- 不新增第三方依赖
- 密码只存 hash
"""
    code, r = call("/api/specs/import", {"text": md, "name": "后台规格.md"})
    spec = (r or {}).get("spec") or {}
    mods = spec.get("modules") or []
    check(code == 200 and r.get("ok"), "Markdown 规格导入接口返回 ok")
    check(len(mods) >= 2, "拆出至少 2 个模块（实际 %d）" % len(mods))
    check(all(m.get("name") for m in mods), "每个模块都有名字")
    acc = sum(len(m.get("acceptance") or []) for m in mods)
    check(acc >= 2, "抽到验收条款（%d 条）" % acc)
    check(len(spec.get("constraints") or []) >= 2, "抽到全局约束")
    check(bool(spec.get("trace")), "解析过程有可解释的 trace")
    check("登录" in json.dumps(mods, ensure_ascii=False), "模块名保留原文（登录）")

    js = json.dumps({"name": "JSON 规格", "summary": "从 JSON 导入",
                     "modules": [{"id": "M1", "name": "支付", "priority": "P0",
                                  "acceptance": ["幂等"]}]}, ensure_ascii=False)
    code, r2 = call("/api/specs/import", {"text": js})
    check(code == 200 and len((r2.get("spec") or {}).get("modules") or []) == 1,
          "JSON 规格也能导入")

    code, r3 = call("/api/specs/import", {"text": "   "})
    check(code == 400 and not r3.get("ok"), "空文档被明确拒绝（400）")

    # 导入的规格要能存下来并分解出分层任务
    code, saved = call("/api/specs", {"name": spec.get("name"), "summary": spec.get("summary"),
                                      "body": md, "constraints": spec.get("constraints"),
                                      "modules": mods})
    sid = (saved.get("spec") or {}).get("id")
    check(code == 200 and sid, "导入结果可以保存成规格")
    code, prev = call("/api/specs/%s/preview" % sid, {"template": "default"})
    tasks = (prev or {}).get("tasks") or []
    phases = {t.get("phase") for t in tasks}
    check(code == 200 and tasks, "规格能分解成任务（%d 条）" % len(tasks))
    check({"设计", "编码", "测试", "评审", "交付"} <= phases,
          "任务覆盖设计/编码/测试/评审/交付五个阶段")
    with_files = [t for t in tasks if t.get("files")]
    check(len(with_files) >= len(tasks) // 2, "多数任务写明了落地文件（%d/%d）"
          % (len(with_files), len(tasks)))
    check(all(t.get("what") and t.get("how") for t in tasks), "每条任务都有「做什么」和「怎么做」")
    check(all(isinstance(t.get("steps"), list) and t["steps"] for t in tasks),
          "每条任务都拆出了子步骤")
    check(any(t.get("phase") == "编码" and t.get("kind") == "implementation" for t in tasks),
          "编码阶段有 implementation 任务")
    check(any(t.get("role") in ("测试", "评审") for t in tasks), "存在测试/评审角色的任务")


def settings_checks():
    print("\n[2] 模型配置与密钥处理")
    code, r = call("/api/settings")
    s = (r or {}).get("settings") or {}
    check(code == 200 and s.get("has_key"), "环境变量里的密钥被识别（has_key）")
    check("api_key" not in s, "接口不回传原始 api_key 字段")
    check("…" in (s.get("key_hint") or "") or "****" in (s.get("key_hint") or ""),
          "密钥以掩码形式回显（%s）" % s.get("key_hint"))
    check(s.get("active"), "已配置且 mode=llm 时标记为可用")

    code, r = call("/api/settings", {"model": "fake-coder-2", "max_steps": 9})
    s2 = (r or {}).get("settings") or {}
    check(code == 200 and s2.get("model") == "fake-coder-2", "保存后模型名生效")
    check(s2.get("max_steps") == 9, "保存后工具步数上限生效")
    check(s2.get("has_key"), "只改模型名不会把密钥清空")

    code, r = call("/api/settings/probe", {})
    check(code == 200 and (r.get("probe") or {}).get("ok"),
          "测试连接能打通假端点（%sms）" % ((r.get("probe") or {}).get("ms")))
    check((r.get("probe") or {}).get("usage") is not None, "探测结果带回 token 用量")


def end_to_end(llm_url):
    print("\n[3] 端到端：模型真的写文件、真的跑测试")
    code, detail = call("/api/workflows/wf_login_001")
    base_def = detail["workflow"]["draft"]["definition"]
    nodes = base_def["nodes"]
    start_tpl = dict(next(n for n in nodes if n["type"] == "start"))
    agent_tpl = dict(next(n for n in nodes if n["type"] == "agent" and n.get("role") == "coder"))
    end_tpl = dict(next(n for n in nodes if n["type"] == "end"))

    def mk(tpl, nid, **kw):
        node = dict(tpl)
        node.update(kw)
        node["id"] = nid
        return node

    new_nodes = [
        mk(start_tpl, "S", name="开始", role="system", deps=[]),
        mk(agent_tpl, "C1", name="实现加法服务", role="coder", deps=["S"], kind="implementation",
           out_schema="code-result-v1", timeout=90, acceptance=[], task_id=None,
           task_title=None, files=[], steps=[]),
        mk(agent_tpl, "T1", name="跑测试", role="tester", deps=["C1"], kind="verification",
           out_schema="test-v1", timeout=90, acceptance=["add(1,2)==3"], task_id=None,
           task_title=None, files=[], steps=[]),
        mk(end_tpl, "E", name="结束", role="system", deps=["T1"]),
    ]
    definition = dict(base_def, workflow_id="wf_llm_e2e", name="LLM 端到端", version="v1",
                      nodes=new_nodes,
                      edges=[{"source": "S", "target": "C1", "label": ""},
                             {"source": "C1", "target": "T1", "label": ""},
                             {"source": "T1", "target": "E", "label": ""}])
    code, saved = call("/api/workflows/wf_login_001", {"definition": definition}, method="PUT")
    check(code == 200 and saved.get("ok"), "自定义草稿保存成功（含 tester 节点）")

    code, started = call("/api/run/start", {"demo": False, "speed": 4, "qa_mode": "pass"})
    check(code == 200 and started.get("ok"), "运行已启动（agent=llm）")
    run_id = started.get("run_id")

    snap = wait_until(lambda s: s["run"]["status"] in ("SUCCESS", "FAILED", "WAITING_HUMAN"),
                      timeout=120)
    status = (snap or {}).get("run", {}).get("status")
    check(status == "SUCCESS", "运行以 SUCCESS 结束（实际 %s）" % status)
    check(FakeLLM.calls >= 4, "假模型确实收到多轮工具调用（%d 次）" % FakeLLM.calls)

    nodes_now = by_id(snap or {"nodes": []})
    c1, t1 = nodes_now.get("C1") or {}, nodes_now.get("T1") or {}
    out1 = c1.get("output") or {}
    check(c1.get("status") == "SUCCESS", "C1 编码节点成功")
    check(bool(out1.get("files")), "C1 产物里列出了落盘文件：%s" % (out1.get("files"),))
    check((out1.get("loc") or 0) > 0, "C1 统计出真实代码行数（%s 行）" % out1.get("loc"))

    out2 = t1.get("output") or {}
    check(t1.get("status") == "SUCCESS", "T1 测试节点成功")
    check((out2.get("passed") or 0) >= 2, "测试节点记录了真实通过数（%s）" % out2.get("passed"))
    check((out2.get("total") or 0) >= 2, "测试节点记录了真实用例总数（%s）" % out2.get("total"))
    check(("pytest" in (out2.get("command") or ""))
          or ("unittest" in (out2.get("command") or "")),
          "测试命令来自真实探测：%s" % out2.get("command"))
    check(("passed" in (out2.get("output_tail") or ""))
          or ("Ran " in (out2.get("output_tail") or "")),
          "带回测试原始输出片段")

    code, files = call("/api/runs/%s/files" % run_id)
    paths = [f["path"] for f in (files.get("files") or [])]
    check(code == 200 and "src/app/service.py" in paths, "工作区里真的有 src/app/service.py")
    check("tests/test_service.py" in paths, "工作区里真的有 tests/test_service.py")
    arts = {a["path"]: a for a in (files.get("artifacts") or [])}
    check("src/app/service.py" in arts and arts["src/app/service.py"]["bytes"] > 0,
          "产物清单里有文件大小（%s 字节）"
          % (arts.get("src/app/service.py", {}).get("bytes")))

    code, f = call("/api/runs/%s/file?path=src/app/service.py" % run_id)
    check(code == 200 and "def add" in (f.get("content") or ""),
          "文件内容接口能读回真实源码")
    code, bad = call("/api/runs/%s/file?path=../../server.py" % run_id)
    check(code == 400 and not bad.get("ok"), "越界读取被拒绝（400）")

    snap2 = call("/api/state")[1]
    arts_run = snap2["run"].get("artifacts") or []
    check(len(arts_run) >= 2, "运行快照登记了产物（%d 个）" % len(arts_run))
    events = [e.get("type") for e in (snap2.get("events") or [])]
    check("artifact.saved" in events, "事件流里有 artifact.saved")
    check("test.result" in events, "事件流里有 test.result")


def main():
    global BASE
    llm_srv, llm_url = start_llm()
    proc, BASE, data_dir = start_server(llm_url)
    try:
        print("[环境] 假模型端点 %s" % llm_url)
        check(wait_health(), "被测服务已启动")
        code, agents = call("/api/agents")
        check((agents.get("settings") or {}).get("active"), "服务端已识别为真实模型模式")

        module_checks()
        spec_import_checks()
        settings_checks()
        end_to_end(llm_url)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        llm_srv.shutdown()

    total = len(PASS) + len(FAIL)
    print("\n结果: %s (%d/%d)" % ("PASS" if not FAIL else "FAIL", len(PASS), total))
    for f in FAIL:
        print("  失败: " + f)
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())