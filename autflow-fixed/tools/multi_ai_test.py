"""F · 多 AI 接入 / 彻底清除密钥 / 任务分组 验收。

用假 OpenAI 兼容端点 + 两个真实进程（服务器重启）验证：

  1. 保存的密钥能被**彻底**清除：磁盘字段真的消失、模式回到模拟，
     重启后也不会复活；环境变量注入的密钥在显式清除后同样不再注入；
  2. 接口只回掩码，掩码回传不会覆盖真密钥，明文密钥永不出现在任何响应里；
  3. 可以同时接入多个 AI：增 / 改 / 删 / 探活，空 key 或掩码 key 视为"不改动"；
  4. 每个讨论角色、每个任务分组都能指派不同的 AI，讨论时确实按分工调用
     （假端点记录每次请求的 model 与 Authorization），发言带 AI 标签；
  5. 任务分解出更细的 part（骨架/数据/接口/功能/测试/评审/集成/交付），
     节点带 part / part_label，画布与任务表能按组同色归拢；
  6. 节点可以单独指定 AI，引擎解析顺序为：节点 > 分组 > 默认。
"""
import json
import os
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
DATA_DIR = ""


def check(cond, label):
    (PASS if cond else FAIL).append(label)
    print(("  ✓ " if cond else "  ✗ ") + label)


class FakeLLM(BaseHTTPRequestHandler):
    calls = []
    lock = threading.Lock()
    vote_counts = {}

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        payload = {}
        if n:
            try:
                payload = json.loads(self.rfile.read(n).decode("utf-8"))
            except Exception:
                payload = {}
        messages = payload.get("messages") or []
        system = "\n".join(str(m.get("content") or "") for m in messages
                           if m.get("role") == "system")
        auth = (self.headers.get("Authorization") or "").replace("Bearer ", "")
        with FakeLLM.lock:
            FakeLLM.calls.append({
                "model": payload.get("model"),
                "auth": auth,
                "system": system[:120],
                "path": self.path,
            })
            idx = len(FakeLLM.calls)

        if "主持人" in system:
            tasks = [
                {"id": "T1", "title": "工程骨架", "role": "架构师", "phase": "设计",
                 "part": "skeleton", "estimate": 1,
                 "what": "搭骨架", "how": "建 src/", "files": ["src/main.py"],
                 "steps": [{"title": "建目录", "how": "src/"}]},
                {"id": "T2", "title": "借阅数据模型", "role": "后端", "phase": "编码",
                 "part": "data", "estimate": 2,
                 "what": "建借阅表", "how": "migrations/001.sql",
                 "files": ["src/models/loan.py"],
                 "steps": [{"title": "建表", "how": "sql"}]},
                {"id": "T3", "title": "借阅接口", "role": "后端", "phase": "编码",
                 "part": "api", "ai": "ai-2", "estimate": 2,
                 "what": "实现借还接口", "how": "POST /api/loans",
                 "files": ["src/api/loans.py"],
                 "steps": [{"title": "写接口", "how": "http"}], "depends_on": ["T2"]},
                {"id": "T4", "title": "测试借阅", "role": "测试", "phase": "测试",
                 "part": "test", "estimate": 1,
                 "what": "写用例", "how": "unittest",
                 "files": ["tests/test_loans.py"],
                 "steps": [{"title": "写用例", "how": "unittest"}], "depends_on": ["T3"]},
                {"id": "T5", "title": "评审与交付", "role": "评审", "phase": "评审",
                 "part": "review", "estimate": 1,
                 "what": "评审", "how": "核对验收",
                 "files": [], "steps": [{"title": "核对", "how": "逐条"}],
                 "depends_on": ["T4"]},
            ]
            return self._say(json.dumps({"tasks": tasks}, ensure_ascii=False))
        if "表态" in system:
            label = "?"
            for lb in ("产品经理", "架构师", "开发者", "测试", "评审"):
                if ("你是" + lb) in system:
                    label = lb
                    break
            with FakeLLM.lock:
                FakeLLM.vote_counts[label] = FakeLLM.vote_counts.get(label, 0) + 1
                nth = FakeLLM.vote_counts[label]
            # 第一轮：测试与评审明确反对（缺少可验收细节），逼出第二轮修订
            if nth == 1 and label in ("测试", "评审"):
                return self._say(json.dumps({"stance": "block",
                                             "reason": "验收条款缺少锁定解除，不能这么做"},
                                            ensure_ascii=False))
            return self._say(json.dumps({"stance": "agree",
                                         "reason": "按这个做"}, ensure_ascii=False))
        if "多智能体评审会" in system:
            name = "角色"
            for lb in ("产品经理", "架构师", "开发者", "测试", "评审"):
                if ("你是" + lb) in system:
                    name = lb
                    break
            return self._say("结论：%s 的意见。\n要点：\n- 第 %d 次发言" % (name, idx))
        return self._say("结论：收到。\n要点：\n- 无补充")

    def _say(self, content):
        body = json.dumps({"choices": [{"message": {"role": "assistant",
                                                    "content": content},
                                          "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 12, "completion_tokens": 8}}
                          ).encode()
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
    p = s.getsockname()[1]
    s.close()
    return p


def start_fake():
    port = free_port()
    srv = QuietServer(("127.0.0.1", port), FakeLLM)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/v1" % port


def start_app(data_dir, llm_url=None, env_key=False, port=None):
    port = port or free_port()
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    env["PYTHONIOENCODING"] = "utf-8"
    for k in ("AUTFLOW_LLM_BASE_URL", "AUTFLOW_LLM_API_KEY", "AUTFLOW_LLM_MODEL",
              "AUTFLOW_LLM_MODE"):
        env.pop(k, None)
    if env_key and llm_url:
        env["AUTFLOW_LLM_BASE_URL"] = llm_url
        env["AUTFLOW_LLM_API_KEY"] = "sk-env-secret-999888"
        env["AUTFLOW_LLM_MODEL"] = "env-model"
        env["AUTFLOW_LLM_MODE"] = "llm"
    proc = subprocess.Popen([sys.executable, SERVER, "--host", "127.0.0.1",
                             "--port", str(port)],
                            cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port


def call(base, path, body=None, method=None, timeout=40):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, json.loads(raw or "{}"), raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode() or "{}"
        try:
            return e.code, json.loads(raw), raw
        except Exception:
            return e.code, {}, raw


def wait_health(base, timeout=30):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(base + "/api/health", timeout=2):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def wait_session(base, sid, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, r, _ = call(base, "/api/plan/" + sid)
        s = r.get("session") or {}
        if s.get("status") in ("done", "failed", "cancelled", "error"):
            return s
        time.sleep(0.25)
    return {}


def settings_file(data_dir):
    return os.path.join(data_dir, "settings.json")


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def main():
    global BASE, DATA_DIR
    fake, llm_url = start_fake()
    data_dir = tempfile.mkdtemp(prefix="autflow-multi-ai-")
    DATA_DIR = data_dir
    proc, BASE = start_app(data_dir, llm_url)
    try:
        if not wait_health(BASE):
            print("服务器没起来")
            sys.exit(2)

        print("\n[0] 先把主密钥存了：只回掩码，磁盘上有明文")
        st, r, raw = call(BASE, "/api/settings", {
            "provider": "openai", "base_url": llm_url, "model": "primary-model",
            "api_key": "sk-primary-secret-123456", "mode": "llm",
            "temperature": 0.2, "max_steps": 4, "max_tokens": 1024, "timeout": 30})
        s = r.get("settings") or {}
        check(st == 200 and s.get("has_key"), "主密钥已保存")
        check("sk-primary-secret-123456" not in raw, "保存响应不回明文密钥")
        check(s.get("key_hint", "").startswith("sk-") and "…" in s.get("key_hint", ""),
              "只回掩码（%s）" % s.get("key_hint"))
        check((read_json(settings_file(data_dir)).get("api_key")
               == "sk-primary-secret-123456"), "磁盘上确实存了密钥（本机文件）")
        check(s.get("active") is True, "配置完整时 active=true")

        print("\n[1] 接入第二个 / 第三个 AI")
        st, r, raw = call(BASE, "/api/ais", {
            "label": "DeepSeek 评审", "provider": "deepseek",
            "base_url": llm_url, "model": "deepseek-reasoner",
            "api_key": "sk-ai1-secret-111"})
        ai1 = (r.get("ai") or {}).get("id")
        check(st == 200 and ai1, "新增 ai-1（%s）" % ai1)
        st, r, _ = call(BASE, "/api/ais", {
            "label": "Qwen 开发", "provider": "dashscope",
            "base_url": llm_url, "model": "qwen-max",
            "api_key": "sk-ai2-secret-222"})
        ai2 = (r.get("ai") or {}).get("id")
        check(st == 200 and ai2 and ai2 != ai1, "新增 ai-2（%s）" % ai2)
        s = r.get("settings") or {}
        check(len(s.get("ais") or []) == 2, "GET /api/ais 能列出两个接入点")
        check(all(("sk-ai" not in json.dumps(a)) for a in (s.get("ais") or [])),
              "列表里没有明文密钥")
        check(all(a.get("has_key") for a in (s.get("ais") or [])), "两个都有密钥")

        print("\n[2] 改 AI 时：空 key / 掩码 key 都不覆盖真密钥")
        st, r, _ = call(BASE, "/api/ais", {"id": ai1, "model": "deepseek-chat",
                                           "api_key": ""})
        a = [x for x in (r.get("settings") or {}).get("ais", []) if x["id"] == ai1][0]
        check(a.get("has_key") and a.get("key_hint", "").endswith("111"),
              "空 key 不清掉已存密钥")
        before = a.get("key_hint")
        st, r, _ = call(BASE, "/api/ais", {"id": ai1, "api_key": "sk-ai1…111"})
        a = [x for x in (r.get("settings") or {}).get("ais", []) if x["id"] == ai1][0]
        check(a.get("key_hint") == before, "掩码回传不会覆盖真密钥")

        print("\n[3] 探活走的是这个 AI 自己的密钥与模型")
        with FakeLLM.lock:
            FakeLLM.calls.clear()
        st, r, raw = call(BASE, "/api/ais/%s/probe" % ai1, {})
        check(st == 200 and (r.get("probe") or {}).get("ok"), "ai-1 探活成功")
        with FakeLLM.lock:
            hit = [c for c in FakeLLM.calls if c["model"] == "deepseek-chat"]
        check(bool(hit), "假端点收到 ai-1 的模型名 deepseek-chat")
        check(any(c["auth"] == "sk-ai1-secret-111" for c in hit),
              "用的是 ai-1 自己的密钥（而不是主密钥）")
        check("sk-ai1-secret-111" not in raw, "探活响应不回明文")

        print("\n[4] 角色 / 分组指派（两次部分更新要合并）")
        st, r, _ = call(BASE, "/api/assign", {"role_ais": {"pm": ai1}})
        check(st == 200 and (r["settings"]["role_ais"] or {}).get("pm") == ai1,
              "指派 pm → ai-1")
        st, r, _ = call(BASE, "/api/assign", {"role_ais": {"reviewer": ai2}})
        ra = (r["settings"]["role_ais"] or {})
        check(ra.get("pm") == ai1 and ra.get("reviewer") == ai2,
              "第二次指派不覆盖第一次")
        st, r, _ = call(BASE, "/api/assign", {"role_ais": {
            "architect": ai1, "coder": ai1, "tester": ai2}})
        ra = (r["settings"]["role_ais"] or {})
        check(len(ra) == 5 and ra.get("tester") == ai2,
              "五个讨论角色都能分工（%s）" % json.dumps(ra, ensure_ascii=False))
        st, r, _ = call(BASE, "/api/assign", {"part_ais": {"data": ai1}})
        st, r, _ = call(BASE, "/api/assign", {"part_ais": {"api": ai2}})
        pa = (r["settings"]["part_ais"] or {})
        check(pa.get("data") == ai1 and pa.get("api") == ai2, "分组指派可累加")

        print("\n[5] 讨论：不同角色真的走不同 AI，且能互相反驳")
        st, r, raw = call(BASE, "/api/specs/import", {
            "name": "图书借阅", "text": (
                "# 图书借阅系统\n\n## 模块\n- 借阅管理：借书与还书\n"
                "- 读者管理：注册与查询\n\n## 接口\n"
                "- POST /api/loans 借书\n- POST /api/returns 还书\n\n"
                "## 数据表\n- loans(id, book_id, user_id, due_at)\n"
                "- users(id, name, phone)\n")})
        parsed = r.get("spec") or {}
        st2, r2, _ = call(BASE, "/api/specs", parsed)
        spec_id = ((r2.get("spec") or {}).get("id") or r2.get("id"))
        check(st == 200 and st2 in (200, 201) and spec_id,
              "导入规格（%s）" % spec_id)
        with FakeLLM.lock:
            FakeLLM.calls.clear()
        st, r, _ = call(BASE, "/api/specs/%s/discuss" % spec_id, {"template": "default"})
        sid = r.get("session_id")
        check(st == 200 and sid, "讨论已开始（%s）" % sid)
        sess = wait_session(BASE, sid)
        check(sess.get("status") == "done", "讨论跑完（%s）" % sess.get("status"))
        mins = sess.get("minutes") or []
        check(len(mins) >= 6, "两轮共 %d 条发言（第二轮只请反对者回应）" % len(mins))
        check(any(m.get("round") == 2 for m in mins), "第二轮有人再次发言（反驳后更正）")
        labels = {}
        for m in mins:
            labels.setdefault(m.get("role"), set()).add(m.get("ai"))
        check(labels.get("pm") == {ai1}, "产品经理的发言来自 ai-1")
        check(labels.get("reviewer") == {ai2}, "评审的发言来自 ai-2")
        check(all(m.get("ai_label") for m in mins),
              "每条发言都带 AI 标签（%s）" % sorted({m.get("ai_label") for m in mins}))
        with FakeLLM.lock:
            models = {c["model"] for c in FakeLLM.calls}
        check({"deepseek-chat", "qwen-max"} <= models,
              "假端点收到两个不同的模型调用（%s）" % sorted(models))
        rounds = sess.get("votes") or []
        flat = [v for rnd in rounds for v in (rnd.get("votes") or [])]
        check(any(v.get("stance") == "block" for v in flat),
              "第一轮确实有人反对（互相反驳，而不是一路同意）")
        check(len(rounds) >= 2, "反对触发了第二轮修订（表决 %d 轮）" % len(rounds))
        check(flat[-5:] and all(v.get("stance") == "agree" for v in flat[-5:]),
              "第二轮全部同意，形成共识")
        check(sess.get("consensus", {}).get("status") == "reached",
              "修正后收敛出共识")

        print("\n[6] 收口任务清单 → 编排：细分组 + 节点 AI")
        st, r, _ = call(BASE, "/api/plan/%s" % sid, {"template": "default"})
        check(st == 200 and r.get("ok"), "任务清单已落成 DAG 草稿")
        st, wf, _ = call(BASE, "/api/workflow")
        defn = wf.get("definition") or wf.get("workflow") or {}
        nodes = defn.get("nodes") or []
        parts = {n.get("part") for n in nodes if n.get("part")}
        check(len(nodes) >= 12, "画布上有完整流程（%d 个节点）" % len(nodes))
        check({"skeleton", "data", "api", "feature", "test", "review",
               "integrate", "delivery"} <= parts,
              "分组覆盖 8 类（实际 %s）" % sorted(parts))
        by_part = {}
        for n in nodes:
            by_part.setdefault(n.get("part"), []).append(n)
        check(len(by_part) >= 5 and all(len(v) >= 1 for v in by_part.values()),
              "每个分组都有节点，可画同色底框")
        check(all(n.get("part_label") for n in nodes if n.get("part")),
              "节点带可读分组名")
        api_nodes = [n for n in nodes if n.get("part") == "api"]
        check(any(n.get("ai") == ai2 for n in api_nodes),
              "模型写的节点级 AI 指派被保留（%s）" % (api_nodes[0].get("ai") if api_nodes else "—"))

        print("\n[7] 引擎的解析顺序：节点 > 分组 > 默认")
        import server as app_server
        app_server.SETTINGS.data["part_ais"] = {"data": ai1, "api": ai2}
        app_server.SETTINGS.data["ais"] = [{"id": ai1}, {"id": ai2}]
        eng = app_server.Engine
        check(eng._ai_for(None, {"ai": ai2, "part": "data"}) == ai2,
              "节点显式指定优先于分组")
        check(eng._ai_for(None, {"part": "data"}) == ai1, "没有节点指定时走分组")
        check(eng._ai_for(None, {"part": "feature"}) == "", "都没指定时回退默认")
        stub = type("E", (), {"cfg": {"agent": "mock"},
                              "_ai_for": lambda self, n: eng._ai_for(None, n)})()
        check(eng._agent_kind(stub, {"role": "coder", "ai": ai1}) == "tool",
              "mock 模式下被指派的节点仍走真实模型（不静默忽略）")
        check(eng._agent_kind(stub, {"role": "coder"}) == "mock",
              "没指派的节点保持模拟")

        print("\n[8] 删除 AI：角色/分组回落到默认")
        st, r, _ = call(BASE, "/api/ais/%s/delete" % ai1, {})
        s = r.get("settings") or {}
        check(st == 200 and ai1 not in [a["id"] for a in s.get("ais", [])],
              "ai-1 已删除")
        check(ai1 not in json.dumps(s.get("role_ais") or {}), "角色映射里不再引用 ai-1")
        check(ai1 not in json.dumps(s.get("part_ais") or {}), "分组映射里不再引用 ai-1")

        print("\n[9] 没配密钥的 AI 探活要明确报错（不能 500）")
        st, r, _ = call(BASE, "/api/ais", {"label": "空密钥", "base_url": llm_url,
                                           "model": "m"})
        empty_id = (r.get("ai") or {}).get("id")
        st, r, _ = call(BASE, "/api/ais/%s/probe" % empty_id, {})
        check(st == 400 and r.get("ok") is False and r.get("message"),
              "空密钥探活 → 400 且带原因")

        print("\n[10] 彻底清除：磁盘字段消失、重启不复活")
        st, r, raw = call(BASE, "/api/settings/clear-key", {})
        s = r.get("settings") or {}
        check(st == 200 and not s.get("has_key"), "清除后 has_key=false")
        check(s.get("mode") == "mock", "模式回到模拟")
        disk = read_json(settings_file(data_dir))
        check("api_key" not in disk or not disk.get("api_key"),
              "settings.json 里不再有密钥字段/值")
        check(disk.get("ignore_env_key") is True,
              "写了忽略环境变量的标记，重启不会被塞回来")
        st, r, raw = call(BASE, "/api/settings")
        check(not (r.get("settings") or {}).get("has_key"), "再次读取仍是未配置")
        check("sk-primary-secret-123456" not in raw, "读取响应不含旧密钥")

        proc.terminate()
        proc.wait(timeout=10)
        proc2, base2 = start_app(data_dir, llm_url)
        BASE = base2
        try:
            check(wait_health(base2), "重启后服务器可用")
            st, r, _ = call(base2, "/api/settings")
            s = r.get("settings") or {}
            check(not s.get("has_key"), "重启后 has_key 仍为 false（彻底清除）")
            check(s.get("mode") == "mock", "重启后仍是模拟模式")
        finally:
            proc2.terminate()
            proc2.wait(timeout=10)

        print("\n[11] 环境变量注入的密钥也能被清除")
        env_dir = tempfile.mkdtemp(prefix="autflow-multi-ai-env-")
        proc3, base3 = start_app(env_dir, llm_url, env_key=True)
        try:
            check(wait_health(base3), "带环境变量密钥的服务器可用")
            st, r, _ = call(base3, "/api/settings")
            s = r.get("settings") or {}
            check(s.get("has_key") and s.get("key_source") == "env",
                  "密钥来源标为 env（界面要如实提示）")
            st, r, _ = call(base3, "/api/settings/clear-key", {})
            check(not (r.get("settings") or {}).get("has_key"), "清除后当前进程不再有密钥")
        finally:
            proc3.terminate()
            proc3.wait(timeout=10)
        proc4, base4 = start_app(env_dir, llm_url, env_key=True)
        try:
            check(wait_health(base4), "同一环境变量重启")
            st, r, _ = call(base4, "/api/settings")
            s = r.get("settings") or {}
            check(not s.get("has_key"), "环境变量不再把密钥塞回来")
            check(s.get("key_source") in ("", None), "key_source 不再标 env")
        finally:
            proc4.terminate()
            proc4.wait(timeout=10)

        print("\n[12] 重新填密钥后，环境变量标记被清掉")
        proc5, base5 = start_app(data_dir, llm_url)
        try:
            wait_health(base5)
            st, r, _ = call(base5, "/api/settings", {
                "base_url": llm_url, "model": "again-model",
                "api_key": "sk-again-777", "mode": "llm"})
            s = r.get("settings") or {}
            check(s.get("has_key") and s.get("active"), "重新保存密钥后恢复可用")
            disk = read_json(settings_file(data_dir))
            check(disk.get("ignore_env_key") is False, "重新填 key 后环境变量恢复可用")
        finally:
            proc5.terminate()
            proc5.wait(timeout=10)
    finally:
        for p in (proc,):
            try:
                p.terminate()
                p.wait(timeout=10)
            except Exception:
                pass
        fake.shutdown()

    print("\n" + "=" * 64)
    print("F · 多 AI / 清除密钥 / 任务分组：%d 通过，%d 失败" % (len(PASS), len(FAIL)))
    if FAIL:
        for f in FAIL:
            print("  ✗ " + f)
        sys.exit(1)


if __name__ == "__main__":
    main()