"""E · 多智能体规划验收：讨论 → 任务清单 → 工作流。

用一个**假 OpenAI 兼容端点**扮演 5 个角色，端到端验证：

  1. 没配模型时开讨论会被明确拒绝（而不是转圈）；
  2. 讨论真的按角色推进：5 位 + 3 位修订，共 8 条发言，每人只说自己那一段；
  3. 收口只要任务清单：模型给的脏数据（坏依赖、环、缺字段）由代码修正，
     修了什么要能说清楚（fixes / trace）；
  4. 任务清单能编排成通过校验的 DAG 并落库，任务字段（phase/what/how/files/steps）齐全；
  5. 模型乱答时明确失败并提示改用离线分解，不产出半张图；
  6. 讨论可以中止。
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

MODULE_TASKS = [
    {"id": "T1", "title": "工程骨架与依赖约束", "phase": "设计", "role": "架构师",
     "priority": "P0", "estimate": 2,
     "what": "搭出可运行骨架并锁定零第三方依赖",
     "how": "建 src/ 与 tests/，写 README 与启动脚本，用标准库实现入口",
     "files": ["src/main.py", "README.md"],
     "steps": [{"title": "建目录", "how": "src/ tests/ data/"},
               {"title": "写入口", "how": "用 http.server 起一个最小服务"}],
     "acceptance": ["服务能启动", "不引入第三方包"]},
    {"id": "T2", "title": "登录：账号密码与验证码", "phase": "编码", "role": "后端",
     "priority": "P0", "estimate": 4,
     "what": "实现账号密码登录与短信验证码校验，错误 5 次锁定",
     "how": "在 src/auth.py 实现 login()，密码只存 sha256，用 src/store.py 存用户",
     "files": ["src/auth.py", "src/store.py"],
     "steps": [{"title": "用户存储", "how": "JSON 文件 + 原子写"},
               {"title": "登录校验", "how": "比对 hash，累计失败次数"}],
     "acceptance": ["正确密码能登录", "错误 5 次后锁定"],
     "depends_on": ["T1"]},
    {"id": "T3", "title": "登录单元测试", "phase": "测试", "role": "测试",
     "priority": "P0", "estimate": 2,
     "what": "覆盖登录成功/失败/锁定三条路径",
     "how": "在 tests/test_auth.py 用 unittest 写 3 条用例，跑 python -m unittest",
     "files": ["tests/test_auth.py"],
     "steps": [{"title": "写用例", "how": "unittest.TestCase"}],
     "acceptance": ["3 条用例全部通过"],
     "depends_on": ["T2"]},
    {"id": "T4", "title": "登录逻辑评审", "phase": "评审", "role": "评审",
     "priority": "P1", "estimate": 1,
     "what": "评审密码存储与锁定策略的风险",
     "how": "逐条核对验收条款，指出风险并给出结论",
     "files": [],
     "steps": [{"title": "核对", "how": "对照 AC 逐条检查"}],
     "acceptance": ["给出通过/不通过结论"],
     "depends_on": ["T3"]},
    {"id": "T5", "title": "集成验收与交付说明", "phase": "交付", "role": "产品经理",
     "priority": "P1", "estimate": 2,
     "what": "把上面的功能串起来跑一遍并写交付说明",
     "how": "启动服务走一遍登录流程，写 DELIVERY.md 记录命令与结果",
     "files": ["DELIVERY.md"],
     "steps": [{"title": "串测", "how": "手工跑一遍冒烟"}],
     "acceptance": ["冒烟通过"], "depends_on": ["T4"]},
]


def check(cond, label):
    (PASS if cond else FAIL).append(label)
    print(("  ✓ " if cond else "  ✗ ") + label)


class FakePlanner(BaseHTTPRequestHandler):
    calls = []
    lock = threading.Lock()
    mode = "ok"            # ok / garbage / slow
    delay = 0.0
    stance_mode = "mixed"  # mixed（首轮评审反对）/ agree / block
    vote_calls = []

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
        user_text = "\n".join(str(m.get("content") or "") for m in messages
                              if m.get("role") == "user")
        with FakePlanner.lock:
            FakePlanner.calls.append(system[:120])
        if FakePlanner.delay:
            time.sleep(FakePlanner.delay)

        if FakePlanner.mode == "garbage":
            return self._say("这不是 JSON，只是一段没用的说明。")

        labels = ("产品经理", "架构师", "开发者", "测试", "评审")
        label = "某角色"
        for lb in labels:
            if ("你是" + lb) in system:
                label = lb
                break
        if "主持人" in system:
            return self._say(json.dumps({"tasks": self._dirty_tasks()}, ensure_ascii=False))
        if "表态" in system:
            with FakePlanner.lock:
                FakePlanner.vote_calls.append(label)
                n = len(FakePlanner.vote_calls)
            mode = FakePlanner.stance_mode
            stance, reason = "agree", "可以按这个做"
            if mode == "block":
                stance, reason = "block", "缺少回滚方案，不能上线"
            elif mode == "mixed" and n <= 5 and label == "评审":
                stance, reason = "block", "验收条款里没有覆盖锁定解除"
            elif mode == "mixed" and n > 5:
                stance, reason = "agree", "分歧已收敛"
            return self._say(json.dumps({"stance": stance, "reason": reason},
                                        ensure_ascii=False))
        if "多智能体评审会" in system:
            greet = ("已按人工指令调整："
                     if "人工指令" in (system + user_text) else "")
            body = {
                "产品经理": "结论：先做登录，再做报表。\n要点：\n- 登录是 P0\n- 报表可以放到二期",
                "架构师": "结论：全标准库实现，单进程足够。\n要点：\n- 用 JSON 文件存用户\n- 不要引入 ORM",
                "开发者": "结论：三个文件够用。\n要点：\n- src/auth.py 放登录\n- src/store.py 放存储",
                "测试": "结论：用 unittest 跑三条路径。\n要点：\n- 成功/失败/锁定\n- 命令 python -m unittest",
                "评审": "结论：锁定策略要写清阈值。\n要点：\n- 验收条款缺「锁定解除」\n- 报表任务不该进本期",
            }.get(label, "结论：没有意见。")
            return self._say(greet + body)
        return self._say("结论：收到。\n要点：\n- 无补充")

    def _dirty_tasks(self):
        """故意做脏：坏依赖、环、缺字段、非法角色、绝对路径文件。"""
        tasks = [dict(t) for t in MODULE_TASKS]
        tasks[1]["depends_on"] = ["T1", "T99"]          # 坏依赖
        tasks[2]["depends_on"] = ["T4"]                  # 与 T4 构成环
        tasks[3]["depends_on"] = ["T4"]                  # 自依赖
        tasks[3]["role"] = "保洁"                        # 非法角色
        del tasks[0]["how"]                              # 缺「怎么做」
        del tasks[0]["steps"]                            # 缺子步骤
        tasks[1]["files"] = ["src/auth.py", "/etc/passwd", "../escape.py"]
        return tasks

    def _say(self, content):
        body = json.dumps({"choices": [{"message": {"role": "assistant", "content": content},
                                        "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 30, "completion_tokens": 20}}).encode()
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


def start_fake():
    port = free_port()
    srv = QuietServer(("127.0.0.1", port), FakePlanner)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/v1" % port


def start_server(llm_url, with_key=True):
    port = free_port()
    data_dir = tempfile.mkdtemp(prefix="autflow-plan-")
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    env["PYTHONIOENCODING"] = "utf-8"
    if with_key:
        env["AUTFLOW_LLM_BASE_URL"] = llm_url
        env["AUTFLOW_LLM_API_KEY"] = "sk-plan-test-123456"
        env["AUTFLOW_LLM_MODEL"] = "fake-planner"
        env["AUTFLOW_LLM_MODE"] = "llm"
    for k in ("AUTFLOW_LLM_URL", "AUTFLOW_LLM_KEY"):
        env.pop(k, None)
    proc = subprocess.Popen([sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
                            cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port


def call(base, path, body=None, method=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
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


def wait_health(base, timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(base + "/api/health", timeout=2):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def wait_plan(base, sid, timeout=90):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        code, data = call(base, "/api/plan/" + sid)
        if code == 200:
            last = data.get("session")
            if last and last.get("status") != "running":
                return last
        time.sleep(0.25)
    return last


def unit_checks():
    print("[0] 任务归一化（不经过网络）")
    import multi_agent as ma

    raw = {"tasks": [
        {"id": "A", "title": "设计登录", "phase": "设计", "role": "架构师"},
        {"id": "B", "title": "实现登录", "phase": "编码",
         "what": "实现登录", "how": "写 src/auth.py", "files": ["src/auth.py"],
         "steps": ["建表", "写校验"], "depends_on": ["A"]},
        {"id": "C", "title": "测试登录", "role": "tester", "depends_on": ["B", "Z"]},
    ]}
    tasks, fixes = ma.normalize_tasks(raw, {"name": "x"})
    check(len(tasks) == 3, "三条任务都被接收")
    check([t["id"] for t in tasks] == ["T1", "T2", "T3"], "任务编号重排为 T1..Tn")
    check(tasks[1]["depends_on"] == ["T1"], "原 id 依赖被重映射为新 id")
    check(all(t["depends_on"] != [t["id"]] for t in tasks), "没有自环")
    check(tasks[2]["phase"] == "测试" and tasks[2]["agent"] == "tester",
          "role=tester 归一到测试阶段与 tester 执行器")
    check(all(t["what"] and t["how"] for t in tasks), "缺「做什么/怎么做」的被补齐")
    check(all(t["steps"] for t in tasks), "缺子步骤的被补齐")
    check(any("Z" in f for f in fixes), "坏依赖被记录进 fixes：%s" % [f for f in fixes if "Z" in f][:1])

    bad_files, _ = ma.normalize_tasks(
        {"tasks": [{"title": "x", "files": ["/etc/passwd", "../a.py", "C:/b.py", "ok/x.py"]}]}, {})
    check(bad_files[0]["files"] == ["ok/x.py"], "越界/绝对路径被过滤，只留正常相对路径")

    cyc, cf = ma.normalize_tasks({"tasks": [
        {"id": "A", "title": "甲", "depends_on": ["B"]},
        {"id": "B", "title": "乙", "depends_on": ["A"]},
    ]}, {})
    pair = [t for t in cyc if t["title"] in ("甲", "乙")]
    check(len(pair) == 2 and all(t["depends_on"] in ([], ["T1"]) for t in pair),
          "互相依赖被断开（不会带着环进编排）：%s" % [t["depends_on"] for t in pair])
    check(any("循环" in f for f in cf), "断环有记录")

    empty, _ = ma.normalize_tasks({"tasks": []}, {})
    check(empty == [], "空清单明确返回空，由调用方决定回退")

    auto, _ = ma.normalize_tasks({"tasks": [{"title": "随便做点什么"}]}, {})
    check(len(auto) >= 2 and any(t["kind"] == "implementation" for t in auto)
          and any(t["kind"] in ("verification", "review") for t in auto),
          "只有一条任务时自动补编码与测试，保证图可执行")

    parsed = ma._parse_speech("结论：先做登录。\n要点：\n- 登录是 P0\n- 报表二期")
    check(parsed["content"].startswith("结论") and len(parsed["points"]) == 2,
          "发言解析出结论与 2 条要点")
    obj = ma._as_json_object('```json\n{"tasks":[{"title":"a"}]}\n```')
    check(obj and obj["tasks"][0]["title"] == "a", "能从代码块里抠出 JSON")


def flow_checks(base, no_key_base):
    print("\n[1] 没配模型时不允许开会")
    code, specs = call(no_key_base, "/api/specs")
    sid_spec = (specs.get("specs") or [{}])[0].get("id")
    check(code == 200 and sid_spec, "示例规格存在（%s）" % sid_spec)
    code, r = call(no_key_base, "/api/specs/%s/discuss" % sid_spec, {"template": "default"})
    check(code == 400 and r.get("code") == "E-AGENT", "模拟模式下讨论被拒绝（E-AGENT）")
    check("模型" in (r.get("message") or ""), "拒绝原因指向配置模型")

    print("\n[2] 群体讨论：主持人调度 + 表决 + 达成共识")
    code, specs = call(base, "/api/specs")
    spec = (specs.get("specs") or [{}])[0]
    sid_spec = spec.get("id")
    check(code == 200 and sid_spec, "读到规格「%s」" % spec.get("name"))
    FakePlanner.calls.clear()
    FakePlanner.vote_calls.clear()
    FakePlanner.delay = 0.25          # 留出人工插话的窗口
    code, r = call(base, "/api/specs/%s/discuss" % sid_spec, {"template": "default"})
    check(code == 200 and r.get("ok"), "讨论已启动")
    sid = r.get("session_id")

    # 讨论进行到有人发言后，人工插一句话引导
    human_text = "先做登录，报表放到二期；不要引入第三方依赖"
    said = None
    end_at = time.time() + 30
    while time.time() < end_at:
        code, cur = call(base, "/api/plan/" + sid)
        minutes = ((cur.get("session") or {}).get("minutes") or [])
        if len(minutes) >= 2 and not said:
            code, said = call(base, "/api/plan/%s/say" % sid, {"text": human_text})
            check(code == 200 and said.get("ok"), "讨论中人工插话被接受")
            check(said.get("running") is True, "插话后讨论继续（不需要重开）")
        if ((cur.get("session") or {}).get("status") or "") not in ("running", "preparing"):
            break
        time.sleep(0.3)
    FakePlanner.delay = 0.0

    session = wait_plan(base, sid, timeout=90)
    check(session and session.get("status") == "done",
          "讨论结束（%s）" % ((session or {}).get("status"),))
    minutes = (session or {}).get("minutes") or []
    roles = [m.get("role") for m in minutes]
    speeches = [m for m in minutes if m.get("kind") != "human"]
    check(len(speeches) >= 5, "共 %d 条 AI 发言" % len(speeches))
    check(set(roles) >= {"pm", "architect", "coder", "tester", "reviewer"},
          "五个角色都发了言")
    check("human" in roles, "纪要里有「人工」这一条")
    human_line = next(m for m in minutes if m.get("kind") == "human")
    check(human_text[:10] in human_line.get("content", ""), "人工内容原样进了纪要")
    check(any("已按人工指令调整" in (m.get("content") or "") for m in speeches),
          "后续发言确实回应了人工指令（不是无视）")

    log = (session or {}).get("manager_log") or []
    check(len(log) >= 5, "主持人记录了 %d 次选人决定" % len(log))
    check(all(x.get("why") for x in log if x.get("label") != "主持人"),
          "每次选人都写了理由（不是固定轮转）")
    check(any("人工指令" in (x.get("why") or "") for x in log),
          "主持人把「回应人工」排进了议程")
    round1 = list(dict.fromkeys(m.get("role") for m in speeches if m.get("round") == 1))
    check(set(round1) == {"pm", "architect", "coder", "tester", "reviewer"},
          "第 1 轮五个人都发了言：%s" % " → ".join(round1))
    check(round1[0] != "pm",
          "首位发言者由规格关注点决定，不再固定是产品经理（%s 先说）" % round1[0])
    check(any(m.get("why") for m in speeches), "每条发言都带着主持人选他的理由")

    votes = (session or {}).get("votes") or []
    check(len(votes) >= 2, "表决了 %d 轮（首轮有人反对 → 再来一轮）" % len(votes))
    check(len(votes[0].get("votes") or []) == 5, "每轮每个角色都表态")
    check(any(v.get("stance") == "block" for v in votes[0]["votes"]),
          "首轮出现了「有阻塞」的表态")
    consensus = (session or {}).get("consensus") or {}
    check(consensus.get("status") == "reached",
          "两轮后达成共识（%s）" % consensus.get("status"))
    check(consensus.get("round") == 2, "共识在第 2 轮达成")
    check((session or {}).get("humans") and session["humans"][0].get("addressed") is True,
          "人工指令被标记为已回应")

    tasks = (session or {}).get("tasks") or []
    check(len(tasks) >= 5, "产出 %d 条任务" % len(tasks))
    check(all(t.get("what") and t.get("how") for t in tasks), "每条任务都有做什么/怎么做")
    check(all(t.get("steps") for t in tasks), "每条任务都有子步骤")
    check(all(t.get("kind") and t.get("out_schema") for t in tasks), "任务带执行器与产物 schema")
    things = {t.get("phase") for t in tasks}
    check({"设计", "编码", "测试"} <= things, "阶段覆盖设计/编码/测试")
    ids = {t["id"] for t in tasks}
    check(all(set(t.get("depends_on") or []) <= ids for t in tasks), "依赖只指向清单内的任务")
    check(any("循环" in f for f in (session.get("fixes") or [])), "环被代码断开并记录")
    check(any("角色" in f for f in (session.get("fixes") or [])), "非法角色被归一并记录")
    check(all(not any(f.startswith("/") for f in (t.get("files") or [])) for t in tasks),
          "绝对路径文件被过滤")
    check((session.get("usage") or {}).get("prompt_tokens", 0) > 0, "累计 token 用量")

    print("\n[3] 讨论结果 → 工作流")
    code, applied = call(base, "/api/plan/" + sid, {"template": "default"})
    check(code == 200 and applied.get("ok"), "应用讨论结果成功")
    stats = applied.get("stats") or {}
    check(stats.get("nodes", 0) >= len(tasks), "生成的图有 %s 个节点" % stats.get("nodes"))
    code, wf = call(base, "/api/workflow")
    check(code == 200 and wf.get("valid"), "编排出的图通过 DAG 校验")
    names = [n.get("name") for n in (wf.get("workflow") or {}).get("nodes", [])]
    check(any("登录" in (nm or "") for nm in names), "图上出现了讨论产出的任务名")

    print("\n[4] 讨论结束后人还能改写结论（续议）")
    before = len(session["minutes"])
    FakePlanner.delay = 0.15
    code, again = call(base, "/api/plan/%s/say" % sid,
                       {"text": "报表改到三期，并补一条回滚方案"})
    check(code == 200 and again.get("ok"), "结束后插话会触发续议")
    time.sleep(0.4)
    FakePlanner.delay = 0.0
    resumed = wait_plan(base, sid, timeout=90)
    check((resumed or {}).get("status") == "done", "续议后重新完成（%s）"
          % ((resumed or {}).get("status")))
    check(len((resumed or {}).get("minutes") or []) > before, "续议新增了发言")
    check(len((resumed or {}).get("humans") or []) >= 2, "人工介入被记录下来")
    check(bool((resumed or {}).get("tasks")), "续议后仍然有可用任务清单")


def blocked_checks():
    print("\n[5] 共识没达成：不允许悄悄生成，人要先拍板")
    FakePlanner.stance_mode = "block"
    try:
        proc, base = start_server(FAKE_URL)
        try:
            check(wait_health(base), "第二个实例已启动")
            code, specs = call(base, "/api/specs")
            sid_spec = (specs.get("specs") or [{}])[0].get("id")
            code, r = call(base, "/api/specs/%s/discuss" % sid_spec, {"template": "default"})
            sid = r.get("session_id")
            session = wait_plan(base, sid, timeout=120)
            check((session or {}).get("status") == "done", "讨论仍然跑完（不假装成功生成）")
            check(((session or {}).get("consensus") or {}).get("status") == "blocked",
                  "共识状态是 blocked")
            check((session or {}).get("consensus", {}).get("unresolved"),
                  "列出了未解决的分歧：%s"
                  % (((session or {}).get("consensus") or {}).get("unresolved") or [])[:1])
            code, applied = call(base, "/api/plan/" + sid, {})
            check(code == 409 and applied.get("code") == "E-CONSENSUS",
                  "直接生成被拒绝（E-CONSENSUS）")
            code, acc = call(base, "/api/plan/%s/decide" % sid, {"action": "accept",
                                                                "note": "风险已知，接受"})
            check(code == 200 and acc.get("ok"), "人工拍板「接受当前清单」成功")
            code, applied = call(base, "/api/plan/" + sid, {})
            check(code == 200 and applied.get("ok"), "拍板后可以生成工作流")
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
    finally:
        FakePlanner.stance_mode = "mixed"


def bad_model_checks():
    print("\n[6] 模型乱答时明确失败，不产出半张图")
    FakePlanner.mode = "garbage"
    try:
        proc, base = start_server(FAKE_URL)
        try:
            check(wait_health(base), "第三个实例已启动")
            code, specs = call(base, "/api/specs")
            sid_spec = (specs.get("specs") or [{}])[0].get("id")
            code, r = call(base, "/api/specs/%s/discuss" % sid_spec, {"template": "default"})
            check(code == 200, "讨论仍能启动")
            session = wait_plan(base, r.get("session_id"), timeout=120)
            check((session or {}).get("status") == "error", "状态是 error 而不是假装成功")
            check((session or {}).get("error", {}).get("code") == "E-FORMAT",
                  "错误码 E-FORMAT")
            code, applied = call(base, "/api/plan/" + r.get("session_id"), {})
            check(code == 409 and not applied.get("ok"), "拿这次会话去编排被拒绝（409）")
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
    finally:
        FakePlanner.mode = "ok"


def cancel_checks():
    print("\n[7] 讨论可以中止")
    FakePlanner.delay = 0.8
    try:
        proc, base = start_server(FAKE_URL)
        try:
            check(wait_health(base), "第四个实例已启动")
            code, specs = call(base, "/api/specs")
            sid_spec = (specs.get("specs") or [{}])[0].get("id")
            code, r = call(base, "/api/specs/%s/discuss" % sid_spec, {"template": "default"})
            sid = r.get("session_id")
            time.sleep(0.5)
            code, c = call(base, "/api/plan/%s/cancel" % sid, {})
            check(code == 200 and c.get("ok"), "取消请求被接受")
            check((c.get("session") or {}).get("status") == "cancelled", "会话标记为已取消")
            time.sleep(2.0)
            code, after = call(base, "/api/plan/" + sid)
            check(after["session"]["status"] == "cancelled", "取消后不会再变回运行中")
            code, applied = call(base, "/api/plan/" + sid, {})
            check(code == 409, "已取消的会话不能用来编排（409）")
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
    finally:
        FakePlanner.delay = 0.0


FAKE_URL = ""


def main():
    global FAKE_URL
    fake, FAKE_URL = start_fake()
    proc, base = start_server(FAKE_URL)
    nokey_proc, nokey_base = start_server(FAKE_URL, with_key=False)
    try:
        print("[环境] 假规划端点 %s" % FAKE_URL)
        check(wait_health(base), "被测服务已启动（已配置模型）")
        check(wait_health(nokey_base), "被测服务已启动（未配置模型）")
        unit_checks()
        flow_checks(base, nokey_base)
        blocked_checks()
        bad_model_checks()
        cancel_checks()
    finally:
        for p in (proc, nokey_proc):
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()
        fake.shutdown()

    total = len(PASS) + len(FAIL)
    print("\n结果: %s (%d/%d)" % ("PASS" if not FAIL else "FAIL", len(PASS), total))
    for f in FAIL:
        print("  失败: " + f)
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())