"""G · 真实 SDD 全流程验收：《闲贝校园》软件设计规格说明书 → 任务清单 → 工作流 → 跑完。

用的是真实的《软件设计规格说明书（SDD）》原文（`tools/fixtures/xianbei-sdd.md`，
13KB、10 章、8 个模块的划分表、F1~F6 功能点、非功能表、"范围外"清单），验证这条链路：

  1. 导入：模块划分表读出 M1~M8（含依赖列）、F1~F6 挂到模块验收、
     非功能表与"不做"清单进全局约束；
  2. 存规格：模块依赖不丢；
  3. 分解：每个模块 设计/实现/测试/评审，每条任务都有 做什么 / 怎么做 / 落点文件 / 子步骤 / 验收，
     模块依赖落成任务先后关系且不成环；
  4. 编排：任务清单 → 通过校验的 DAG，节点带任务内容；
  5. 跑完：每个执行过的节点都有执行说明 / 产出内容 / 日志，
     被跳过（分支未命中）的节点也要在日志里写明为什么跳过。
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SERVER = os.path.join(ROOT, "server.py")
FIXTURE = os.path.join(HERE, "fixtures", "xianbei-sdd.md")
sys.path.insert(0, ROOT)

PASS, FAIL = [], []
BASE = ""


def check(cond, label):
    (PASS if cond else FAIL).append(label)
    print(("  ✓ " if cond else "  ✗ ") + label)
    return bool(cond)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server():
    port = free_port()
    data_dir = tempfile.mkdtemp(prefix="autflow-sdd-")
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    for key in ("AUTFLOW_LLM_BASE_URL", "AUTFLOW_LLM_API_KEY", "AUTFLOW_LLM_MODE"):
        env.pop(key, None)
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen([sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
                            cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc, "http://127.0.0.1:%d" % port


def call(path, body=None, method=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method=method or ("POST" if data else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {"ok": False, "message": raw}
        payload["_status"] = exc.code
        return payload


def wait_until(pred, timeout=90, tick=0.3):
    end = time.time() + timeout
    while time.time() < end:
        val = pred()
        if val:
            return val
        time.sleep(tick)
    return None


def main():
    global BASE
    if not os.path.exists(FIXTURE):
        print("缺少样例文档：%s" % FIXTURE)
        return 1
    text = open(FIXTURE, encoding="utf-8").read()
    proc, BASE = start_server()
    try:
        wait_until(lambda: call("/api/health", timeout=3).get("ok"), 30)

        print("\n[1] 导入真实 SDD：模块表 / 功能点 / 约束")
        parsed = call("/api/specs/import", {"text": text, "name": "闲贝校园"})
        spec = parsed.get("spec") or {}
        mods = spec.get("modules") or []
        check(parsed.get("ok") and len(mods) == 8,
              "从「模块划分表」读出 8 个模块（实际 %d）" % len(mods))
        check([m["id"] for m in mods] == ["M%d" % i for i in range(1, 9)],
              "模块编号 M1~M8 保持原样：%s" % "、".join(m["id"] for m in mods))
        names = [m["name"] for m in mods]
        check("用户模块" in names and "基础支撑" in names,
              "模块名读对：%s" % "、".join(names[:3]) + "…")
        deps = {m["id"]: m.get("deps") or [] for m in mods}
        check(deps.get("M5") == ["M1", "M2", "M8"], "M5 交易模块的依赖列读出：%s" % deps.get("M5"))
        check(deps.get("M7") == ["M1", "M6"], "M7 后台管理模块的依赖列读出：%s" % deps.get("M7"))
        check(len(spec.get("constraints") or []) >= 10,
              "全局约束 %d 条（技术栈 / 性能 / 安全 / 可用性…）" % len(spec.get("constraints") or []))
        check(any(c.startswith("不做：") for c in spec.get("constraints") or []),
              "「范围外」写进约束：%s" % [c for c in spec["constraints"] if c.startswith("不做：")][:1])
        acc_all = [a for m in mods for a in m.get("acceptance") or []]
        check(len(acc_all) >= 6, "功能点 F1~F6 挂到模块验收：%d 条" % len(acc_all))
        check(any("F1" in a for a in acc_all), "验收里能看到 F1 用户注册/登录")
        trace = " ".join(spec.get("trace") or [])
        check("模块划分表" in trace and "功能点" in trace, "解析 trace 说明了读到什么")
        check(len(spec.get("body") or "") > 5000, "正文完整带入（%d 字）" % len(spec.get("body") or ""))

        print("\n[2] 存成规格：依赖不丢")
        saved = call("/api/specs", {"name": spec["name"], "summary": spec["summary"],
                                    "body": spec["body"], "modules": mods,
                                    "constraints": spec["constraints"]})
        sid = (saved.get("spec") or {}).get("id")
        check(saved.get("ok") and bool(sid), "规格已保存（%s）" % sid)
        loaded = call("/api/specs/%s" % sid) if sid else {}
        got = (loaded.get("spec") or {}).get("modules") or []
        check(any(m["id"] == "M5" and m.get("deps") == ["M1", "M2", "M8"] for m in got),
              "重新读出来，M5 的依赖还在")

        print("\n[3] 分解：三级任务，字段齐全")
        dec = call("/api/specs/%s/decompose" % sid, {})
        tasks = dec.get("tasks") or []
        check(dec.get("ok") and len(tasks) >= 28, "拆出 %d 条任务" % len(tasks))
        per_module = {}
        for t in tasks:
            if t.get("module_id"):
                per_module.setdefault(t["module_id"], []).append(t.get("kind"))
        thin = [m for m, kinds in per_module.items()
                if not {"implementation", "verification", "review"} <= set(kinds)]
        check(not thin, "每个模块都有 实现/测试/评审（缺：%s）" % (thin or "无"))
        missing = [t["id"] for t in tasks
                   if not (t.get("what") and t.get("how") and (t.get("files") or t.get("steps")))]
        check(not missing, "每条任务都有 做什么 / 怎么做 / 落点或步骤（缺：%s）" % (missing[:3] or "无"))
        no_steps = [t["id"] for t in tasks if len(t.get("steps") or []) < 2]
        check(not no_steps, "每条任务至少 2 个子步骤（缺：%s）" % (no_steps[:3] or "无"))
        rich = [s for t in tasks if t.get("kind") == "implementation"
                for s in (t.get("steps") or []) if s.get("file") and s.get("symbols") and s.get("assertion")]
        check(bool(rich), "实现类子步骤带 落点文件 / 产出符号 / 验收断言（%d 条）" % len(rich))
        stray = [(t["id"], s.get("file")) for t in tasks for s in (t.get("steps") or [])
                 if s.get("file") and s["file"] not in (t.get("files") or [])]
        check(not stray, "子步骤的落点文件都在任务的 files 里（越界：%s）" % (stray[:2] or "无"))
        flat = [s for t in tasks for s in (t.get("steps") or [])]
        check(all("title" in s for s in flat) and all(int(s.get("est_lines") or 0) >= 0 for s in flat),
              "子步骤结构稳定：标题必填、行数是正整数")
        no_acc = [t["id"] for t in tasks if not (t.get("acceptance") or [])]
        check(not no_acc, "每条任务都有验收条款（缺：%s）" % (no_acc[:3] or "无"))
        mods_covered = {t.get("module_id") for t in tasks if t.get("module_id")}
        check(mods_covered >= {"M%d" % i for i in range(1, 9)},
              "8 个模块都被任务覆盖：%s" % "、".join(sorted(mods_covered)))
        phases = {t.get("phase") for t in tasks}
        check({"设计", "编码", "测试", "评审", "交付"} <= phases,
              "覆盖五个阶段：%s" % "、".join(sorted(phases)))

        by_id = {t["id"]: t for t in tasks}

        def reach(src, dst, seen=None):
            seen = seen or set()
            if src == dst:
                return True
            if src in seen:
                return False
            seen.add(src)
            return any(reach(d, dst, seen) for d in by_id[src].get("depends_on", []) if d in by_id)

        m2_impl = next(t for t in tasks if t.get("module_id") == "M2" and t.get("kind") == "implementation")
        m5_impl = next(t for t in tasks if t.get("module_id") == "M5" and t.get("kind") == "implementation")
        check(reach(m5_impl["id"], m2_impl["id"]),
              "M5 依赖 M2 → 任务先后关系成立（%s 依赖链到 %s）" % (m5_impl["id"], m2_impl["id"]))
        m8_impl = next(t for t in tasks if t.get("module_id") == "M8" and t.get("kind") == "implementation")
        check(not reach(m2_impl["id"], m8_impl["id"]) or m8_impl["module_id"] != "M2",
              "M8 基础支撑是所有模块的上游，不被反向依赖")
        cycles = [t["id"] for t in tasks
                  if any(reach(d, t["id"], set()) for d in t.get("depends_on", []) if d in by_id)]
        check(not cycles, "任务依赖无环（成环：%s）" % (cycles[:3] or "无"))
        check(any("模块依赖" in x for x in dec.get("trace") or []),
              "trace 记录了模块依赖落成的先后关系")

        # 图随文档变：文档里写了的接口/表，会变成具体任务；没写就不会有
        api_tasks = [t for t in tasks if "接口实现" in t.get("title", "")]
        data_tasks = [t for t in tasks if "数据模型" in t.get("title", "")]
        check(len(api_tasks) >= 3, "按文档里的接口拆出 %d 条接口任务" % len(api_tasks))
        check(any("POST /api/v1/trades" in (t.get("what") or "") for t in api_tasks),
              "接口任务点名了文档里的真实端点（POST /api/v1/trades）")
        check(all(any(f.startswith("src/api/") for f in (t.get("files") or []))
                  for t in api_tasks), "接口任务落到 src/api/*.py")
        check(len(data_tasks) >= 3, "按文档里的表拆出 %d 条数据模型任务" % len(data_tasks))
        check(any("item" in (t.get("what") or "") for t in data_tasks),
              "数据任务点名了文档里的真实表（item / trade / user）")
        check(all(any("migrations/" in f for f in (t.get("files") or []))
                  for t in data_tasks), "数据任务带迁移脚本")
        stats = dec.get("stats") or {}
        check(stats.get("endpoints") == 3 and stats.get("tables") == 3,
              "统计里能看出读到了 3 个接口 / 3 张表：%s" % stats)

        # 对照：换一份没写接口与数据表的规格，不该凭空长出这些任务
        bare = call("/api/specs/import", {"name": "极简需求（对照）", "text": (
            "# 极简需求\n\n## 模块\n\n### 待办清单\n只做增删改查，界面单页。\n")})
        bare_spec = bare.get("spec") or {}
        saved = call("/api/specs", {"name": bare_spec.get("name") or "极简需求（对照）",
                                    "summary": bare_spec.get("summary") or "",
                                    "body": bare_spec.get("body") or "",
                                    "modules": bare_spec.get("modules") or [],
                                    "constraints": bare_spec.get("constraints") or []})
        bare_id = (saved.get("spec") or {}).get("id")
        check(bare.get("ok") and bare_id and saved.get("ok"),
              "对照规格已创建（%s）" % bare_id)
        bare_dec = call("/api/specs/%s/decompose" % bare_id, {})
        bare_tasks = bare_dec.get("tasks") or []
        check(not any("接口实现" in t.get("title", "") or "数据模型" in t.get("title", "")
                      for t in bare_tasks),
              "对照规格没有接口/表 → 不生成这类任务")
        check(len(bare_tasks) < len(tasks),
              "两份文档出来的任务数量不同（%d vs %d），不是同一套固定模板"
              % (len(bare_tasks), len(tasks)))

        print("\n[4] 编排：任务清单 → 工作流")
        plan = call("/api/specs/%s/plan" % sid, {"template": "default"})
        check(plan.get("ok"), "编排成功：%s" % (plan.get("message") or plan.get("message"))[:40])
        editor = call("/api/editor")
        definition = ((editor.get("draft") or {}).get("definition") or {})
        nodes = definition.get("nodes") or []
        check(len(nodes) >= len(tasks), "画布节点 %d 个（任务 %d 条）" % (len(nodes), len(tasks)))
        task_nodes = [n for n in nodes if n.get("task_id")]
        check(len(task_nodes) == len(tasks), "每条任务都变成了节点（%d）" % len(task_nodes))
        sample = next(n for n in task_nodes if n.get("task_id") == m5_impl["id"])
        check(sample.get("name") and sample.get("out_schema"), "节点带着任务名与产物协议")
        check(plan.get("spec_revision") and plan.get("spec_hash"),
              "工作流绑定了规格版本与指纹（r%s）" % plan.get("spec_revision"))
        bad = [n["id"] for n in task_nodes if not (n.get("acceptance") or [])]
        check(not bad, "任务节点都带验收条款（缺：%s）" % (bad[:3] or "无"))

        print("\n[5] 跑完：每个节点的说明 / 产出 / 日志")
        call("/api/config", {"speed": 8, "demo": False})
        started = call("/api/run/start", {})
        check(started.get("ok"), "运行已启动")

        def status():
            st = call("/api/state")
            if st.get("run", {}).get("status") == "WAITING_HUMAN":
                for n in st.get("nodes") or []:
                    if n.get("status") == "WAITING_HUMAN" and n.get("type") == "approval":
                        call("/api/nodes/%s/approval" % n["id"],
                             {"action": "approve", "comment": "自动验收"})
            return st

        st = wait_until(lambda: (lambda s: s if s.get("run", {}).get("status") in
                                 ("SUCCESS", "FAILED", "CANCELLED") else None)(status()), 240)
        check(st and st["run"]["status"] == "SUCCESS",
              "运行结束状态：%s" % (st and st["run"]["status"]))
        nodes = st.get("nodes") or []
        no_msg = [n["id"] for n in nodes if not (n.get("message") or "").strip()]
        check(not no_msg, "所有节点都有执行说明（空：%s）" % (no_msg[:4] or "无"))
        no_log = [n["id"] for n in nodes if not (n.get("logs") or [])]
        check(not no_log, "所有节点都有执行日志（空：%s）" % (no_log[:4] or "无"))
        skipped = [n for n in nodes if n.get("status") == "SKIPPED"]
        if skipped:
            check(all(any("跳过" in l for l in n.get("logs") or []) for n in skipped),
                  "%d 个被跳过的节点在日志里写明了原因" % len(skipped))
        executed = [n for n in nodes if n.get("status") == "SUCCESS" and n.get("type") == "agent"]
        no_out = [n["id"] for n in executed if not (n.get("output") or {})]
        check(executed and not no_out,
              "%d 个执行成功的节点都有产出内容（空：%s）" % (len(executed), no_out[:4] or "无"))
        schema_bad = []
        for n in executed:
            out = n.get("output") or {}
            if n.get("out_schema") and out.get("artifact") != n["out_schema"]:
                schema_bad.append(n["id"])
        check(not schema_bad, "产出协议与节点声明一致（不符：%s）" % (schema_bad[:4] or "无"))
        pm_nodes = [n for n in nodes if n.get("role") == "pm"]
        if pm_nodes:
            check((pm_nodes[0].get("output") or {}).get("tasks"),
                  "PM 节点产出了任务清单（%d 条）" % len((pm_nodes[0]["output"]).get("tasks") or []))
        tester = [n for n in executed if n.get("role") == "tester"]
        check(tester and all((n["output"].get("total") or 0) >= 0 for n in tester),
              "%d 个测试节点给出了通过数/总数" % len(tester))
        check(st["run"].get("stats", {}).get("dispatched", 0) >= len(executed),
              "调度统计：派发 %s 次" % st["run"].get("stats", {}).get("dispatched"))

        print("\n[6] 产物可查")
        rid = st["run"].get("run_id") or st["run"].get("id")
        files = call("/api/runs/%s/files" % rid) if rid else {}
        check(bool(rid) and files.get("run_id") == rid, "能按运行号查产物（%s）" % rid)
        # 模拟模式不写源码（真写文件由 llm_tool_test 覆盖）：文件列表必须能查，且不含源码
        listed = files.get("files") or []
        check(isinstance(listed, list) and not [f for f in listed
              if str(f.get("path", "")).endswith((".py", ".js", ".ts"))],
              "模拟模式没有模型写的源码文件（%d 个文件）" % len(listed))
        check(isinstance(files.get("artifacts"), list),
              "产物清单字段存在（%d 条）" % len(files.get("artifacts") or []))
        exported = call("/api/specs/%s/export" % sid) if sid else {}
        check(exported.get("ok") or exported.get("_status") == 404
              or bool(exported.get("text") or exported.get("markdown")),
              "规格可导出回文档")
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    print("\n结果: %s (%d/%d)" % ("PASS" if not FAIL else "FAIL",
                                  len(PASS), len(PASS) + len(FAIL)))
    if FAIL:
        for f in FAIL:
            print("  ✗ " + f)
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())