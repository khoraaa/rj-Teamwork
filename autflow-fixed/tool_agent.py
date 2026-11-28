"""真实执行器：让模型在**受限工作区**里写代码、读文件、跑命令。

这不是"让模型输出一段 JSON 就当完成"，而是：
  1. 每次运行有一个独立工作区目录（`<数据目录>/runs/<run_id>/workspace`）；
  2. 模型通过工具调用真的落盘、真的跑测试命令；
  3. 所有路径都做越界检查，命令有白名单、超时、输出截断，环境变量不泄露密钥；
  4. 结束时按工作区实际内容统计产物（文件、行数、sha256、测试结果），而不是模型自报。

本地模拟模式（mode=mock）完全不碰这里，保证无密钥时全流程与旧版一致。
"""
import hashlib
import json
import os
import re
import importlib.util
import shlex
import shutil
import subprocess
import sys
import time

# 直接指向当前解释器：Windows 上 `python` 常被 Store 别名挡住（exit 9009）
PYTHON = ('"%s"' % sys.executable) if " " in sys.executable else sys.executable

MAX_FILE_BYTES = 400 * 1024
MAX_RUN_BYTES = 24 * 1024 * 1024
MAX_OUTPUT = 8000

# 允许执行的命令首词（只放开发/测试常用工具，运维类命令一律不给）
ALLOWED_PROGRAMS = {
    "python", "python3", "py", "pip", "pytest", "node", "npm", "npx", "tsc",
    "eslint", "jest", "vitest", "vite", "go", "cargo", "rustc", "mvn", "java",
    "javac", "dotnet", "make", "cmake", "gcc", "g++", "git", "echo", "dir", "type",
}
SHELL_META = ("&&", "||", "|", ";", ">", "<", "`", "$(", "${", "\n", "\r")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


class Workspace:
    """一次运行的隔离工作区：所有读写都限制在 root 之内。"""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.created = time.time()

    def ensure(self):
        os.makedirs(self.root, exist_ok=True)
        return self

    # ---------------- 路径安全 ----------------
    def safe_path(self, rel):
        if not isinstance(rel, str) or not rel.strip():
            raise ValueError("路径不能为空")
        rel = rel.strip().replace("\\", "/")
        if "\x00" in rel:
            raise ValueError("路径含非法字符")
        if re.match(r"^[a-zA-Z]:", rel) or rel.startswith("/"):
            raise ValueError("只接受工作区内的相对路径：%s" % rel)
        parts = [p for p in rel.split("/") if p not in ("", ".")]
        if any(p == ".." for p in parts):
            raise ValueError("不允许跳出工作区：%s" % rel)
        if parts and parts[0] == ".git":
            raise ValueError("不允许直接改 .git")
        target = os.path.abspath(os.path.join(self.root, *parts))
        if target != self.root and not target.startswith(self.root + os.sep):
            raise ValueError("越界路径：%s" % rel)
        return target, "/".join(parts)

    # ---------------- 文件操作 ----------------
    def write(self, rel, content):
        target, norm = self.safe_path(rel)
        data = (content or "").encode("utf-8")
        if len(data) > MAX_FILE_BYTES:
            raise ValueError("单文件超过 %dKB 上限" % (MAX_FILE_BYTES // 1024))
        if self.total_bytes() + len(data) > MAX_RUN_BYTES:
            raise ValueError("本次运行写入总量超过 %dMB 上限" % (MAX_RUN_BYTES // 1024 // 1024))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as f:
            f.write(data)
        return {"path": norm, "bytes": len(data), "sha256": sha256_bytes(data)[:12]}

    def read(self, rel, limit=MAX_FILE_BYTES):
        target, norm = self.safe_path(rel)
        with open(target, "rb") as f:
            data = f.read(limit + 1)
        return {"path": norm, "content": data[:limit].decode("utf-8", "replace"),
                "truncated": len(data) > limit}

    def list(self, base="", depth=2):
        target, norm = self.safe_path(base or ".")
        out = []
        if not os.path.isdir(target):
            return out
        for dirpath, dirnames, filenames in os.walk(target):
            dirnames[:] = [d for d in dirnames if d not in (".git", "__pycache__", "node_modules", ".venv")]
            rel_dir = os.path.relpath(dirpath, self.root).replace("\\", "/")
            if rel_dir.count("/") >= depth:
                dirnames[:] = []
            for name in sorted(filenames):
                full = os.path.join(dirpath, name)
                rel_file = os.path.relpath(full, self.root).replace("\\", "/")
                try:
                    size = os.path.getsize(full)
                except OSError:
                    continue
                out.append({"path": rel_file, "bytes": size})
        return out[:400]

    def total_bytes(self):
        total = 0
        for item in self.list("", depth=8):
            total += item["bytes"]
        return total

    def snapshot(self):
        """{path: {bytes, sha256}}，用于算出"这次真正改了哪些文件"。"""
        snap = {}
        for item in self.list("", depth=8):
            try:
                with open(os.path.join(self.root, item["path"]), "rb") as f:
                    snap[item["path"]] = {"bytes": item["bytes"],
                                          "sha256": sha256_bytes(f.read())[:16]}
            except OSError:
                continue
        return snap

    # ---------------- 命令执行 ----------------
    def run(self, command, timeout=120):
        cmd = (command or "").strip()
        if not cmd:
            raise ValueError("命令为空")
        if len(cmd) > 400:
            raise ValueError("命令过长")
        for meta in SHELL_META:
            if meta in cmd:
                raise ValueError("命令里不允许出现 shell 连接符（%s），一次只跑一条" % meta)
        argv = shlex.split(cmd, posix=False)
        if not argv:
            raise ValueError("命令为空")
        prog = os.path.basename(argv[0]).strip('"\'').lower()
        prog = re.sub(r"\.(exe|cmd|bat)$", "", prog)
        python_versioned = re.fullmatch(r"python\d+(?:\.\d+)?", prog) is not None
        if prog not in ALLOWED_PROGRAMS and not python_versioned:
            raise ValueError("命令 %r 不在白名单内（可用：%s）"
                             % (prog, "、".join(sorted(ALLOWED_PROGRAMS))[:200]))
        # 环境变量：放行常规变量（缺 SystemDrive/ProgramData 这类变量会让被测程序
        # 生成 "%SystemDrive%\..." 这种字面量目录），但绝不把密钥传给被测程序
        secret_re = re.compile(r"(?i)(api[_-]?key|token|secret|password|passwd|"
                               r"credential|authorization)")
        env = {k: v for k, v in os.environ.items()
               if not secret_re.search(k) and not k.upper().startswith("AUTFLOW_LLM")}
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        started = time.time()
        try:
            proc = subprocess.run(cmd, cwd=self.root, env=env, timeout=timeout,
                                  shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
            out = (proc.stdout or b"").decode("utf-8", "replace")
            code = proc.returncode
        except subprocess.TimeoutExpired as exc:
            out = ((exc.stdout or b"").decode("utf-8", "replace")
                   if isinstance(exc.stdout, bytes) else (exc.stdout or ""))
            return {"ok": False, "code": -9, "ms": int((time.time() - started) * 1000),
                    "output": out[-MAX_OUTPUT:], "timeout": True}
        return {"ok": code == 0, "code": code,
                "ms": int((time.time() - started) * 1000),
                "output": out[-MAX_OUTPUT:], "timeout": False}


# --------------------------------------------------------------------------
# 测试结果解析：不同语言的测试框架，输出格式各异，但都只需要 passed/total
# --------------------------------------------------------------------------
def parse_test_output(text):
    text = text or ""
    passed = total = 0
    found = False
    for pat in (r"(\d+) passed[^\d]*(\d+) failed", r"(\d+) failed[^\d]*(\d+) passed"):
        m = re.search(pat, text)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            failed = a if "failed" in m.group(0)[:6] else b
            passed = b if failed == a else a
            total = passed + failed
            found = True
            break
    if not found:
        m = re.search(r"(\d+) passed", text)
        if m:
            passed = total = int(m.group(1))
            found = True
    if not found:
        m = re.search(r"Tests:\s*(\d+) passed,\s*(\d+) total", text)
        if m:
            passed, total = int(m.group(1)), int(m.group(2))
            found = True
    if not found:
        m = re.search(r"Ran (\d+) tests?", text)      # 标准库 unittest
        if m:
            total = int(m.group(1))
            passed = total
            fail = re.search(r"FAILED \(([^)]*)\)", text)
            if fail:
                bad = sum(int(x) for x in re.findall(r"(?:failures|errors)=(\d+)",
                                                     fail.group(1)))
                passed = max(0, total - bad)
            found = True
    if not found:
        m = re.search(r"# pass (\d+)", text)
        if m:
            passed = int(m.group(1))
            m2 = re.search(r"# fail (\d+)", text)
            total = passed + (int(m2.group(1)) if m2 else 0)
            found = True
    if not found:
        m = re.search(r"(\d+)\s+passing", text)
        if m:
            passed = int(m.group(1))
            m2 = re.search(r"(\d+)\s+failing", text)
            total = passed + (int(m2.group(1)) if m2 else 0)
            found = True
    return {"passed": passed, "total": total, "found": found}


def has_pytest():
    try:
        return importlib.util.find_spec("pytest") is not None
    except Exception:
        return False


PYTEST_CMD = PYTHON + " -m pytest -q"
# 没装 pytest 时退回标准库 unittest：宁可少个框架，也不能让"测试阶段"跑不起来
UNITTEST_CMD = PYTHON + " -m unittest discover -s tests"


def detect_test_command(ws):
    """按工作区内容推断测试命令：pytest → 标准库 unittest → npm test / node。"""
    names = {item["path"].split("/")[-1] for item in ws.list("", depth=1)}
    has_tests_dir = os.path.isdir(os.path.join(ws.root, "tests"))
    py_cmd = PYTEST_CMD if has_pytest() else UNITTEST_CMD
    if {"pytest.ini", "conftest.py", "pyproject.toml", "setup.cfg", "tox.ini"} & names:
        return py_cmd
    if has_tests_dir:
        return py_cmd
    if "package.json" in names:
        try:
            with open(os.path.join(ws.root, "package.json"), encoding="utf-8") as f:
                pkg = json.load(f)
            if (pkg.get("scripts") or {}).get("test"):
                return "npm test --silent"
        except Exception:
            pass
    pys = [p for p in (item["path"] for item in ws.list("", depth=3)) if p.endswith(".py")]
    if pys:
        return py_cmd
    return ""


# --------------------------------------------------------------------------
# 工具定义：OpenAI 与 Anthropic 两种协议共用一份 schema
# --------------------------------------------------------------------------
TOOLS = [
    {"name": "list_files",
     "description": "列出工作区里的文件（相对路径 + 字节数）。开始干活前先看一眼现状。",
     "parameters": {"type": "object", "properties": {
         "path": {"type": "string", "description": "相对目录，默认项目根"}}, "required": []}},
    {"name": "read_file",
     "description": "读取工作区里某个文本文件的内容。",
     "parameters": {"type": "object", "properties": {
         "path": {"type": "string", "description": "相对路径"}},
         "required": ["path"]}},
    {"name": "write_file",
     "description": "把完整内容写入工作区文件（覆盖写）。必须写可运行的完整代码，不要留 TODO 或省略号。",
     "parameters": {"type": "object", "properties": {
         "path": {"type": "string", "description": "相对路径，如 src/app.py"},
         "content": {"type": "string", "description": "文件完整内容"}},
         "required": ["path", "content"]}},
    {"name": "run_command",
     "description": "在工作区里执行一条命令（只允许 pytest/python/node/npm/git 等开发命令），用于跑测试或自检。",
     "parameters": {"type": "object", "properties": {
         "command": {"type": "string", "description": "完整命令行，例如 python -m pytest -q"}},
         "required": ["command"]}},
    {"name": "finish",
     "description": "任务完成时调用。files 填你写/改过的相对路径，summary 一句话说清做了什么。",
     "parameters": {"type": "object", "properties": {
         "summary": {"type": "string"},
         "files": {"type": "array", "items": {"type": "string"}},
         "notes": {"type": "string", "description": "遗留风险或后续建议"}},
         "required": ["summary"]}},
]


def _as_json_obj(text):
    """模型可能直接把产物 JSON 当正文返回；能解析就解析，不能就算了。"""
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(text[start:end + 1])
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


# 轻量安全扫描：只做能在几秒内跑完、且结论可解释的静态检查
SECURITY_PATTERNS = (
    (r"\beval\s*\(", "high", "使用 eval 执行动态代码"),
    (r"\bexec\s*\(", "high", "使用 exec 执行动态代码"),
    (r"\bos\.system\s*\(", "high", "直接调用 shell"),
    (r"subprocess\.[a-z_]+\([^)]*shell\s*=\s*True", "medium", "subprocess 使用 shell=True"),
    (r"(?i)(password|passwd|secret|token|api_key)\s*=\s*['\"][^'\"]{4,}['\"]", "high", "疑似硬编码凭据"),
    (r"(?i)verify\s*=\s*False", "high", "关闭 TLS 证书校验"),
    (r"\bpickle\.loads\s*\(", "medium", "反序列化不可信数据"),
    (r"(?i)\b(TODO|FIXME|XXX)\b", "low", "存在未完成标记"),
)
SCAN_EXT = (".py", ".js", ".ts", ".tsx", ".html", ".css", ".json", ".sh", ".yml", ".yaml")


def scan_workspace(ws, limit=60):
    findings = []
    for item in ws.list("", depth=6):
        if not item["path"].endswith(SCAN_EXT):
            continue
        try:
            text = ws.read(item["path"], limit=200000)["content"]
        except Exception:
            continue
        for pattern, level, label in SECURITY_PATTERNS:
            for m in re.finditer(pattern, text):
                line = text.count("\n", 0, m.start()) + 1
                findings.append({"file": item["path"], "line": line,
                                 "level": level, "issue": label,
                                 "snippet": text.splitlines()[line - 1][:120]
                                 if line - 1 < len(text.splitlines()) else ""})
                break                      # 同一文件同一类问题只报一次
        if len(findings) >= limit:
            break
    return findings[:limit]


def security_score(findings):
    weight = {"high": 25, "medium": 10, "low": 3}
    penalty = sum(weight.get(f["level"], 5) for f in findings)
    return max(0, 100 - penalty)


class ToolAgent:
    """带工具的编码执行器。run() 返回 {ok, output, error}，由编排器转成节点状态。"""

    name = "tool"

    def __init__(self, client, settings, workspace, log=None, ai_id=None):
        self.client = client
        self.settings = settings
        self.ws = workspace
        self.log = log or (lambda *_: None)
        # 这个节点指定的 AI（为空表示用默认端点）；调用时透传给 LLMClient
        self.ai_id = ai_id or None

    # ---------------- 工具分发 ----------------
    def _exec_tool(self, name, args):
        try:
            if name == "list_files":
                return {"ok": True, "files": self.ws.list(args.get("path") or "")}
            if name == "read_file":
                return {"ok": True, **self.ws.read(args.get("path") or "")}
            if name == "write_file":
                info = self.ws.write(args.get("path") or "", args.get("content") or "")
                self.log("写入 %s（%d 字节）" % (info["path"], info["bytes"]))
                return {"ok": True, **info}
            if name == "run_command":
                res = self.ws.run(args.get("command") or "",
                                  timeout=int(self.settings.get("timeout") or 180))
                self.log("执行 %s → exit %s" % (args.get("command"), res["code"]))
                return {"ok": True, **res}
            if name == "finish":
                return {"ok": True, "done": True, **args}
            return {"ok": False, "error": "未知工具 %s" % name}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    # ---------------- 提示词 ----------------
    def _system_prompt(self, node):
        return "\n".join([
            "你是一名资深工程师，在隔离工作区里完成一个具体开发任务。",
            "规则：",
            "1. 先 list_files 看清现状，再动手；新项目就从零建目录结构。",
            "2. write_file 必须写**完整可运行**的代码：不要省略号、不要 TODO、不要伪代码。",
            "3. 写完用 run_command 跑一次测试或自检（python -m pytest -q / npm test / node 脚本）。",
            "4. 测试失败要自己改，最多迭代 %s 步。" % (self.settings.get("max_steps") or 14),
            "5. 结束时调用 finish，files 里列出真正写过的文件。",
            "6. 只用标准库或已声明的依赖，不要联网下载。",
            "任务类型：%s；节点：%s" % (node.get("kind") or "implementation",
                                        node.get("name") or ""),
        ])

    def _user_prompt(self, node, ctx):
        task = ctx.get("task") or {}
        spec = ctx.get("spec") or {}
        lines = [
            "## 规格",
            "名称：%s" % (spec.get("name") or ""),
            "摘要：%s" % (spec.get("summary") or ""),
        ]
        modules = spec.get("modules") or []
        if modules:
            lines.append("模块：")
            for m in modules[:8]:
                lines.append("- %s（%s）：%s" % (m.get("name"), m.get("priority") or "P1",
                                                (m.get("goal") or "")[:120]))
        if spec.get("constraints"):
            lines.append("全局约束：" + "；".join(str(c) for c in spec["constraints"][:6]))
        lines += ["", "## 本次任务", "标题：%s" % (task.get("title") or node.get("name")),
                  "做什么：%s" % (task.get("what") or ""),
                  "怎么做：%s" % (task.get("how") or ""),
                  "优先级：%s，预估 %s 人日" % (task.get("priority") or "P1",
                                              task.get("estimate") or 3)]
        if task.get("files"):
            lines.append("目标文件：" + "、".join(task["files"]))
        if task.get("acceptance"):
            lines.append("验收标准：")
            lines.extend("- " + str(a) for a in task["acceptance"][:8])
        upstream = ctx.get("upstream") or []
        if upstream:
            lines.append("")
            lines.append("## 上游交付物摘要")
            for item in upstream[:4]:
                lines.append("- %s：%s" % (item.get("name"), (item.get("summary") or "")[:200]))
        lines.append("")
        lines.append("## 工作区现状")
        files = self.ws.list("", depth=3)
        if files:
            lines.extend("- %s (%d B)" % (f["path"], f["bytes"]) for f in files[:40])
        else:
            lines.append("（空工作区，需要你从零创建）")
        return "\n".join(lines)

    # ---------------- 主流程 ----------------
    def run(self, node, ctx):
        started = time.time()
        before = self.ws.snapshot()
        self.log("开始执行（模型 %s）" % self.settings.get("model"))
        messages = [{"role": "system", "content": self._system_prompt(node)},
                    {"role": "user", "content": self._user_prompt(node, ctx)}]
        usage = {"in": 0, "out": 0}
        steps = 0
        finished = None
        max_steps = int(self.settings.get("max_steps") or 14)
        transcript = []
        # 节点自带 timeout 时以它为准：超时要能被引擎归类成 E-TIMEOUT 并触发重试
        timeout_s = node.get("timeout") or ctx.get("timeout") or self.settings.get("timeout") or 180
        timeout_s = max(1, min(int(timeout_s), 600))

        while steps < max_steps:
            steps += 1
            try:
                res = self.client.chat(messages, tools=TOOLS, timeout=timeout_s,
                               ai_id=getattr(self, "ai_id", None))
            except Exception as exc:                      # LLMError 由上层归类
                code = getattr(exc, "code", "E-AGENT")
                return {"ok": False, "error": {"code": code, "reason": str(exc)},
                        "output": {"artifact": node.get("out_schema") or "code-result-v1",
                                   "steps": steps}}
            usage["in"] += res["usage"].get("in", 0)
            usage["out"] += res["usage"].get("out", 0)
            content = res.get("content") or ""
            calls = res.get("tool_calls") or []
            if content:
                transcript.append(content[:600])

            if not calls:
                # 没有工具调用：模型直接给了结论，视作 finish
                finished = {"summary": content.strip()[:800] or "（模型未给总结）", "files": []}
                break

            # 按协议无关格式回填：assistant 带 tool_calls，随后每个结果一条 tool 消息
            messages.append({"role": "assistant", "content": content or "",
                             "tool_calls": [{"id": c.get("id") or "call_%d" % i,
                                             "name": c.get("name"),
                                             "arguments": c.get("arguments") or {}}
                                            for i, c in enumerate(calls)]})
            for i, call in enumerate(calls):
                name = call.get("name")
                args = call.get("arguments") or {}
                result = self._exec_tool(name, args)
                if result.get("done"):
                    finished = {"summary": result.get("summary") or "",
                                "files": result.get("files") or [],
                                "notes": result.get("notes") or ""}
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id") or "call_%d" % i,
                    "name": name,
                    "content": json.dumps(result, ensure_ascii=False)[:6000]})
            if finished:
                break

        after = self.ws.snapshot()
        changed = sorted(p for p, meta in after.items()
                         if before.get(p, {}).get("sha256") != meta["sha256"])
        loc = 0
        for path in changed:
            try:
                with open(os.path.join(self.ws.root, path), encoding="utf-8",
                          errors="replace") as f:
                    loc += sum(1 for _ in f)
            except OSError:
                continue
        digest = sha256_bytes("".join(after[p]["sha256"] for p in changed).encode())[:16]
        elapsed = round(time.time() - started, 1)

        if (not changed and node.get("kind") in (None, "implementation", "foundation", "delivery")
                and not (finished and finished.get("summary"))):
            return {"ok": False, "output": {"artifact": node.get("out_schema") or "code-result-v1",
                                            "steps": steps},
                    "error": {"code": "E-AGENT",
                              "reason": "模型跑了 %d 步但没写入任何文件" % steps}}

        files = changed or ((finished or {}).get("files") or [])
        summary = (finished or {}).get("summary") or "完成 %d 个文件" % len(files)
        perms = sorted({item["path"] for item in self.ws.list("", depth=6)
                        if item["path"].endswith((".sh", ".bash", ".bat", ".ps1", ".cmd"))})
        base = {"files": files, "loc": loc, "steps": steps, "sha256": digest,
                "summary": summary[:600], "model": self.settings.get("model"),
                "usage": usage, "elapsed": elapsed, "exec": perms[:8]}

        # 产物按节点**声明**的 schema 产出，而不是按 kind 猜：
        # 猜错 schema 会被格式门拦下并触发无意义重试。
        schema = node.get("out_schema") or "code-result-v1"
        payload = dict(base, artifact=schema)
        # 模型直接回了一段 JSON 产物（设计/验证类节点常见），把它并进来
        inline = _as_json_obj((finished or {}).get("summary") or "")
        if inline:
            for k, v in inline.items():
                if k != "artifact":
                    payload.setdefault(k, v)

        # 测试/评审类任务：真的跑一次测试，用退出码与输出说话
        if node.get("kind") in ("verification", "integration", "review"):
            cmd = detect_test_command(self.ws)
            if not cmd:
                return {"ok": False, "output": dict(payload, passed=0, total=0),
                        "error": {"code": "E-REVIEW",
                                  "reason": "工作区里没有可执行的测试"
                                            "（需要 tests/ 目录或 package.json 的 test 脚本），"
                                            "不能声称通过"}}
            res = self.ws.run(cmd, timeout=int(self.settings.get("timeout") or 180))
            parsed = parse_test_output(res["output"])
            passed, total = parsed["passed"], parsed["total"]
            ok = res["ok"] and (total == 0 or passed == total)
            checks = [{"criterion": str(a), "status": "pass" if ok else "fail",
                       "evidence": "%s · exit %s" % (cmd, res["code"])}
                      for a in (node.get("acceptance") or [])]
            payload.update(passed=passed, total=total, duration_ms=res["ms"],
                           command=cmd, output_tail=res["output"][-2000:],
                           acceptance_results=checks,
                           merged=bool(ok))
            if not res["ok"]:
                return {"ok": False, "output": payload,
                        "error": {"code": "E-REVIEW",
                                  "reason": "测试失败：%s 退出码 %s" % (cmd, res["code"])}}
            if total and passed < total:
                return {"ok": False, "output": payload,
                        "error": {"code": "E-REVIEW",
                                  "reason": "通过 %d/%d 条测试" % (passed, total)}}

        if schema == "scan-v1":
            # 安全扫描不编分数：真的扫一遍工作区里的危险写法
            findings = scan_workspace(self.ws)
            payload["findings"] = findings
            payload["score"] = security_score(findings)
            payload.setdefault("summary", "扫描 %d 个文件，发现 %d 处风险"
                               % (len(self.ws.list("", depth=6)), len(findings)))

        for key, default in (("endpoints", []), ("entities", []),
                             ("requirements", []), ("tasks", []),
                             ("passed", 0), ("total", 0), ("loc", loc),
                             ("merged", True), ("summary", summary[:600])):
            payload.setdefault(key, default)
        return {"ok": True, "output": payload}
