#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""C 任务 · 隔离式验收编排器。

每个需要服务的套件都使用独立的临时数据目录和动态端口，避免测试之间改写草稿、
发布版本或历史。结果写入 acceptance-results/<timestamp>/，包括逐个套件日志、
环境信息和汇总 Markdown/JSON。

用法:
  python tools/acceptance_runner.py
  python tools/acceptance_runner.py --skip-browser
  python tools/acceptance_runner.py --only e2e_acceptance --only browser_e2e
"""
import argparse
import datetime
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server.py")
ARTIFACT_ROOT = os.path.join(ROOT, "acceptance-results")


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def wait_health(base, proc, timeout=20):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("服务进程提前退出，退出码 %s" % proc.returncode)
        try:
            with urllib.request.urlopen(base + "/api/health", timeout=1) as res:
                data = json.loads(res.read().decode("utf-8"))
            if data.get("ok"):
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.15)
    raise RuntimeError("等待服务健康检查超时：%s" % (last_error or "无响应"))


def stop_process(proc):
    if not proc or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def command_text(command):
    return " ".join(str(x) for x in command)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_environment():
    env = {
        "timestamp": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": sys.version.replace("\n", " "),
        "server_sha256": sha256(SERVER),
    }
    try:
        env["node"] = subprocess.check_output(
            ["node", "--version"], text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        env["node"] = "unavailable: %s" % exc
    if "com.docker" in platform.platform().lower() or os.path.exists("/.dockerenv"):
        env["runtime"] = "container"
    else:
        env["runtime"] = "host"
    return env


def suite_definitions():
    py = sys.executable
    specs = [
        {
            "name": "python_compile",
            "title": "Python 语法编译",
            "command": [py, "-m", "py_compile", "server.py", "tools/acceptance_runner.py",
                        "tools/e2e_acceptance.py", "tools/recovery_test.py",
                        "tools/executor_test.py"],
            "server": False,
        },
        {
            "name": "frontend_syntax",
            "title": "前端 JavaScript 语法",
            "command": ["node", "--check", "static/app.js"],
            "server": False,
        },
        {
            "name": "check_ui",
            "title": "DOM 引用与布局静态检查",
            "command": [py, "tools/check_ui.py"],
            "server": False,
        },
        {
            "name": "spec_task_test",
            "title": "设计规格、PM 任务契约、任务图与编排 API",
            "command": [py, "tools/spec_task_test.py"],
            "server": True,
        },
        {
            "name": "demo_matrix",
            "title": "规格 × 模板定制能力矩阵",
            "command": [py, "tools/demo_matrix.py", "--base"],
            "server": True,
        },
        {
            "name": "e2e_acceptance",
            "title": "C · 需求到历史完整 API 链路",
            "command": [py, "tools/e2e_acceptance.py"],
            "server": True,
            "with_artifacts": True,
        },
        {
            "name": "smoke_test",
            "title": "主链路冒烟",
            "command": [py, "tools/smoke_test.py"],
            "server": True,
        },
        {
            "name": "scenario_test",
            "title": "分支 / 审批 / 暂停 / 校验 / 重试",
            "command": [py, "tools/scenario_test.py"],
            "server": True,
        },
        {
            "name": "editor_test",
            "title": "编辑、版本与运行期隔离",
            "command": [py, "tools/editor_test.py"],
            "server": True,
        },
        {
            "name": "graph_edit_test",
            "title": "结构改图、发布与执行",
            "command": [py, "tools/graph_edit_test.py"],
            "server": True,
        },
        {
            "name": "generate_test",
            "title": "需求生成、审批与历史归档",
            "command": [py, "tools/generate_test.py"],
            "server": True,
        },
        {
            "name": "executor_test",
            "title": "B · 执行器、错误分类与超时重试",
            "command": [py, "tools/executor_test.py"],
            "server": True,
        },
        {
            "name": "fe_test",
            "title": "前端交互联调（DOM 桩）",
            "command": ["node", "tools/fe_test.js"],
            "server": True,
        },
        {
            "name": "recovery_test",
            "title": "B · 运行中崩溃与重启恢复",
            "command": [py, "tools/recovery_test.py"],
            "server": False,
        },
        {
            "name": "browser_e2e",
            "title": "C · 真实浏览器完整验收",
            "command": ["node", "tools/browser_e2e.js"],
            "server": True,
            "with_artifacts": True,
            "browser": True,
        },
    ]
    return specs


def run_suite(spec, out_dir, timeout, skip_browser):
    if spec.get("browser") and skip_browser:
        return {
            "name": spec["name"],
            "title": spec["title"],
            "status": "SKIP",
            "duration_sec": 0,
            "command": command_text(spec["command"]),
            "log": "",
            "reason": "按参数跳过浏览器验收",
        }

    command = list(spec["command"])
    data_dir = None
    server_proc = None
    server_log_path = os.path.join(out_dir, spec["name"] + "-server.log")
    suite_artifacts = os.path.join(out_dir, spec["name"] + "-artifacts")
    started = time.time()
    env = dict(os.environ)
    env["PYTHONPYCACHEPREFIX"] = os.path.join(tempfile.gettempdir(), "autflow-pycache")
    try:
        if spec["server"]:
            data_dir = tempfile.mkdtemp(prefix="autflow_accept_%s_" % spec["name"])
            port = free_port()
            base = "http://127.0.0.1:%d" % port
            env["AUTFLOW_DATA_DIR"] = data_dir
            log_stream = open(server_log_path, "w", encoding="utf-8")
            try:
                server_proc = subprocess.Popen(
                    [sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
                    cwd=ROOT, env=env, stdout=log_stream,
                    stderr=subprocess.STDOUT, text=True)
                wait_health(base, server_proc)
                command.append(base)
                if spec.get("with_artifacts"):
                    command.append(suite_artifacts)
            finally:
                log_stream.close()

        proc = subprocess.run(
            command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=timeout)
        output = proc.stdout or ""
        status = "PASS" if proc.returncode == 0 else "FAIL"
        reason = "" if proc.returncode == 0 else "退出码 %d" % proc.returncode
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + "\n超时：%s 秒" % timeout
        status = "FAIL"
        reason = "执行超时"
    except Exception as exc:
        output = "套件启动/执行异常：%s" % exc
        status = "FAIL"
        reason = str(exc)
    finally:
        stop_process(server_proc)
        if data_dir:
            shutil.rmtree(data_dir, ignore_errors=True)

    log_path = os.path.join(out_dir, spec["name"] + ".log")
    with open(log_path, "w", encoding="utf-8") as stream:
        stream.write("$ %s\n\n" % command_text(command))
        stream.write(output)
        if not output.endswith("\n"):
            stream.write("\n")

    result = {
        "name": spec["name"],
        "title": spec["title"],
        "status": status,
        "duration_sec": round(time.time() - started, 2),
        "command": command_text(command),
        "log": os.path.relpath(log_path, ROOT),
        "reason": reason,
    }
    if spec["server"]:
        result["server_log"] = os.path.relpath(server_log_path, ROOT)
    if os.path.isdir(suite_artifacts):
        result["artifacts"] = os.path.relpath(suite_artifacts, ROOT)
    return result


def write_reports(out_dir, environment, results):
    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    skipped = sum(r["status"] == "SKIP" for r in results)
    summary = {
        "status": "PASS" if failed == 0 else "FAIL",
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "environment": environment,
        "results": results,
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2)

    lines = [
        "# AutoFlow C 任务验收报告",
        "",
        "- 总结果：**%s**" % summary["status"],
        "- 通过 / 失败 / 跳过：%d / %d / %d" % (passed, failed, skipped),
        "- 执行时间：%s" % environment["timestamp"],
        "- 运行环境：%s" % environment["platform"],
        "- Python：%s" % environment["python"],
        "- Node：%s" % environment["node"],
        "- server.py SHA-256：`%s`" % environment["server_sha256"],
        "",
        "| 套件 | 状态 | 用时 | 说明 |",
        "|---|---:|---:|---|",
    ]
    for item in results:
        detail = item.get("reason") or item.get("log") or ""
        lines.append("| %s | %s | %.2fs | %s |" % (
            item["title"], item["status"], item["duration_sec"],
            str(detail).replace("|", "\\|")))
    lines.extend([
        "",
        "每个需要服务的套件均使用独立临时数据目录与动态端口，测试不会改写演示数据。",
        "浏览器截图位于 `browser_e2e-artifacts/`，完整输出见各套件 `.log` 文件。",
        "",
    ])
    with open(os.path.join(out_dir, "summary.md"), "w", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description="AutoFlow C 任务隔离验收")
    parser.add_argument("--only", action="append", default=[],
                        help="只运行指定套件名，可重复传入")
    parser.add_argument("--skip-browser", action="store_true",
                        help="跳过真实浏览器套件")
    parser.add_argument("--timeout", type=int, default=240,
                        help="单个套件超时秒数，默认 240")
    parser.add_argument("--out", default="",
                        help="结果目录，默认 acceptance-results/<timestamp>")
    args = parser.parse_args()

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.abspath(args.out or os.path.join(ARTIFACT_ROOT, stamp))
    os.makedirs(out_dir, exist_ok=True)

    specs = suite_definitions()
    if args.only:
        wanted = set(args.only)
        specs = [s for s in specs if s["name"] in wanted]
        missing = wanted - {s["name"] for s in specs}
        if missing:
            print("未知套件：" + "、".join(sorted(missing)), file=sys.stderr)
            return 2

    environment = collect_environment()
    results = []
    print("验收输出目录:", out_dir)
    print("环境:", environment["platform"], environment["python"])
    for spec in specs:
        print("\n>>> [%s] %s" % (spec["name"], spec["title"]), flush=True)
        result = run_suite(spec, out_dir, args.timeout, args.skip_browser)
        results.append(result)
        print("<<< %s %.2fs" % (result["status"], result["duration_sec"]), flush=True)

    write_reports(out_dir, environment, results)
    failed = [r for r in results if r["status"] == "FAIL"]
    print("\n结果:", "PASS" if not failed else "FAIL",
          "(%d/%d)" % (len(results) - len(failed), len(results)))
    print("报告:", os.path.join(out_dir, "summary.md"))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
