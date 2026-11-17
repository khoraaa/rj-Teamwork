#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""定制能力自证：同一引擎下，规格 × 模板 的对比矩阵。

用法：
    python tools/demo_matrix.py            # 自动起一个隔离实例（临时数据目录 + 随机端口）
    python tools/demo_matrix.py --base http://127.0.0.1:8010   # 复用已在跑的实例（会覆盖其草稿）

做的事：
  1. 造两份结构差异明显的设计规格（4 模块 / 2 模块）；
  2. 对每份规格，分别用 default / lean / enterprise 三个模板编排；
  3. 校验每张图，打印 任务数 / 节点数 / 边数 / 关键路径，并对照"同规格换模板"与"同模板换规格"。
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server.py")

SPEC_A = {
    "name": "社区内容平台（小红书式）",
    "summary": "做一个图文内容社区：发笔记、推荐流、点赞评论关注、内容合规审核",
    "body": "目标：面向年轻用户的内容社区 MVP。\n"
            "核心链路：发布笔记 → 进入推荐池 → 首页信息流分发 → 互动 → 站内通知。\n"
            "红线：公开内容先过审核，违规内容不得进入推荐池，审核动作必须留痕。",
    "modules": [
        {"name": "账号与内容发布", "priority": "P0",
         "goal": "登录后发布图文笔记，支持草稿与定时发布",
         "acceptance": ["支持图文草稿保存与恢复",
                        "发布成功后 3 秒内可被检索到",
                        "违规内容在发布环节即被拦截"]},
        {"name": "信息流推荐", "priority": "P0",
         "goal": "首页推荐流，按兴趣与热度混排",
         "acceptance": ["首屏渲染 < 800ms",
                        "支持下拉刷新与无限滚动",
                        "同一用户 10 分钟内不重复推荐同一条"]},
        {"name": "互动与通知", "priority": "P1",
         "goal": "点赞、评论、关注，以及对应的站内通知",
         "acceptance": ["互动后 1 秒内产生通知",
                        "通知列表区分已读未读",
                        "取关后不再收到该用户通知"]},
        {"name": "内容审核与风控", "priority": "P0",
         "goal": "文本敏感词与图片机审，命中转人工复核队列",
         "acceptance": ["敏感词命中率 ≥ 95%",
                        "人工复核队列支持指派与结论回写",
                        "每次审核动作写入审计日志"]},
    ],
    "constraints": ["内容合规须满足《网络信息内容生态治理规定》",
                    "灰度发布，支持按用户分桶与一键回滚"],
}

SPEC_B = {
    "name": "支付对账中台",
    "summary": "做支付渠道的 T+1 对账：拉流水、比对差异、差错工单与人工核销",
    "body": "目标：把三方支付渠道的日终流水与内部交易单做 T+1 对账，差异进差错池。\n"
            "核心链路：定时拉取对账单 → 逐笔比对 → 差异分类 → 差错工单 → 人工核销。\n"
            "红线：资金数据只读、不出内网；所有核销动作双人复核。",
    "modules": [
        {"name": "对账任务调度", "priority": "P0",
         "goal": "每日定时拉取渠道对账单并完成逐笔比对",
         "acceptance": ["T+1 上午 9 点前产出对账结果",
                        "差异按类型自动分类（金额/状态/单边账）",
                        "任务失败支持按日期重跑且幂等"]},
        {"name": "差错处理与人工核销", "priority": "P0",
         "goal": "差错工单的指派、处理、双人复核与留痕",
         "acceptance": ["工单可指派到人并带 SLA 计时",
                        "核销必须双人复核方可关闭",
                        "全链路操作审计可追溯"]},
    ],
    "constraints": ["资金数据只读访问，禁止出内网",
                    "对账差异率需 < 0.01% 才算通过验收"],
}

TEMPLATES = ["default", "lean", "enterprise"]


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def call(base, path, body=None, method=None, timeout=20):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, headers={"Content-Type": "application/json"},
        method=method or ("POST" if data is not None else "GET"))
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return json.loads(res.read().decode("utf-8"))


def wait_health(base, proc, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("服务进程提前退出，退出码 %s" % proc.returncode)
        try:
            call(base, "/api/health", timeout=2)
            return
        except Exception:
            time.sleep(0.15)
    raise RuntimeError("服务未在 %s 秒内就绪" % timeout)


def start_server():
    # 不用 tempfile.mkdtemp：它的 0o700 DACL 在受限沙箱里会让服务进程写不进去，
    # 用工作区内的临时目录更稳（跑完即删）。
    port = free_port()
    data_dir = os.path.join(ROOT, ".tmp", "matrix-%d" % port)
    os.makedirs(data_dir, exist_ok=True)
    env = dict(os.environ)
    env["AUTFLOW_DATA_DIR"] = data_dir
    env["PYTHONUTF8"] = "1"
    log_path = os.path.join(data_dir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, text=True)
    base = "http://127.0.0.1:%d" % port
    try:
        wait_health(base, proc)
    except Exception:
        log.close()
        raise
    return proc, base, data_dir


def cjk_pad(text, width, align="left"):
    """按显示宽度对齐（中文按 2 列算），避免表格在终端里歪掉。"""
    text = str(text)
    used = sum(2 if _wide(ch) else 1 for ch in text)
    gap = max(0, width - used)
    return text + " " * gap if align == "left" else " " * gap + text


def _wide(ch):
    import unicodedata
    return unicodedata.east_asian_width(ch) in ("W", "F")


def main():
    base = None
    proc = None
    data_dir = None
    if "--base" in sys.argv:
        base = sys.argv[sys.argv.index("--base") + 1].rstrip("/")
    else:
        proc, base, data_dir = start_server()

    try:
        print("实例: %s\n" % base)
        ids = []
        for spec in (SPEC_A, SPEC_B):
            out = call(base, "/api/specs", spec)
            ids.append((spec["name"], out["spec"]["id"]))

        print("规格：")
        for name, sid in ids:
            print("  %-4s %s" % (sid, name))
        print()

        rows = []
        for name, sid in ids:
            for tpl in TEMPLATES:
                out = call(base, "/api/specs/%s/plan" % sid, {"template": tpl})
                d = out["draft"]["definition"]
                val = call(base, "/api/validate", {"workflow": d})
                rows.append({
                    "spec": sid, "name": name, "template": tpl,
                    "tasks": len(out["tasks"]), "nodes": len(d["nodes"]),
                    "edges": len(d["edges"]),
                    "depth": len(out["stats"].get("critical_path") or []),
                    "valid": val["valid"], "errors": val["errors"],
                })

        head = (cjk_pad("规格", 6) + cjk_pad("规格名", 26) + cjk_pad("模板", 12)
                + cjk_pad("任务", 6, "right") + cjk_pad("节点", 6, "right")
                + cjk_pad("边", 6, "right") + cjk_pad("深度", 6, "right")
                + cjk_pad("校验", 8, "right"))
        print(head)
        print("-" * 76)
        for r in rows:
            print(cjk_pad(r["spec"], 6) + cjk_pad(r["name"][:12], 26)
                  + cjk_pad(r["template"], 12)
                  + cjk_pad(r["tasks"], 6, "right") + cjk_pad(r["nodes"], 6, "right")
                  + cjk_pad(r["edges"], 6, "right") + cjk_pad(r["depth"], 6, "right")
                  + cjk_pad("PASS" if r["valid"] else "FAIL", 8, "right"))

        bad = [r for r in rows if not r["valid"]]
        print()
        if bad:
            for r in bad:
                print("校验失败 %s × %s：%s" % (r["spec"], r["template"], r["errors"]))
            return 1

        # 两个维度的差异，直接算出来给结论
        print("同规格换模板（流程形状变化）：")
        for name, sid in ids:
            group = [r for r in rows if r["spec"] == sid]
            print("  %s：%s" % (sid, " → ".join(
                "%s %d 节点" % (r["template"], r["nodes"]) for r in group)))
        print("同模板换规格（任务内容变化）：")
        for tpl in TEMPLATES:
            group = [r for r in rows if r["template"] == tpl]
            print("  %-10s：%s" % (tpl, " → ".join(
                "%s %d 任务/%d 节点" % (r["spec"], r["tasks"], r["nodes"]) for r in group)))
        return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if data_dir:
            import shutil
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
