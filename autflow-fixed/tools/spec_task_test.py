#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""设计规格、PM 任务分解与任务图一致性回归测试。"""
import json
import os
import sys
import tempfile
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
# 单独运行时使用临时数据目录，避免 Engine 初始化改写项目演示数据。
if not os.environ.get("AUTFLOW_DATA_DIR"):
    os.environ["AUTFLOW_DATA_DIR"] = tempfile.mkdtemp(prefix="autflow_spec_task_")

import server  # noqa: E402

RESULTS = []
BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else None


def check(ok, message):
    RESULTS.append(bool(ok))
    print(("  ✓ " if ok else "  ✗ ") + message)


def call(path, body=None, method=None):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, headers={"Content-Type": "application/json"},
        method=method or ("POST" if data is not None else "GET"),
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    print("[1] 规格模块 ID 与 revision")
    before = server.normalize_spec({
        "name": "稳定性测试规格",
        "modules": [
            {"id": "M-A", "name": "模块 A", "acceptance": ["A 验收"]},
            {"id": "M-B", "name": "模块 B", "acceptance": ["B 验收"]},
        ],
    }, spec_id="SP-T")
    after = server.normalize_spec({
        "name": "稳定性测试规格",
        "modules": [
            {"id": "M-B", "name": "模块 B", "acceptance": ["B 验收"]},
            {"id": "M-A", "name": "模块 A", "acceptance": ["A 验收"]},
        ],
    }, spec_id="SP-T")
    a_after = next(m for m in after["modules"] if m["id"] == "M-A")
    check(a_after["name"] == "模块 A", "模块重排后 M-A 仍指向模块 A")

    spec = server.SPECS.get("SP1")
    check(int(spec.get("revision") or 0) >= 1, "规格带有 revision")
    check(len(spec.get("content_hash") or "") == 64, "规格带有 SHA-256 内容指纹")

    print("\n[2] PM 任务契约")
    result = server.PM_AGENT.decompose(spec)
    tasks = result["tasks"]
    required = {
        "id", "title", "module", "role", "priority", "estimate", "depends_on",
        "acceptance", "acceptance_sources", "spec_ref", "spec_revision",
        "spec_hash", "kind", "out_schema", "source",
    }
    check(tasks and required <= set(tasks[0]), "任务包含完整字段契约")
    check(all(len(t["acceptance"]) == len(t["acceptance_sources"]) for t in tasks),
          "每条验收标准都有规格出处")
    check(any(t["kind"] == "design" and t["out_schema"] == "design-v1" for t in tasks),
          "设计任务使用 design-v1")
    check(any(t["kind"] == "verification" and t["out_schema"] == "test-v1" for t in tasks),
          "验收任务使用 test-v1")

    pm_node = {"id": "N2", "role": "pm", "name": "需求分析与拆分", "out_schema": "prd-v1"}
    pm_output = server._mock_output(pm_node, {
        "requirement": spec["summary"],
        "tasks": tasks,
    })
    ok, reason = server.check_output_schema("prd-v1", pm_output)
    check(ok, "PM 产物通过 prd-v1：" + (reason or "ok"))
    check(len(pm_output.get("tasks") or []) == len(tasks), "PM 产物保留全部 tasks")
    check(all("acceptance" in t and "spec_ref" in t for t in pm_output["tasks"]),
          "PM 产物保留验收标准与规格出处")

    print("\n[3] 任务图与执行器")
    engine = server.Engine()
    definition, _ = server.plan_from_tasks(spec, tasks, "default")
    engine.load(definition)
    with engine.lock:
        engine._new_run()
    check(len(engine.run.get("tasks") or []) == len(tasks), "运行快照把 tasks 传给 Agent 上下文")

    valid, errors, _, _ = server.validate_workflow(definition)
    check(valid, "任务清单与 DAG 节点一一对应")
    broken = server._clone(definition)
    broken["nodes"] = [n for n in broken["nodes"] if n.get("task_id") != tasks[0]["id"]]
    valid_broken, broken_errors, _, _ = server.validate_workflow(broken)
    check(not valid_broken and any(e["code"] == "E-TASK-NODE" for e in broken_errors),
          "删除任务节点后校验拒绝悬空任务")

    verification = next(t for t in tasks if t["kind"] == "verification")
    test_output = server._mock_output(
        {"id": "N-T", "task_id": verification["id"], "role": "qa",
         "out_schema": "test-v1", "acceptance": verification["acceptance"]},
        {"tasks": tasks},
    )
    ok, reason = server.check_output_schema("test-v1", test_output)
    check(ok, "验收任务产物通过 test-v1：" + (reason or "ok"))
    check(len(test_output.get("acceptance_results") or []) == len(verification["acceptance"]),
          "验收任务逐条返回验收结果与证据")

    print("\n[4] LLM 产物契约与界面字段")
    parsed = server._parse_llm_output(
        '```json\n{"loc": 12, "files": ["a.py"], "sha256": "abc"}\n```',
        "code-result-v1",
    )
    check(parsed.get("artifact") == "code-result-v1", "LLM JSON 按节点 schema 解析，不套 llm-v1")

    with open(os.path.join(ROOT, "static", "app.js"), encoding="utf-8") as f:
        app_js = f.read()
    with open(os.path.join(ROOT, "static", "index.html"), encoding="utf-8") as f:
        index_html = f.read()
    check("验收标准" in app_js and "规格出处" in app_js,
          "任务清单界面直接渲染验收标准与规格出处")
    check("syncTaskDependencies" in app_js, "任务节点编辑会同步任务依赖")
    check('id="selSpecTemplate"' in index_html,
          "设计规格面板提供独立的流程模板选择")
    check("const tplSel = $('#selSpecTemplate')" in app_js,
          "规格编排使用规格模板下拉框，不复用快速生成入口")
    check("await refresh();" in app_js and "render();" in app_js,
          "规格编排成功后立即刷新画布和标题")
    check("draftName" in app_js,
          "工作流栏标题优先使用当前草稿定义名称")

    if BASE:
        print("\n[5] 规格 API 与编排接口")
        created = call("/api/specs", {
            "name": "API 任务契约测试",
            "summary": "验证规格更新、分解与编排",
            "modules": [
                {"name": "接口模块", "priority": "P0", "acceptance": ["接口可用"]},
                {"name": "测试模块", "priority": "P1", "acceptance": ["回归通过"]},
            ],
        })
        sid = created["spec"]["id"]
        api_before = call("/api/specs/" + sid)
        mods = api_before["spec"]["modules"]
        reordered = [mods[1], mods[0]]
        updated = call("/api/specs/" + sid, {
            "name": api_before["spec"]["name"],
            "summary": api_before["spec"]["summary"],
            "body": api_before["spec"].get("body", ""),
            "constraints": api_before["spec"].get("constraints", []),
            "modules": reordered,
        }, "PUT")
        check(updated["spec"]["revision"] == 2, "API 更新后 revision 递增")
        check(next(m for m in updated["spec"]["modules"] if m["id"] == mods[0]["id"])["name"]
              == "接口模块", "API 重排后模块 ID 不漂移")

        decomposed = call("/api/specs/%s/decompose" % sid, {})
        check(decomposed["ok"] and all(t.get("out_schema") for t in decomposed["tasks"]),
              "分解 API 返回任务级 schema")
        planned = call("/api/specs/%s/plan" % sid, {"template": "default"})
        draft = planned["draft"]["definition"]
        check(planned.get("spec_revision") == 2 and len(draft.get("spec_snapshot", {}).get("modules", [])) == 2,
              "编排 API 冻结规格 revision 与快照")
        check(len(draft.get("tasks") or []) == len(decomposed["tasks"]),
              "编排 API 保留完整任务清单")

    passed = sum(RESULTS)
    print("\n结果: %s (%d/%d)" % (
        "PASS" if passed == len(RESULTS) else "FAIL", passed, len(RESULTS)))
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
