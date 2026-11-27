#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Focused tests for the substep code viewer.

行为契约：
  1. 本次运行真的写了文件 → 直接读文件，绝不调用模型；
  2. 模拟运行没写文件 → 按子步骤生成一次并缓存，再点同一子步骤只读缓存；
  3. 模型返回空内容 → 502，消息里带 finish_reason，方便判断是不是 max_tokens 太小；
  4. 越界路径 → 400，且不会触发生成；
  5. 没配模型 → 400，提示先去「模型」里配置。
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

_DATA_DIR = tempfile.TemporaryDirectory(prefix="autflow-preview-test-")
os.environ["AUTFLOW_DATA_DIR"] = _DATA_DIR.name
os.environ.pop("AUTFLOW_ENABLE_LOCAL_CODE_EXECUTION", None)

import server


class CodePreviewTest(unittest.TestCase):
    RUN_ID = "RUN-PREVIEW-1"

    def setUp(self):
        self._old_wf = server.ENGINE.wf
        self._old_run = dict(server.ENGINE.run)
        self._old_agent = server.ENGINE.cfg.get("agent")
        self._old_archive = server.ARCHIVE.runs
        server.ARCHIVE.runs = []
        if os.path.isdir(server.PREVIEW_CACHE_DIR):
            shutil.rmtree(server.PREVIEW_CACHE_DIR)
        server.ENGINE.wf = {
            "workflow_id": "preview-test",
            "tasks": [{
                "id": "T1", "title": "实现登录", "phase": "编码",
                "files": ["src/login.py"],
                "steps": [{"title": "编写登录逻辑", "how": "校验用户名和密码"}],
            }],
            "nodes": [{"id": "N1", "type": "agent", "task_id": "T1"}],
            "edges": [],
        }
        self.ws_dir = os.path.join(server.WORKSPACES_DIR, self.RUN_ID)
        if os.path.isdir(self.ws_dir):
            shutil.rmtree(self.ws_dir)
        os.makedirs(self.ws_dir, exist_ok=True)
        server.ENGINE.run.update({
            "run_id": self.RUN_ID,
            "status": "WAITING_HUMAN",
            "workspace": self.ws_dir,
            "definition": server.ENGINE.wf,
        })

    def tearDown(self):
        server.ENGINE.wf = self._old_wf
        server.ENGINE.run.clear()
        server.ENGINE.run.update(self._old_run)
        server.ENGINE.cfg["agent"] = self._old_agent
        server.ARCHIVE.runs = self._old_archive

    def write_file(self, rel, text):
        path = os.path.join(self.ws_dir, *rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def post_preview(self, payload):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            request = urllib.request.Request(
                "http://127.0.0.1:%d/api/tasks/preview-code" % httpd.server_address[1],
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                response = urllib.request.urlopen(request, timeout=10)
            except Exception as exc:
                response = exc
            with response:
                status = getattr(response, "status", None) or getattr(response, "code", None)
                return status, json.loads(response.read().decode("utf-8"))
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)

    def model_settings(self):
        return mock.patch.dict(
            server.SETTINGS.data,
            {"base_url": "http://model.invalid/v1", "api_key": "test-key", "model": "m"})

    def test_reads_workspace_file_without_model_call(self):
        self.write_file("src/login.py", "def login(u, p):\n    return True\n")
        with mock.patch.object(server.LLM, "chat") as chat:
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 200)
        self.assertIn("def login", body["code"])
        self.assertEqual(body["path"], "src/login.py")
        self.assertEqual(body["source"], "workspace")
        self.assertFalse(body["saved"])
        self.assertFalse(body["executed"])
        chat.assert_not_called()

    def test_generates_once_then_reads_cache(self):
        reply = {"content": "```python\ndef login(u, p):\n    return u == 'a'\n```",
                 "finish": "stop"}
        with self.model_settings(), mock.patch.object(
                server.LLM, "chat", return_value=reply) as chat:
            status, first = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0, "generate": 1})
            status2, second = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 200)
        self.assertEqual(first["source"], "generated")
        self.assertIn("def login", first["code"])
        self.assertNotIn("```", first["code"])
        self.assertFalse(first["executed"])
        self.assertEqual(status2, 200)
        self.assertEqual(second["source"], "cache")
        self.assertEqual(second["code"], first["code"])
        self.assertEqual(chat.call_count, 1)

    def test_empty_model_content_reports_finish_reason(self):
        with self.model_settings(), mock.patch.object(
                server.LLM, "chat", return_value={"content": "", "finish": "length"}) as chat:
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0, "generate": 1})

        self.assertEqual(status, 502)
        self.assertFalse(body["ok"])
        self.assertIn("finish_reason=length", body["message"])
        self.assertEqual(chat.call_count, 2)  # 空返回会用短提示词重试一次

    def test_missing_model_config_is_actionable(self):
        with mock.patch.dict(server.SETTINGS.data, {"base_url": "", "api_key": ""}):
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0, "generate": 1})

        self.assertEqual(status, 400)
        self.assertIn("模型", body["message"])

    def test_path_escape_is_rejected_without_generation(self):
        outside = os.path.join(os.path.dirname(self.ws_dir), "secret.py")
        with open(outside, "w", encoding="utf-8") as f:
            f.write("TOP SECRET")
        server.ENGINE.wf["tasks"][0]["files"] = ["../secret.py"]
        with self.model_settings(), mock.patch.object(server.LLM, "chat") as chat:
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 400)
        self.assertNotIn("SECRET", body.get("code") or "")
        self.assertIn("工作区", body["message"])
        chat.assert_not_called()

    def test_archived_run_reads_its_own_workspace(self):
        archived_id = "RUN-PREVIEW-ARCHIVED"
        ws = os.path.join(server.WORKSPACES_DIR, archived_id)
        os.makedirs(os.path.join(ws, "src"), exist_ok=True)
        with open(os.path.join(ws, "src", "login.py"), "w", encoding="utf-8") as f:
            f.write("# archived code\n")
        server.ARCHIVE.runs = [{
            "run_id": archived_id,
            "definition": server.ENGINE.wf,
        }]
        server.ENGINE.run["status"] = "IDLE"
        status, body = self.post_preview({
            "run_id": archived_id, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 200)
        self.assertIn("archived code", body["code"])
        self.assertEqual(body["run_id"], archived_id)

    def test_missing_target_file_is_rejected(self):
        server.ENGINE.wf["tasks"][0]["files"] = []
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 400)
        self.assertIn("没有关联", body["message"])

    def test_default_agent_selection_stays_mock(self):
        server.ENGINE.cfg["agent"] = "llm"
        with mock.patch.object(server, "LOCAL_CODE_EXECUTION_ENABLED", False):
            self.assertEqual(server.ENGINE._agent_kind({"id": "N1", "role": "coder"}), "mock")

    def test_symbols_index_is_returned(self):
        self.write_file("src/login.py",
                        "class Auth:\n    def login(self):\n        return True\n")
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})
        names = [s["name"] for s in body["symbols"]]
        self.assertEqual(status, 200)
        self.assertIn("Auth", names)
        self.assertIn("login", names)
        self.assertEqual(body["granularity"], "file")   # 该步骤没有计划符号，整份返回
        self.assertIsNone(body["slice"])
        self.assertEqual(body["strategy"], "indent")

    def test_splits_brace_language(self):
        self.write_file("src/login.py",
                        "export class Auth {\n  login(u) {\n    return true;\n  }\n}\n")
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})
        names = [s["name"] for s in body["symbols"]]
        self.assertEqual(status, 200)
        self.assertIn("Auth", names)
        self.assertIn("login", names)
        self.assertEqual(body["strategy"], "brace")

    def test_file_without_symbols_is_file_granularity(self):
        self.write_file("src/login.py", "just a note, no declaration\n")
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})
        self.assertEqual(status, 200)
        self.assertEqual(body["symbols"], [])
        self.assertEqual(body["granularity"], "file")


    def test_step_returns_only_its_own_symbol(self):
        self.write_file("src/login.py",
                        "class User:\n    pass\n\n\nclass Order:\n    pass\n")
        server.ENGINE.wf["tasks"][0]["steps"][0]["symbols"] = ["class User"]
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 200)
        self.assertIn("class User", body["code"])
        self.assertNotIn("class Order", body["code"])
        self.assertEqual(body["granularity"], "symbol")
        self.assertEqual(body["slice"]["start"], 1)
        self.assertEqual(body["slice"]["end"], 2)
        self.assertIn("User", body["slice"]["names"])

    def test_whole_flag_returns_full_file(self):
        self.write_file("src/login.py",
                        "class User:\n    pass\n\n\nclass Order:\n    pass\n")
        server.ENGINE.wf["tasks"][0]["steps"][0]["symbols"] = ["class User"]
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0, "whole": 1})

        self.assertEqual(status, 200)
        self.assertIn("class User", body["code"])
        self.assertIn("class Order", body["code"])
        self.assertEqual(body["granularity"], "file")
        self.assertIsNone(body["slice"])


    def test_whole_can_switch_to_any_task_file(self):
        self.write_file("src/login.py", "# login\n")
        self.write_file("src/user.py", "# user store\n")
        server.ENGINE.wf["tasks"][0]["files"] = ["src/login.py", "src/user.py"]
        status, body = self.post_preview({
            "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0,
            "whole": 1, "path": "src/user.py"})

        self.assertEqual(status, 200)
        self.assertIn("user store", body["code"])
        self.assertEqual(body["path"], "src/user.py")
        self.assertEqual(body["task_files"], ["src/login.py", "src/user.py"])

    def test_whole_rejects_file_not_in_task(self):
        self.write_file("src/login.py", "# login\n")
        outside = os.path.join(self.ws_dir, "src", "other.py")
        with open(outside, "w", encoding="utf-8") as f:
            f.write("# other\n")
        with mock.patch.object(server.LLM, "chat") as chat:
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0,
                "whole": 1, "path": "src/other.py"})

        self.assertEqual(status, 400)
        self.assertIn("关联的文件", body["message"])
        chat.assert_not_called()


    def test_missing_file_does_not_call_model_by_default(self):
        with mock.patch.object(server.LLM, "chat") as chat:
            status, body = self.post_preview({
                "run_id": self.RUN_ID, "task_id": "T1", "step_index": 0})

        self.assertEqual(status, 404)
        self.assertTrue(body["missing"])
        self.assertIn("模拟运行不写盘", body["message"])
        self.assertEqual(body["task_files"], ["src/login.py"])
        chat.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)