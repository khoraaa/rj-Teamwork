#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Focused tests for safe, read-only substep code previews."""

import json
import os
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
    def setUp(self):
        self._old_wf = server.ENGINE.wf
        self._old_status = server.ENGINE.run.get("status")
        self._old_agent = server.ENGINE.cfg.get("agent")
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
        server.ENGINE.run["status"] = "IDLE"

    def tearDown(self):
        server.ENGINE.wf = self._old_wf
        server.ENGINE.run["status"] = self._old_status
        server.ENGINE.cfg["agent"] = self._old_agent

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
                response = urllib.request.urlopen(request, timeout=5)
            except Exception as exc:
                response = exc
            with response:
                return response.status, json.loads(response.read().decode("utf-8"))
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)

    def test_preview_returns_code_without_creating_workspace(self):
        reply = {"content": "```python\nprint('preview')\n```"}
        with mock.patch.dict(server.SETTINGS.data, {
                "base_url": "https://example.invalid/v1", "api_key": "test-key"}), \
                mock.patch.object(server.LLM, "chat", return_value=reply) as chat:
            status, body = self.post_preview({"task_id": "T1", "step_index": 0})

        self.assertEqual(status, 200)
        self.assertEqual(body["code"], "print('preview')")
        self.assertFalse(body["saved"])
        self.assertFalse(body["executed"])
        self.assertEqual(body["path"], "src/login.py")
        self.assertNotIn("tools", chat.call_args.kwargs)
        self.assertFalse(os.path.exists(server.WORKSPACES_DIR))

    def test_missing_target_file_is_rejected_without_model_call(self):
        server.ENGINE.wf["tasks"][0]["files"] = []
        with mock.patch.object(server.LLM, "chat") as chat:
            status, body = self.post_preview({"task_id": "T1", "step_index": 0})

        self.assertEqual(status, 400)
        self.assertIn("没有关联", body["message"])
        chat.assert_not_called()

    def test_default_agent_selection_stays_mock(self):
        server.ENGINE.cfg["agent"] = "llm"
        with mock.patch.object(server, "LOCAL_CODE_EXECUTION_ENABLED", False):
            self.assertEqual(server.ENGINE._agent_kind({"id": "N1", "role": "coder"}), "mock")


if __name__ == "__main__":
    unittest.main(verbosity=2)
