"""真实模型接入：配置存储、OpenAI / Anthropic 兼容客户端、连通性探针。

设计约束（这是工程工具，不是演示）：
  * 密钥只存在 `<数据目录>/settings.json`，接口只回掩码，日志里不出现明文；
  * 所有网络调用都有超时、有限重试、可分类错误码，绝不无限等待；
  * 没有配置端点/密钥时抛 LLMNotConfigured，由上层决定回退到本地模拟还是报错。
"""
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request

DEFAULT_SETTINGS = {
    "provider": "openai",            # openai | anthropic | custom
    "base_url": "https://api.openai.com/v1",
    "model": "gpt-4o-mini",
    "api_key": "",
    "temperature": 0.2,
    "max_tokens": 4096,
    "timeout": 180,
    "max_steps": 14,                 # 工具循环最多几步
    "mode": "mock",                  # mock：本地确定性模拟；llm：真实模型
    # 多 AI 接入：每个条目是一个独立的端点/模型/密钥，可分别指派给讨论角色或任务分组
    "ais": [],
    "ignore_env_key": False,        # 用户显式清除后为 True：环境变量不再把密钥塞回来
    "role_ais": {},                  # {"pm": "ai-1", "architect": "ai-2", ...}
    "part_ais": {},                  # {"数据模型": "ai-1", "接口实现": "ai-2", ...}
}

PROVIDER_PRESETS = [
    {"id": "openai", "label": "OpenAI 兼容（DeepSeek / Kimi / 通义 / vLLM 等）",
     "base_url": "https://api.openai.com/v1", "model": "gpt-4o-mini",
     "docs": "任何兼容 /chat/completions 的端点都可以，填到 /v1 为止"},
    {"id": "anthropic", "label": "Anthropic Claude",
     "base_url": "https://api.anthropic.com/v1", "model": "claude-sonnet-4-5",
     "docs": "走 /messages 接口，需要 x-api-key"},
    {"id": "deepseek", "label": "DeepSeek",
     "base_url": "https://api.deepseek.com/v1", "model": "deepseek-chat",
     "docs": "OpenAI 兼容协议"},
    {"id": "moonshot", "label": "Moonshot / Kimi",
     "base_url": "https://api.moonshot.cn/v1", "model": "moonshot-v1-8k",
     "docs": "OpenAI 兼容协议"},
    {"id": "dashscope", "label": "阿里通义 DashScope",
     "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
     "model": "qwen-plus", "docs": "OpenAI 兼容模式"},
    {"id": "ollama", "label": "本地 Ollama / vLLM",
     "base_url": "http://127.0.0.1:11434/v1", "model": "qwen2.5-coder:7b",
     "docs": "本机模型，key 随便填"},
]


class LLMError(Exception):
    """带稳定错误码的模型调用异常：E-NETWORK / E-TIMEOUT / E-AGENT / E-FORMAT。"""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


class LLMNotConfigured(LLMError):
    def __init__(self, message="未配置模型端点或密钥"):
        super().__init__("E-AGENT", message)


def mask_key(key):
    """只回显可辨识的前后 4 位，中间打码。"""
    key = (key or "").strip()
    if not key:
        return ""
    if len(key) <= 10:
        return key[:2] + "****"
    return key[:4] + "…" + key[-4:]


class SettingsStore:
    """把模型配置落盘；写入走临时文件 + 原子替换，避免半截 JSON。"""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = dict(DEFAULT_SETTINGS)
        self.env_key = False
        self.load()

    # ---------------- 多 AI 接入点 ----------------
    def _new_id(self):
        used = {a.get("id") for a in (self.data.get("ais") or [])}
        i = 1
        while ("ai-%d" % i) in used:
            i += 1
        return "ai-%d" % i

    def ais(self):
        with self.lock:
            return [dict(a) for a in (self.data.get("ais") or [])]

    def spec(self, ai_id=None):
        """取某个 AI 的有效配置；ai_id 为空走默认（原来的单模型配置）。"""
        with self.lock:
            base = dict(self.data)
            if not ai_id or ai_id in ("default", ""):
                return base
            for a in (self.data.get("ais") or []):
                if a.get("id") == ai_id:
                    merged = dict(base)
                    for k in ("temperature", "max_tokens", "timeout", "max_steps"):
                        if a.get(k) not in (None, ""):
                            merged[k] = a[k]
                    # 端点/模型/密钥只认这个 AI 自己的：没填就是没配，
                    # 绝不能悄悄回落到主模型的密钥去调用
                    merged["provider"] = a.get("provider") or "openai"
                    merged["base_url"] = a.get("base_url") or ""
                    merged["model"] = a.get("model") or ""
                    merged["api_key"] = a.get("api_key") or ""
                    merged["mode"] = "llm"
                    merged["_ai_id"] = ai_id
                    merged["_ai_label"] = a.get("label") or a.get("model") or ai_id
                    return merged
            return base

    def upsert_ai(self, patch):
        """新增/修改一个 AI。空 key 或掩码 key 表示"不改动已保存的密钥"。"""
        patch = dict(patch or {})
        with self.lock:
            items = list(self.data.get("ais") or [])
            ai_id = (patch.get("id") or "").strip()
            existing = None
            for a in items:
                if a.get("id") == ai_id:
                    existing = a
                    break
            if patch.pop("clear_key", False):
                if existing is not None:
                    existing.pop("api_key", None)
                    existing["has_key"] = False
                self.data["ais"] = items
                self._write()
                return existing or {}
            if existing is None:
                ai_id = self._new_id()
                existing = {"id": ai_id, "label": "", "provider": "openai",
                            "base_url": "", "model": "", "api_key": "",
                            "temperature": 0.2}
                items.append(existing)
            existing["id"] = ai_id
            for k in ("label", "provider", "base_url", "model"):
                if patch.get(k) is not None:
                    existing[k] = str(patch.get(k) or "").strip()
            if "temperature" in patch:
                try:
                    existing["temperature"] = max(0.0, min(2.0, float(patch["temperature"])))
                except (TypeError, ValueError):
                    pass
            key = patch.get("api_key")
            if isinstance(key, str) and key.strip() and "…" not in key and "****" not in key:
                existing["api_key"] = key.strip()
                self.data["ignore_env_key"] = False
            if not existing.get("label"):
                existing["label"] = existing.get("model") or ai_id
            self.data["ais"] = items
            self._write()
            return dict(existing)

    def delete_ai(self, ai_id):
        with self.lock:
            items = [a for a in (self.data.get("ais") or []) if a.get("id") != ai_id]
            self.data["ais"] = items
            for scope in ("role_ais", "part_ais"):
                mapping = dict(self.data.get(scope) or {})
                mapping = {k: v for k, v in mapping.items() if v != ai_id}
                self.data[scope] = mapping
            self._write()
            return True

    def clear_key(self, ai_id=None):
        """彻底清除密钥：从内存和磁盘上都删掉这个字段（不是写空串）。"""
        with self.lock:
            if not ai_id or ai_id == "default":
                self.data.pop("api_key", None)
                self.data["api_key"] = ""
                self.data["mode"] = "mock"
                self.data["ignore_env_key"] = True   # 重启后环境变量也不再注入
                self.env_key = False
            else:
                for a in (self.data.get("ais") or []):
                    if a.get("id") == ai_id:
                        a.pop("api_key", None)
                        a["has_key"] = False
            self._write(omit_empty_key=True)
            return True

    def _write(self, omit_empty_key=False):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        data = dict(self.data)
        if omit_empty_key and data.get("api_key") == "":
            data.pop("api_key", None)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def load(self):
        with self.lock:
            try:
                with open(self.path, encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if k in DEFAULT_SETTINGS:
                            self.data[k] = v
                    # 重新填了密钥就恢复"环境变量可用"
                    if (raw.get("api_key") or "").strip():
                        self.data["ignore_env_key"] = False
                    if not isinstance(self.data.get("ais"), list):
                        self.data["ais"] = []
                    for k in ("role_ais", "part_ais"):
                        if not isinstance(self.data.get(k), dict):
                            self.data[k] = {}
            except FileNotFoundError:
                pass
            except Exception:
                pass                      # 配置损坏时退回默认值，不阻塞启动
            # 环境变量兜底：容器/CI 里不想把密钥写盘时用得上
            # （同时认两种命名：AUTFLOW_LLM_URL 与 AUTFLOW_LLM_BASE_URL）
            url_env = os.environ.get("AUTFLOW_LLM_URL") or os.environ.get("AUTFLOW_LLM_BASE_URL")
            key_env = os.environ.get("AUTFLOW_LLM_KEY") or os.environ.get("AUTFLOW_LLM_API_KEY")
            if url_env:
                self.data["base_url"] = url_env
            if key_env and not self.data.get("ignore_env_key"):
                self.data["api_key"] = key_env
                self.env_key = True
            if os.environ.get("AUTFLOW_LLM_MODEL"):
                self.data["model"] = os.environ["AUTFLOW_LLM_MODEL"]
            if os.environ.get("AUTFLOW_LLM_PROVIDER"):
                self.data["provider"] = os.environ["AUTFLOW_LLM_PROVIDER"]
            if os.environ.get("AUTFLOW_LLM_TIMEOUT"):
                try:
                    self.data["timeout"] = int(os.environ["AUTFLOW_LLM_TIMEOUT"])
                except ValueError:
                    pass
            if os.environ.get("AUTFLOW_LLM_MODE"):
                self.data["mode"] = os.environ["AUTFLOW_LLM_MODE"]
            elif url_env and key_env:
                # 环境变量给了端点和密钥，视为"明确要用真实模型"
                self.data["mode"] = "llm"
            return self.data

    def save(self, patch):
        with self.lock:
            for k, v in (patch or {}).items():
                if k not in DEFAULT_SETTINGS or k == "api_key" and v is None:
                    continue
                if k in ("temperature",):
                    try:
                        v = max(0.0, min(2.0, float(v)))
                    except (TypeError, ValueError):
                        continue
                elif k in ("max_tokens", "timeout", "max_steps"):
                    try:
                        v = max(1, int(v))
                    except (TypeError, ValueError):
                        continue
                elif k == "mode":
                    v = "llm" if v == "llm" else "mock"
                elif isinstance(v, str):
                    v = v.strip()
                self.data[k] = v
            if (patch or {}).get("api_key"):
                self.data["ignore_env_key"] = False   # 重新填了密钥：环境变量恢复可用
                self.env_key = False
            self._write()
            return self.data

    # ---------------- 对外视图 ----------------
    def public(self):
        """给前端的配置视图：密钥只有掩码，同时告诉前端"到底能不能用"。"""
        with self.lock:
            d = dict(self.data)
        key = d.pop("api_key", "")
        configured = bool(d.get("base_url")) and bool(key)
        d["has_key"] = bool(key)
        d["key_hint"] = mask_key(key)
        d["key_source"] = "env" if self.env_key else ("file" if key else "")
        d["configured"] = configured
        d["active"] = bool(d.get("mode") == "llm" and configured)
        d["presets"] = PROVIDER_PRESETS
        d["ais"] = [{
            "id": a.get("id"), "label": a.get("label") or a.get("model") or a.get("id"),
            "provider": a.get("provider"), "base_url": a.get("base_url"),
            "model": a.get("model"), "has_key": bool(a.get("api_key")),
            "key_hint": mask_key(a.get("api_key")),
            "temperature": a.get("temperature"),
            "active": bool(a.get("api_key") and a.get("base_url")),
        } for a in (d.get("ais") or [])]
        d["role_ais"] = dict(d.get("role_ais") or {})
        d["part_ais"] = dict(d.get("part_ais") or {})
        d["primary"] = {"has_key": d["has_key"], "model": d.get("model"),
                        "base_url": d.get("base_url"), "key_hint": d["key_hint"]}
        # 任一接入点可用即可开会/跑真实模型
        d["any_active"] = bool(d["active"]) or any(a["active"] for a in d["ais"])
        return d


def _to_openai(messages):
    """协议无关消息 → OpenAI /chat/completions 线格式。"""
    out = []
    for m in messages:
        role = m.get("role")
        if role == "tool":
            out.append({"role": "tool", "tool_call_id": m.get("tool_call_id") or "",
                        "content": m.get("content") or ""})
            continue
        calls = m.get("tool_calls") or []
        if role == "assistant" and calls:
            out.append({"role": "assistant", "content": m.get("content") or "",
                        "tool_calls": [{
                            "id": c.get("id") or ("call_%d" % i),
                            "type": "function",
                            "function": {"name": c.get("name"),
                                         "arguments": json.dumps(c.get("arguments") or {},
                                                                 ensure_ascii=False)},
                        } for i, c in enumerate(calls)]})
            continue
        out.append({"role": role or "user", "content": m.get("content") or ""})
    return out


def _to_anthropic(messages):
    """协议无关消息 → Anthropic /messages 线格式（工具结果要包成 tool_result 块）。"""
    out = []
    for m in messages:
        role = m.get("role")
        if role == "system":
            continue
        if role == "tool":
            block = {"type": "tool_result", "tool_use_id": m.get("tool_call_id") or "",
                     "content": m.get("content") or ""}
            if out and out[-1]["role"] == "user" and isinstance(out[-1]["content"], list):
                out[-1]["content"].append(block)
            else:
                out.append({"role": "user", "content": [block]})
            continue
        calls = m.get("tool_calls") or []
        if role == "assistant" and calls:
            blocks = []
            if m.get("content"):
                blocks.append({"type": "text", "text": m["content"]})
            for c in calls:
                blocks.append({"type": "tool_use", "id": c.get("id") or "toolu_%d" % len(blocks),
                               "name": c.get("name"),
                               "input": c.get("arguments") or {}})
            out.append({"role": "assistant", "content": blocks})
            continue
        text = m.get("content") or ""
        if out and out[-1]["role"] == role and isinstance(out[-1]["content"], str):
            out[-1]["content"] = out[-1]["content"] + "\n" + text
        else:
            out.append({"role": role or "user", "content": text})
    return out


class LLMClient:
    """OpenAI 兼容 + Anthropic 两种协议的统一调用入口。"""

    def __init__(self, settings):
        self.settings = settings

    # ---------------- 内部 ----------------
    def _conf(self, ai_id=None):
        if ai_id:
            return self._conf_from(self.settings.spec(ai_id))
        return self._conf_from(dict(self.settings.data))

    def _conf_from(self, d):
        base = (d.get("base_url") or "").strip().rstrip("/")
        key = (d.get("api_key") or "").strip()
        if not base or not key:
            raise LLMNotConfigured(
                "还没配置模型：点右上角「模型」填入 base_url 与 API Key")
        provider = (d.get("provider") or "openai").strip().lower()
        # 允许用户只填主机名：补上默认路径
        if provider == "anthropic":
            if not base.endswith("/v1"):
                base = base + "/v1"
        else:
            if not base.rstrip("/").split("/")[-1].startswith("v"):
                base = base + "/v1"
        return d, base, key, provider

    def _post(self, url, payload, headers, timeout):
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        attempts = 3
        last = None
        for i in range(attempts):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode("utf-8"))
            except socket.timeout:
                last = LLMError("E-TIMEOUT", "调用模型超时（> %ss）" % timeout)
            except urllib.error.HTTPError as e:
                detail = ""
                try:
                    detail = e.read().decode("utf-8", "replace")[:400]
                except Exception:
                    pass
                if e.code in (429, 500, 502, 503, 504):
                    last = LLMError("E-NETWORK", "模型服务返回 HTTP %d：%s" % (e.code, detail))
                elif e.code in (401, 403):
                    raise LLMError("E-AGENT", "鉴权失败（HTTP %d）：密钥或权限不对" % e.code)
                elif e.code == 404:
                    raise LLMError("E-AGENT", "端点不存在（HTTP 404）：检查 base_url 是否少了 /v1")
                else:
                    raise LLMError("E-AGENT", "模型服务拒绝请求（HTTP %d）：%s" % (e.code, detail))
            except urllib.error.URLError as e:
                last = LLMError("E-NETWORK", "连不上模型服务：%s" % (e.reason,))
            except json.JSONDecodeError as e:
                last = LLMError("E-FORMAT", "模型返回的不是 JSON：%s" % e)
            if i < attempts - 1:
                time.sleep(1.0 + i)       # 限流/抖动退避
        raise last

    # ---------------- 对外 ----------------
    def chat(self, messages, tools=None, force_json=False, timeout=None,
             temperature=None, max_tokens=None, ai_id=None):
        """返回 {content, tool_calls, usage, raw}。tool_calls 结构统一成
        [{'id','name','arguments'(dict)}]，两种协议对上层是同一套。

        消息用一套**协议无关**的格式描述，这里再翻译成各自线格式：
          {"role":"system"|"user"|"assistant","content":str}
          {"role":"assistant","content":str,"tool_calls":[{id,name,arguments}]}
          {"role":"tool","tool_call_id":id,"name":name,"content":str}
        """
        d, base, key, provider = self._conf(ai_id)
        timeout = int(timeout or d.get("timeout") or 180)
        if provider == "anthropic":
            sys_prompt = "\n\n".join(m["content"] for m in messages if m.get("role") == "system")
            conv = _to_anthropic(messages)
            payload = {
                "model": d.get("model"),
                "max_tokens": int(max_tokens or d.get("max_tokens") or 4096),
                "temperature": float(d.get("temperature") if temperature is None
                                     else temperature),
                "messages": conv,
            }
            if sys_prompt:
                payload["system"] = sys_prompt
            if tools:
                payload["tools"] = [{
                    "name": t["name"], "description": t.get("description", ""),
                    "input_schema": t.get("parameters") or {"type": "object", "properties": {}},
                } for t in tools]
            data = self._post(base + "/messages", payload, {
                "Content-Type": "application/json", "x-api-key": key,
                "anthropic-version": "2023-06-01"}, timeout)
            blocks = data.get("content") or []
            text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
            calls = []
            for b in blocks:
                if b.get("type") == "tool_use":
                    calls.append({"id": b.get("id"), "name": b.get("name"),
                                  "arguments": b.get("input") or {}})
            usage = data.get("usage") or {}
            return {"content": text, "tool_calls": calls,
                    "usage": {"in": usage.get("input_tokens", 0),
                              "out": usage.get("output_tokens", 0)}, "raw": data}

        payload = {
            "model": d.get("model"),
            "temperature": float(d.get("temperature") if temperature is None
                                 else temperature),
            "max_tokens": int(max_tokens or d.get("max_tokens") or 4096),
            "messages": _to_openai(messages),
        }
        if tools:
            payload["tools"] = [{"type": "function", "function": t} for t in tools]
        if force_json:
            payload["response_format"] = {"type": "json_object"}
        data = self._post(base + "/chat/completions", payload, {
            "Content-Type": "application/json", "Authorization": "Bearer " + key}, timeout)
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        calls = []
        for c in (msg.get("tool_calls") or []):
            fn = c.get("function") or {}
            args = {}
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except Exception:
                args = {"_raw": fn.get("arguments")}
            calls.append({"id": c.get("id"), "name": fn.get("name"), "arguments": args})
        usage = data.get("usage") or {}
        return {"content": msg.get("content") or "", "tool_calls": calls,
                "reasoning": msg.get("reasoning_content") or "",
                "usage": {"in": usage.get("prompt_tokens", 0),
                          "out": usage.get("completion_tokens", 0)},
                "finish": choice.get("finish_reason"), "raw": data}

    def probe(self, ai_id=None):
        """连通性自检：发一条极短请求，返回耗时与模型回显（可指定某个 AI）。"""
        started = time.time()
        conf = self.settings.spec(ai_id)
        res = self.chat([{"role": "user", "content": "只回复两个字：可用"}],
                        timeout=45, ai_id=ai_id)
        ms = int((time.time() - started) * 1000)
        return {"ok": True, "ms": ms, "ai_id": ai_id or "default",
                "model": conf.get("model"), "provider": conf.get("provider"),
                "reply": (res.get("content") or "").strip()[:80],
                "usage": res.get("usage")}