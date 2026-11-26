# -*- coding: utf-8 -*-
"""多智能体规划：把一份设计规格交给几个角色讨论，讨论结果落成任务清单。

和"让模型直接吐一张流程图"的区别在于分工与约束：

  1. 角色分工 —— 产品经理 / 架构师 / 开发者 / 测试 / 评审，每人只对自己那一段负责；
  2. 两轮讨论 —— 第一轮各自出主张，第二轮互相质疑与修订（不是各说各话）；
  3. 结论收口 —— 主持人只输出**任务清单**，不输出图；图由代码按任务依赖编排
     （`plan_from_tasks`），这样画出来的结构和校验规则仍然完全可控；
  4. 代码兜底 —— 模型给的任务先过 `normalize_tasks`：补字段、修依赖、断环、
     非法值回落到合法值；实在不能用就退回规则分解，绝不让界面拿到半张图。

所有进度都以"发言"为单位抛给上层（`on_minute`），前端可以边讨论边显示。
"""
import json
import re
import time

PHASES = ("设计", "编码", "测试", "评审", "交付")

ROLE_ALIAS = {
    "pm": "产品经理", "产品经理": "产品经理", "product": "产品经理",
    "architect": "架构师", "架构师": "架构师", "arch": "架构师",
    "coder": "开发者", "developer": "开发者", "开发者": "开发者", "后端": "开发者",
    "frontend": "前端", "前端": "前端",
    "tester": "测试", "qa": "测试", "测试": "测试",
    "reviewer": "评审", "review": "评审", "评审": "评审",
}

# 任务角色 → 执行器角色（与 server.TASK_ROLE_TO_AGENT 对齐，这里只保留常用集合）
TASK_ROLES = ("产品经理", "架构师", "后端", "前端", "测试", "评审")
ROLE_TO_AGENT = {"产品经理": "pm", "架构师": "architect", "后端": "coder",
                 "前端": "coder", "测试": "tester", "评审": "reviewer"}

KIND_OF = {"设计": "design", "编码": "implementation", "测试": "verification",
           "评审": "review", "交付": "delivery"}
SCHEMA_OF = {"design": "design-v1", "implementation": "code-result-v1",
             "verification": "test-v1", "review": "verify-v1", "delivery": "verify-v1"}
PRIORITIES = ("P0", "P1", "P2")

DEFAULT_BRIEF = [
    ("pm", "产品经理",
     "你是产品经理。基于规格说明，说明：模块如何切分、优先级排序的理由、"
     "哪些验收条款最关键、规格里哪些地方含糊需要确认。不要写代码。"),
    ("architect", "架构师",
     "你是架构师。说明：整体技术方案、模块之间的接口与数据结构、依赖方向、"
     "最大的技术风险与技术债。不要写完整代码。"),
    ("coder", "开发者",
     "你是开发者。说明：每块功能落在哪些文件、关键实现步骤、实现难点与工作量估算、"
     "需要新增的依赖（尽量为零）。"),
    ("tester", "测试",
     "你是测试工程师。说明：测试策略（单元/接口/回归）、用例矩阵要点、"
     "可自动化的验收方式与测试命令、哪些点难以自动化。"),
    ("reviewer", "评审",
     "你是评审专家。先说结论：这份规格能不能按上面的思路交付；再指出 2-3 个"
     "别人没提到的风险或遗漏（责任、边界、验收盲区）。"),
]


# 群聊成员：key 与 DEFAULT_BRIEF 对齐；focus 是"谁该接这个话头"的判据
GROUP = [
    {"key": "pm", "label": "产品经理", "focus": ["范围", "优先级", "验收", "不做", "需求"]},
    {"key": "architect", "label": "架构师",
     "focus": ["接口", "架构", "数据", "依赖", "风险", "技术"]},
    {"key": "coder", "label": "开发者", "focus": ["文件", "实现", "工作量", "难点", "代码"]},
    {"key": "tester", "label": "测试", "focus": ["用例", "测试", "回归", "可验证", "自动化"]},
    {"key": "reviewer", "label": "评审", "focus": ["遗漏", "阻塞", "异议", "风险", "交付"]},
]
ROLE_BY_KEY = {g["key"]: g for g in GROUP}
STANCES = ("agree", "revise", "block")
STANCE_LABEL = {"agree": "同意", "revise": "要改", "block": "有阻塞"}
CONSENSUS_LABEL = {"reached": "已达成共识", "adopted": "人工拍板采纳",
                   "partial": "部分同意，仍有分歧", "blocked": "有人反对（阻塞）",
                   "none": "未表决"}


class ChatManager:
    """群聊主持人（对应 AutoGen 的 GroupChatManager）：不产出内容，只做调度。

    三件事：
      select()   —— 下一句谁说：有人工指令先让对口的角色接；有未决异议让负责人回应；
                    否则按"还没表态 + 规格关注点"挑人（不是固定的 5 人轮转）；
      vote()     —— 收口前让每个成员对当前方案表态（同意 / 要改 / 有阻塞）；
      conclude() —— 按表决判定共识，没达成时给出还要讨论什么。
    """

    def __init__(self, client, max_rounds=3, turn_cap=18):
        self.client = client
        self.max_rounds = max_rounds
        self.turn_cap = turn_cap

    def _match_role(self, text, default="pm"):
        text = text or ""
        best, score = None, 0
        for g in GROUP:
            sc = sum(1 for w in g["focus"] if w and w in text)
            if sc > score:
                best, score = g["key"], sc
        return best or default

    def _topic_order(self, spec, candidates):
        body = ((spec.get("body") or "") + " " +
                " ".join(spec.get("constraints") or []))
        bonus = {g["key"]: sum(1 for w in g["focus"] if w in body) for g in candidates}
        idx = {g["key"]: i for i, g in enumerate(GROUP)}
        return sorted(candidates, key=lambda g: (-bonus[g["key"]], idx[g["key"]]))

    def select(self, session, spec):
        """返回 (role_key, 理由)。role_key=None 表示本轮该投票了。"""
        if len(session["minutes"]) >= self.turn_cap:
            return None, "达到发言上限（%d 条）" % self.turn_cap
        pending = [h for h in session.get("humans", []) if not h.get("addressed")]
        if pending:
            human = pending[-1]
            key = human.get("to") or self._match_role(human["text"])
            return key, "回应人工指令：%s" % human["text"][:36]
        open_obj = [o for o in session.get("objections", []) if not o.get("answered")]
        if open_obj:
            obj = open_obj[0]
            return obj.get("owner") or self._match_role(obj["text"]),                 "回应未决异议：%s" % obj["text"][:36]
        counts = {g["key"]: 0 for g in GROUP}
        for m in session["minutes"]:
            if m.get("role") in counts and m.get("kind") != "human":
                counts[m["role"]] += 1
        silent = [g for g in GROUP if counts[g["key"]] == 0]
        if not silent:
            return None, "全员都已发言，转入表决"
        key = self._topic_order(spec, silent)[0]["key"]
        return key, "尚未表态，按规格关注点优先请他先说"

    def vote(self, session, spec, round_no):
        """每个成员对「当前方案」表态：同意 / 要改 / 有阻塞。"""
        votes = []
        for g in GROUP:
            if not session.get("_alive", lambda: True)():
                break
            system = (
                "你是" + g["label"] + "。现在对「按讨论形成的任务方案」表态。"
                + _role_brief(g["key"])
                + " 只输出 JSON：{\"stance\":\"agree|revise|block\",\"reason\":\"一句理由\"}。"
                + " agree=可以按这个做；revise=方向可以但要改某处；block=有阻塞不能这么做。"
            )
            obj = _chat_json(self.client, session, system,
                             ai_id=_ai_of(session, g["key"]) or None) or {}
            stance = str(obj.get("stance") or "").strip().lower()
            if stance not in STANCES:
                stance = "revise"
            votes.append({"role": g["key"], "label": g["label"], "stance": stance,
                          "reason": str(obj.get("reason") or "")[:200]})
        with session["_lock"]:
            session["votes"].append({"round": round_no, "votes": votes})
        return votes

    def conclude(self, session):
        """按最近一轮表决判定共识。"""
        rounds = session.get("votes") or []
        votes = rounds[-1]["votes"] if rounds else []
        if not votes:
            return {"status": "none", "unresolved": [], "agree": [], "block": [], "revise": []}
        agree = [v["label"] for v in votes if v["stance"] == "agree"]
        revise = [v for v in votes if v["stance"] == "revise"]
        block = [v for v in votes if v["stance"] == "block"]
        if block:
            status = "blocked"
            unresolved = ["%s：%s" % (v["label"], v["reason"] or "有阻塞") for v in block]
        elif len(agree) >= 4 and len(revise) <= 1:
            status = "reached"
            unresolved = []
        elif len(agree) == 5:
            status = "reached"
            unresolved = []
        else:
            status = "partial"
            unresolved = ["%s：%s" % (v["label"], v["reason"] or "要改") for v in revise]
        return {"status": status, "agree": agree,
                "revise": [v["label"] for v in revise],
                "block": [v["label"] for v in block],
                "unresolved": unresolved[:6],
                "round": rounds[-1]["round"] if rounds else 0}


def _role_brief(key):
    for k, label, brief in DEFAULT_BRIEF:
        if k == key:
            return brief
    return ""


def _ai_of(session, role):
    """这个角色被指派到哪个 AI（空串 = 默认）。"""
    return ((session.get("role_ais") or {}).get(role) or "").strip()


def _ai_label(settings, ai_id):
    """给界面看的人类可读名字（模型名或用户起的名字）。"""
    if not ai_id:
        return ""
    try:
        spec = settings.spec(ai_id)
    except Exception:
        return ai_id
    return (spec.get("_ai_label") or spec.get("model") or ai_id)


def _chat_json(client, session, system, user=None, max_tokens=700, temperature=0.2,
               ai_id=None):
    """要一段 JSON：失败重试一次，仍失败返回 None（调用方有兜底）。"""
    messages = [{"role": "system", "content": system}]
    if user is not None:
        messages.append({"role": "user", "content": user})
    reply = client.chat(messages, temperature=temperature, force_json=True,
                        max_tokens=max_tokens, ai_id=ai_id)
    usage = reply.get("usage")
    _bump(session, usage)
    obj = _as_json_object(reply.get("content") or "")
    if obj is None:
        messages.append({"role": "assistant", "content": (reply.get("content") or "")[:1500]})
        messages.append({"role": "user",
                         "content": "上面不是合法 JSON。请只输出 JSON 对象，不要任何解释。"})
        reply = client.chat(messages, temperature=0.1, force_json=True,
                            max_tokens=max_tokens, ai_id=ai_id)
        _bump(session, reply.get("usage"))
        obj = _as_json_object(reply.get("content") or "")
    return obj


def _bump(session, usage):
    if not isinstance(usage, dict):
        return
    with session["_lock"]:
        for target, keys in (("prompt_tokens", ("prompt_tokens", "in")),
                             ("completion_tokens", ("completion_tokens", "out"))):
            for k in keys:
                v = usage.get(k)
                if isinstance(v, int):
                    session["usage"][target] = session["usage"].get(target, 0) + v
                    break


def _now():
    return time.strftime("%H:%M:%S")


def _ctx_brief(spec, template):
    """把规格压成一段供讨论用的上下文（控制 token，但保留判断所需的信息）。"""
    mods = spec.get("modules") or []
    lines = []
    for i, m in enumerate(mods, 1):
        goals = m.get("goal") or m.get("summary") or ""
        acc = [str(a) for a in (m.get("acceptance") or [])][:4]
        lines.append("%d) %s%s（优先级 %s）%s" % (
            i, m.get("name") or ("模块%d" % i),
            ("：" + goals[:120]) if goals else "",
            m.get("priority") or "P1",
            ("；验收：" + "；".join(acc)) if acc else ""))
    cons = [str(c) for c in (spec.get("constraints") or [])][:6]
    body = (spec.get("body") or "").strip()
    return "\n".join([
        "规格名称：%s" % (spec.get("name") or "未命名"),
        "摘要：%s" % (spec.get("summary") or "（无）"),
        "模块：",
        "\n".join(lines) or "（规格里尚未填写模块）",
        "全局约束：" + ("；".join(cons) if cons else "（无）"),
        "流程模板：%s" % template,
        "正文节选：\n" + (body[:1200] if body else "（无）"),
    ])


def _parse_speech(text):
    """把一次发言整理成 {content, points[]}：能解析出列表就结构化，否则原样保留。"""
    raw = (text or "").strip()
    if not raw:
        return {"content": "", "points": []}
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                content = str(obj.get("content") or obj.get("reply") or obj.get("summary") or "")
                points = obj.get("points") or obj.get("key_points") or []
                if isinstance(points, str):
                    points = [points]
                return {"content": content.strip(),
                        "points": [str(p).strip() for p in points if str(p).strip()][:5]}
        except Exception:
            pass
    points = []
    keep = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or re.fullmatch(r"要点[:：]?", s):
            continue
        m = re.match(r"^(?:[-*•]|\d+[.、)])\s*(.+)$", s)
        if m:
            item = m.group(1).strip()
            if len(item) > 1:
                points.append(item[:160])
            continue
        keep.append(s)
    content = " ".join(keep)[:600] or (points[0] if points else "")
    return {"content": content, "points": points[:5]}


def _as_json_object(text):
    """从模型输出里抠出第一个完整的 JSON 对象（容忍 ```json 包裹与前后废话）。"""
    if not text:
        return None
    s = text.strip()
    s = re.sub(r"^```(?:json|JSON)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    for cand in (s, s[s.find("{"):s.rfind("}") + 1] if "{" in s and "}" in s else ""):
        if not cand:
            continue
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict):
                return obj
            if isinstance(obj, list):
                return {"tasks": obj}
        except Exception:
            continue
    return None


def _str_list(value, limit=8, each=200):
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        s = str(item).strip()
        if s:
            out.append(s[:each])
        if len(out) >= limit:
            break
    return out


def _safe_files(value):
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        s = str(item).strip().replace("\\", "/")
        if not s or s.startswith("/") or ".." in s.split("/") or re.match(r"^[A-Za-z]:", s):
            continue
        out.append(s[:120])
        if len(out) >= 6:
            break
    return out


PART_IDS = ("skeleton", "data", "api", "feature", "test", "review",
            "integrate", "delivery")
PART_ALIAS = {"骨架": "skeleton", "基础设施": "skeleton", "设计": "skeleton",
              "数据": "data", "数据模型": "data", "迁移": "data",
              "接口": "api", "接口实现": "api", "api": "api",
              "功能": "feature", "功能实现": "feature", "编码": "feature",
              "测试": "test", "验证": "test",
              "评审": "review", "代码评审": "review",
              "集成": "integrate", "联调": "integrate", "集成验收": "integrate",
              "交付": "delivery", "文档": "delivery"}


def _norm_part(value, phase, kind, title):
    """任务分组：模型写的 part 优先，其次按阶段/标题细化。"""
    raw_part = str(value or "").strip().lower()
    if raw_part in PART_IDS:
        return raw_part
    if raw_part in PART_ALIAS:
        return PART_ALIAS[raw_part]
    text = title or ""
    if "数据模型" in text or "迁移" in text:
        return "data"
    if "接口" in text:
        return "api"
    return {"设计": "skeleton", "测试": "test", "评审": "review",
            "交付": "delivery"}.get(phase, "feature")


def _norm_phase(value, role_hint, title):
    text = "%s %s" % (value or "", title or "")
    for phase in PHASES:
        if phase in text:
            return phase
    if role_hint in ("测试", "tester"):
        return "测试"
    if role_hint in ("评审", "reviewer"):
        return "评审"
    if "交付" in (title or "") or "上线" in (title or ""):
        return "交付"
    if "设计" in (title or "") or "方案" in (title or ""):
        return "设计"
    return "编码"


def _norm_steps(value, title):
    steps = []
    if isinstance(value, list):
        for i, item in enumerate(value, 1):
            if isinstance(item, dict):
                t = str(item.get("title") or item.get("name") or "").strip()
                how = str(item.get("how") or item.get("detail") or "").strip()
                if t or how:
                    steps.append({"title": (t or ("步骤%d" % i))[:60], "how": how[:200]})
            else:
                s = str(item).strip()
                if s:
                    steps.append({"title": s[:60], "how": ""})
    elif isinstance(value, str) and value.strip():
        steps.append({"title": value.strip()[:60], "how": ""})
    return steps[:4]


def normalize_tasks(raw, spec, template="default"):
    """把模型给的任务清单修成系统能用的形状。

    返回 (tasks, fixes)：`tasks` 为空表示模型没给出可用结果，调用方应回退到规则分解。
    `fixes` 是给人看的修正记录（补了什么字段、丢了哪条坏依赖、断了哪个环）。
    """
    items = raw.get("tasks") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        return [], ["模型没有返回 tasks 数组"]
    fixes = []
    picked = []
    for item in items[:40]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("name") or "").strip()
        if not title:
            fixes.append("丢弃 1 条没有标题的任务")
            continue
        picked.append((str(item.get("id") or item.get("key") or "T%d" % (len(picked) + 1)).strip(),
                       title, item))
    if not picked:
        return [], fixes or ["模型返回的任务清单为空"]

    # 第一遍：编号 + 角色/阶段/字段归一
    idmap, tasks = {}, []
    for idx, (key, title, item) in enumerate(picked, 1):
        tid = "T%d" % idx
        idmap[key] = tid
        role_raw = str(item.get("role") or item.get("owner") or "").strip()
        role = ROLE_ALIAS.get(role_raw.lower(), ROLE_ALIAS.get(role_raw, ""))
        phase = _norm_phase(item.get("phase"), role_raw, title)
        if not role:
            role = {"设计": "架构师", "测试": "测试", "评审": "评审",
                    "交付": "产品经理"}.get(phase, "后端")
            fixes.append("%s 未写角色，按「%s」阶段定为 %s" % (tid, phase, role))
        if role not in TASK_ROLES:
            role = "后端"
        kind = KIND_OF.get(phase, "implementation")
        priority = str(item.get("priority") or "P1").upper()
        if priority not in PRIORITIES:
            priority = "P1"
        try:
            estimate = max(1, min(20, int(item.get("estimate") or 2)))
        except Exception:
            estimate = 2
        what = str(item.get("what") or "").strip()
        how = str(item.get("how") or "").strip()
        acceptance = _str_list(item.get("acceptance"), limit=8)
        if not what:
            what = title + ("（验收：%s）" % "；".join(acceptance[:2]) if acceptance else "")
            fixes.append("%s 补了「做什么」" % tid)
        if not how:
            files_hint = "、".join(_safe_files(item.get("files"))[:2])
            how = "按规格实现%s；完成后自测并补充测试" % (
                ("，落在 " + files_hint) if files_hint else "")
            fixes.append("%s 补了「怎么做」" % tid)
        steps = _norm_steps(item.get("steps"), title)
        if not steps:
            steps = [{"title": "实现", "how": how[:120]},
                     {"title": "自测", "how": "运行测试命令，确认全部通过"}]
            fixes.append("%s 补了子步骤" % tid)
        deps_raw = item.get("depends_on") or item.get("deps") or []
        tasks.append({
            "id": tid, "title": title[:80], "phase": phase, "phase_id": phase,
            "role": role, "agent": ROLE_TO_AGENT.get(role, "coder"),
            "kind": kind, "out_schema": SCHEMA_OF.get(kind, "code-result-v1"),
            "priority": priority, "estimate": estimate,
            "what": what[:300], "how": how[:300],
            "files": _safe_files(item.get("files")),
            "steps": steps, "acceptance": acceptance or ["按规格完成「%s」" % title[:40]],
            "module": item.get("module") or "",
            "part": _norm_part(item.get("part"), phase, kind, title),
            "ai": str(item.get("ai") or "").strip()[:40],
            "_deps_raw": deps_raw if isinstance(deps_raw, list) else [],
            "source": "ai-discussion",
        })

    # 第二遍：依赖重映射 + 去自环 + 去环
    for t in tasks:
        deps = []
        for d in t.pop("_deps_raw")[:6]:
            key = str(d).strip()
            mapped = idmap.get(key) or (key if key in {x["id"] for x in tasks} else None)
            if mapped and mapped != t["id"] and mapped not in deps:
                deps.append(mapped)
            elif mapped == t["id"]:
                fixes.append("%s 依赖了自己，已丢弃" % t["id"])
            elif mapped is None:
                fixes.append("%s 的依赖「%s」不存在，已丢弃" % (t["id"], key))
        t["depends_on"] = deps

    # 断环：按编号顺序，若加入边会回到自己就丢掉这条边
    order = {t["id"]: i for i, t in enumerate(tasks)}
    for t in tasks:
        keep = []
        for d in t["depends_on"]:
            if order.get(d, 99) < order.get(t["id"], 99):
                keep.append(d)
            else:
                fixes.append("%s→%s 会造成循环依赖，已断开" % (d, t["id"]))
        t["depends_on"] = keep

    if not any(t["kind"] == "implementation" for t in tasks):
        fixes.append("模型没给编码任务，已按规格补占位任务")
        tasks.append({
            "id": "T%d" % (len(tasks) + 1), "title": "按规格完成功能实现",
            "phase": "编码", "phase_id": "编码", "role": "后端", "agent": "coder",
            "kind": "implementation", "out_schema": "code-result-v1",
            "priority": "P0", "estimate": 5,
            "what": "实现规格中各模块的功能", "how": "按模块拆分文件实现，写完自测",
            "files": [], "steps": [{"title": "实现", "how": "按模块写代码"},
                                   {"title": "自测", "how": "运行测试"}],
            "acceptance": ["规格中的验收条款全部满足"], "module": "",
            "depends_on": [], "source": "ai-discussion",
        })
    if not any(t["kind"] in ("verification", "review") for t in tasks):
        fixes.append("模型没给测试/评审任务，已补")
        tasks.append({
            "id": "T%d" % (len(tasks) + 1), "title": "测试与评审",
            "phase": "测试", "phase_id": "测试", "role": "测试", "agent": "tester",
            "kind": "verification", "out_schema": "test-v1",
            "priority": "P0", "estimate": 2,
            "what": "对实现跑测试并评审", "how": "运行测试命令，检查验收条款",
            "files": [], "steps": [{"title": "跑测试", "how": "执行测试命令"}],
            "acceptance": ["测试全部通过"], "module": "",
            "depends_on": [t["id"] for t in tasks if t["kind"] == "implementation"][:3],
            "source": "ai-discussion",
        })
    return tasks, fixes


class PlanManager:
    """讨论会话管理：一个会话一个线程，前端轮询进度。

    没有引入队列/WebSocket：讨论是一次性、时长以分钟计的任务，轮询 700ms 足够，
    而且不会影响现有运行事件通道（SSE 只服务运行状态，不该被规划流量污染）。
    """

    def __init__(self, client, settings, emitter=None):
        self.client = client
        self.settings = settings
        self.emitter = emitter or (lambda *a, **k: None)
        self.sessions = {}
        self.lock = __import__("threading").RLock()

    # ------------------------------------------------------------------ 会话
    def start(self, spec, template="default", options=None):
        sid = "PL-%d" % int(time.time() * 1000)
        session = {
            "id": sid, "spec_id": spec.get("id"), "spec_name": spec.get("name"),
            "template": template, "status": "running", "stage": "准备",
            "started_at": time.time(), "minutes": [], "tasks": [], "trace": [],
            "fixes": [], "error": None, "fallback": False, "usage": {},
            "spec_revision": spec.get("revision"),
            # 群聊状态：表决 / 人工指令 / 未决异议 / 主持人日志 / 共识
            "votes": [], "humans": [], "objections": [], "manager_log": [],
            "consensus": {"status": "none", "unresolved": [], "agree": [],
                          "revise": [], "block": []},
            "round": 1, "human_decision": None,
            # 每个讨论角色可以用不同的 AI：{"pm": "ai-1", ...}
            "role_ais": dict(((options or {}).get("role_ais") or {})),
            "_spec": spec, "_lock": self.lock,
            "_alive": (lambda: self._alive(session)),
        }
        with self.lock:
            self.sessions[sid] = session
        t = __import__("threading").Thread(target=self._run, args=(session, spec),
                                           name="plan-%s" % sid, daemon=True)
        t.start()
        return session

    def get(self, sid):
        with self.lock:
            s = self.sessions.get(sid)
            if not s:
                return None
            # 内部字段（锁、存活判断、原始规格）不对外，也不能被 JSON 序列化
            safe = {k: v for k, v in s.items() if not k.startswith("_")}
            return json.loads(json.dumps(safe, default=str))

    def cancel(self, sid):
        with self.lock:
            s = self.sessions.get(sid)
            if not s:
                return False
            s["cancelled"] = True
            if s["status"] == "running":
                s["status"] = "cancelled"
                s["stage"] = "已取消"
            return True

    def _max_tokens(self, default):
        # SettingsStore 用 dict 存配置，这里给个带兜底的读取
        try:
            return int(self.settings.data.get("max_tokens") or default)
        except Exception:
            return default

    def _alive(self, session):
        return not session.get("cancelled")

    def _push(self, session, item):
        with self.lock:
            session["minutes"].append(item)

    # ------------------------------------------------------------------ 群聊
    def _say(self, session, spec, role, round_no, reason, extra=""):
        """一个成员发言：看到全部纪要 + 人工指令，输出结论 + 要点 + 立场。"""
        label = ROLE_BY_KEY[role]["label"]
        brief = _role_brief(role)
        # 只有"这次发言真的看过"的人工指令才算被回应：
        # 取快照，避免发言途中新插入的指令被这条（其实没看到的）发言标成已回应
        humans = [h for h in session.get("humans", []) if not h.get("addressed")]
        human_note = ""
        if humans:
            human_note = ("\n\n【人工指令，必须优先回应】\n%s\n"
                          "请直接在发言里说明：这条要求你怎么落实、和原方案哪里不同。"
                          % "\n".join("· " + h["text"][:200] for h in humans[-2:]))
        near = session["minutes"][-8:]
        transcript = "\n".join("%s%s：%s" % (
            "【人工】" if m.get("kind") == "human" else "", m["label"],
            m["content"][:220]) for m in near) or "（你是第一个发言）"
        messages = [
            {"role": "system", "content":
             "你正在参加一场多智能体评审会（群聊）。%s\n"
             "规则：只讲你负责的那一段，中文，不超过 160 字；先给一句结论，"
             "再用 2-3 条「- 」开头的要点；不要重复别人说过的；不要输出 JSON。" % brief},
            {"role": "user", "content":
             "%s\n\n群聊纪要（最近 8 条）：\n%s%s\n\n"
             "主持人请你发言（第 %d 轮，原因：%s）。%s"
             % (_ctx_brief(spec, session["template"]), transcript, human_note,
                round_no, reason, extra)},
        ]
        ai_id = _ai_of(session, role)
        reply = self.client.chat(messages, temperature=0.4,
                                 max_tokens=min(900, self._max_tokens(900)),
                                 ai_id=ai_id or None)
        speech = _parse_speech(reply.get("content") or "")
        _bump(session, reply.get("usage"))
        item = {"role": role, "label": label, "round": round_no,
                "content": speech["content"], "points": speech["points"],
                "at": _now(), "why": reason,
                "ai": ai_id, "ai_label": _ai_label(self.settings, ai_id)}
        self._push(session, item)
        with self.lock:
            session["manager_log"].append({"round": round_no, "role": role,
                                           "label": label, "why": reason,
                                           "at": item["at"]})
            for h in humans:
                if (h.get("to") or "") in ("", role):
                    h["addressed"] = True
            for o in session["objections"]:
                if not o.get("answered") and o.get("owner") == role:
                    o["answered"] = True
        return item

    def say(self, sid, text, to=""):
        """人插话：讨论中排队（主持人下一位就回应）；已结束则续议。"""
        import threading
        text = (text or "").strip()
        if not text:
            return {"ok": False, "message": "内容为空"}
        with self.lock:
            s = self.sessions.get(sid)
            if not s:
                return {"ok": False, "message": "讨论会话不存在"}
            entry = {"text": text[:500], "to": to or "", "at": _now(),
                     "addressed": False}
            s["humans"].append(entry)
            s["minutes"].append({"role": "human", "label": "人工", "round": s.get("round", 1),
                                 "content": text[:500], "points": [], "at": entry["at"],
                                 "kind": "human"})
            running = s["status"] == "running"
            if not running:
                s.setdefault("_alive", lambda: True)
                s["status"] = "running"
                s["stage"] = "人工介入 · 重新讨论"
                s["error"] = None
        if not running:
            t = threading.Thread(target=self._resume, args=(s, s.get("_spec")),
                                 name="plan-resume-%s" % sid, daemon=True)
            t.start()
        return {"ok": True, "session_id": sid, "running": True,
                "message": "已加入讨论，主持人会在下一位让对口的角色回应" if running
                           else "已重新开始讨论"}

    def decide(self, sid, action, note=""):
        """共识没达成时由人拍板：accept=按当前清单生成；resume=继续讨论。"""
        with self.lock:
            s = self.sessions.get(sid)
            if not s:
                return {"ok": False, "message": "讨论会话不存在"}
            if action == "accept":
                s["human_decision"] = {"action": "accept", "note": note[:200],
                                      "at": _now()}
                s["consensus"]["status"] = "adopted"
                s["stage"] = "人工拍板 · 按当前清单生成"
                return {"ok": True, "message": "已采纳当前清单"}
        if action == "resume":
            return self.say(sid, note or "继续讨论未解决的分歧，给出可执行的结论")
        return {"ok": False, "message": "未知操作"}

    def _run(self, session, spec):
        try:
            self._discuss(session, spec)
        except Exception as exc:      # 网络、鉴权、格式……都收敛成一个可读错误
            with self.lock:
                session["status"] = "error"
                session["stage"] = "讨论失败"
                session["error"] = {"code": getattr(exc, "code", "E-AGENT"),
                                    "message": str(exc)}
            self.emitter("plan.failed", {"session": session["id"],
                                         "message": str(exc)})

    def _discuss(self, session, spec):
        manager = ChatManager(self.client)
        with self.lock:
            session["stage"] = "第 1 轮 · 群体讨论"
            session["round"] = 1
        for round_no in range(1, manager.max_rounds + 1):
            if not self._alive(session):
                return
            with self.lock:
                session["round"] = round_no
                session["stage"] = ("第 %d 轮 · 群体讨论" % round_no if round_no == 1
                                    else "第 %d 轮 · 收敛异议" % round_no)
            while self._alive(session):
                role, reason = manager.select(session, spec)
                if role is None:
                    with self.lock:
                        session["manager_log"].append({"round": round_no, "role": "",
                                                       "label": "主持人", "why": reason,
                                                       "at": _now()})
                    break
                extra = ""
                if round_no > 1 and session.get("consensus", {}).get("unresolved"):
                    extra = ("上一轮没达成共识，分歧是：%s。请针对这些分歧给结论，不要重复方案。"
                             % "；".join(session["consensus"]["unresolved"][:4]))
                self._say(session, spec, role, round_no, reason, extra)
            if not self._alive(session):
                return
            with self.lock:
                session["stage"] = "第 %d 轮 · 表决" % round_no
            manager.vote(session, spec, round_no)
            verdict = manager.conclude(session)
            with self.lock:
                session["consensus"] = verdict
                pending = [h for h in session["humans"] if not h.get("addressed")]
                if verdict["status"] == "reached" and not pending:
                    break
                session["objections"] = [{"text": x, "answered": False,
                                          "owner": self._owner_of(x)}
                                         for x in verdict["unresolved"]]
            if not self._alive(session):
                return
            if pending and round_no >= manager.max_rounds:
                # 轮次用完了但人还没被回应：单独补一轮回应人工，不再硬凑共识
                with self.lock:
                    session["stage"] = "回应人工指令"
                for _ in range(2):
                    role, reason = manager.select(session, spec)
                    if role is None:
                        break
                    self._say(session, spec, role, round_no, reason)
                manager.vote(session, spec, round_no)
                with self.lock:
                    session["consensus"] = manager.conclude(session)
                break
        self._finish(session, spec, manager)

    def _owner_of(self, text):
        best, score = "pm", 0
        for g in GROUP:
            sc = sum(1 for w in g["focus"] if w in (text or ""))
            if sc > score:
                best, score = g["key"], sc
        return best

    def _finish(self, session, spec, manager):
        with self.lock:
            session["stage"] = "汇总 · 生成任务清单"
        raw = self._decide(session, spec)
        tasks, fixes = normalize_tasks(raw, spec, session["template"])
        consensus = session.get("consensus") or {"status": "none", "unresolved": []}
        if not tasks:
            with self.lock:
                session["fallback"] = True
                session["stage"] = "模型没有给出可用任务"
                session["status"] = "error"
                session["error"] = {"code": "E-FORMAT",
                                    "message": "模型没有返回可用的任务清单（可改用「分解预览」走离线规则分解）"}
                session["fixes"] = fixes or ["模型输出不可用"]
                session["finished_at"] = time.time()
            self.emitter("plan.failed", {"session": session["id"],
                                         "message": "模型没有给出可用任务"})
            return
        rounds = len(session.get("votes") or [])
        with self.lock:
            session["tasks"] = tasks
            session["fixes"] = fixes
            session["status"] = "done"
            session["stage"] = ("完成 · 已达成共识" if consensus["status"] in
                                ("reached", "adopted") else "完成 · 未完全达成共识")
            session["finished_at"] = time.time()
            session["trace"] = [
                "群体讨论：%d 轮 / %d 条发言（%d 次表决）"
                % (rounds, len([m for m in session["minutes"] if m.get("kind") != "human"]),
                   rounds),
                "共识：%s%s" % (CONSENSUS_LABEL.get(consensus["status"], consensus["status"]),
                              "（同意 %d 人）" % len(consensus.get("agree") or [])),
                "人工介入：%d 次" % len(session.get("humans") or []),
                "产出任务 %d 条（模型提案 + 代码归一）" % len(tasks),
                "阶段分布：" + "、".join("%s %d" % (p, sum(1 for t in tasks if t["phase"] == p))
                                       for p in PHASES
                                       if any(t["phase"] == p for t in tasks)),
            ] + (["未解决：" + x for x in consensus.get("unresolved") or []][:4]) \
              + ["修正：" + f for f in fixes[:6]]
        self.emitter("plan.done", {"session": session["id"], "tasks": len(tasks)})

    def _resume(self, session, spec):
        """人工介入后的续议：先回应人，再重新表决并重出清单。"""
        try:
            if not spec:
                raise RuntimeError("原始规格已不存在，无法续议")
            manager = ChatManager(self.client)
            round_no = int(session.get("round") or 1) + 1
            with self.lock:
                session["round"] = round_no
                session["stage"] = "第 %d 轮 · 回应人工指令" % round_no
            for _ in range(3):
                if not self._alive(session):
                    return
                role, reason = manager.select(session, spec)
                if role is None:
                    break
                self._say(session, spec, role, round_no, reason)
            with self.lock:
                session["stage"] = "第 %d 轮 · 表决" % round_no
            manager.vote(session, spec, round_no)
            with self.lock:
                session["consensus"] = manager.conclude(session)
                session["objections"] = [{"text": x, "answered": False,
                                          "owner": self._owner_of(x)}
                                         for x in session["consensus"]["unresolved"]]
            self._finish(session, spec, manager)
        except Exception as exc:
            with self.lock:
                session["status"] = "error"
                session["stage"] = "续议失败"
                session["error"] = {"code": getattr(exc, "code", "E-AGENT"),
                                    "message": str(exc)}
            self.emitter("plan.failed", {"session": session["id"], "message": str(exc)})

    def _decide(self, session, spec):
        sys_prompt = (
            "你是这场评审会的主持人。把讨论结果写成**任务清单 JSON**，供编排器生成工作流。\n"
            "只输出一个 JSON 对象，不要解释、不要 Markdown、不要代码块围栏。\n"
            "格式：{\"tasks\":[{\"id\":\"T1\",\"title\":\"…\",\"phase\":\"设计|编码|测试|评审|交付\","
            "\"role\":\"产品经理|架构师|后端|前端|测试|评审\",\"priority\":\"P0|P1|P2\","
            "\"estimate\":3,\"what\":\"做什么（含验收口径）\",\"how\":\"怎么做（用什么实现、写哪个文件、跑什么命令）\","
            "\"files\":[\"src/x.py\"],\"steps\":[{\"title\":\"步骤\",\"how\":\"做法\"}],"
            "\"acceptance\":[\"可验证的验收条款\"],\"depends_on\":[\"T1\"]}]}\n"
            "要求：\n"
            "1. 5-14 条任务；必须有工程骨架、每个模块的实现、测试、评审、集成验收；\n"
            "2. what 和 how 必须具体（写清文件与做法），不要写「按需实现」这类空话；\n"
            "3. depends_on 只能引用本清单里的 id，不能自环，不能有环；\n"
            "4. 阶段只用 设计/编码/测试/评审/交付 五个值。")
        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content":
             "%s\n\n讨论纪要：\n%s\n\n请输出任务清单 JSON。"
             % (_ctx_brief(spec, session["template"]),
                "\n".join("%s（第%d轮）：%s" % (m["label"], m["round"], m["content"])
                          for m in session["minutes"]))},
        ]
        reply = self.client.chat(messages, temperature=0.2, force_json=True,
                                 max_tokens=min(3000, self._max_tokens(3000)))
        _bump(session, reply.get("usage"))
        obj = _as_json_object(reply.get("content") or "")
        if obj is None:
            # 一次重试：有些模型第一次会带解释，按"只要 JSON"再要一次
            messages.append({"role": "assistant", "content": (reply.get("content") or "")[:2000]})
            messages.append({"role": "user",
                             "content": "上面不是合法 JSON。请只输出 JSON 对象，别加任何说明。"})
            reply = self.client.chat(messages, temperature=0.1, force_json=True,
                                     max_tokens=min(3000, self._max_tokens(3000)))
            _bump(session, reply.get("usage"))
            obj = _as_json_object(reply.get("content") or "")
        return obj or {"tasks": []}
