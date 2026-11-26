"""设计规格文档导入：把 Markdown / 纯文本 / JSON 规格解析成可编辑的规格结构。

支持三种常见写法（都按"缺什么补什么"处理，绝不静默丢内容）：
  1. `## 模块` / `## Modules` 段落，下面用 `### 名称` 分模块；
  2. 直接一堆 `###`/`##` 标题，每个标题当一个模块；
  3. 项目符号列表 `- 登录：账号密码 + 短信验证码`。

验收条款从模块内的 `验收`/`AC-` 行抽取，全局约束从 `约束`/`非功能`/`Constraints` 段抽取。
解析结果会带一份 trace，前端直接展示"我从你的文档里读到了什么、哪几条没读出来"。
"""
import json
import re

MAX_BODY = 40000

PRIORITY_RE = re.compile(r"\bP([0-2])\b", re.I)
FR_RE = re.compile(r"\bFR[-_ ]?(\d{1,3})\b", re.I)
AC_RE = re.compile(r"\bAC[-_ ]?(\d{1,3})\b", re.I)
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*\S)\s*$")
BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+(.+)$")

MODULE_WORDS = ("模块", "module", "modules", "功能", "子系统", "服务", "组件")
CONSTRAINT_WORDS = ("约束", "非功能", "constraint", "quality", "限制", "要求")
ACCEPT_WORDS = ("验收", "acceptance", "ac ", "ac-", "测试点", "检查项")
OUT_OF_SCOPE_WORDS = ("范围外", "不做的", "不包括", "out of scope", "non-goal", "非目标")
FEATURE_RE = re.compile(r"^\s*(?:[-*+]\s*)?(F\d{1,3})\s*[.、:：]?\s*(.+)$")
MODULE_ROW_RE = re.compile(r"^\s*(M\d{1,3})\s*[.、:：]?\s*(.+)$")
TABLE_ROW_RE = re.compile(r"^\s*\|(.+)\|\s*$")


def _clean_heading(text):
    """去掉 "1.2 模块三：登录" 这类编号，留下真正的名字。"""
    text = text.strip()
    text = re.sub(r"^[\d.、)（(一二三四五六七八九十]+\s*[、.)）]?\s*", "", text)
    text = re.sub(r"^(模块|子模块|module)\s*[A-Za-z0-9一二三四五六七八九十]*\s*[:：]\s*", "",
                  text, flags=re.I)
    return text.strip(" :：-") or text.strip()


def _priority_of(text):
    m = PRIORITY_RE.search(text or "")
    if m:
        return "P" + m.group(1)
    low = (text or "").lower()
    if any(w in low for w in ("核心", "必须", "critical", "must")):
        return "P0"
    if any(w in low for w in ("可选", "锦上添花", "optional", "nice")):
        return "P2"
    return "P1"


def _sections(text):
    """按标题切块，返回 [(level, title, [lines])]。第一个 H1 之前的内容单独一块。"""
    out = [(0, "", [])]
    for line in text.splitlines():
        m = HEADING_RE.match(line)
        if m and len(m.group(1)) <= 3:
            out.append((len(m.group(1)), m.group(2).strip(), []))
        else:
            out[-1][2].append(line)
    return out


def _bullets(lines):
    return [m.group(1).strip() for m in (BULLET_RE.match(x) for x in lines) if m]


def _paragraph(lines, limit=240):
    """第一段正文（跳过标题行、表格与空行）。"""
    buf = []
    for line in lines:
        s = line.strip()
        if not s:
            if buf:
                break
            continue
        if s.startswith("|") or s.startswith("```") or s.startswith(">"):
            continue
        buf.append(s)
        if len(" ".join(buf)) > limit:
            break
    return " ".join(buf)[:limit]


def _acceptance_of(lines):
    """模块内的验收条款：支持 `验收：` 段、`AC-01` 行、复选框。"""
    out = []
    grabbing = False
    for line in lines:
        s = line.strip()
        low = s.lower()
        if any(w in low for w in ACCEPT_WORDS) and len(s) < 60:
            grabbing = True
            rest = re.sub(r"^[^:：]*[:：]\s*", "", s)
            if rest and rest != s:
                out.append(rest)
            continue
        m = BULLET_RE.match(s)
        if m:
            item = m.group(1).strip()
            if re.match(r"^\[[ xX]\]", item):        # - [ ] 复选框本身就是验收项
                out.append(item.lstrip("[ xX]").strip())
            elif grabbing:
                out.append(item)
            continue
        if grabbing and s and not s.startswith("#") and not s.startswith("|"):
            continue
        if re.match(r"^#{1,6}\s", s):
            grabbing = False
    return [a for a in out if len(a) > 2][:8]


def _cells(line):
    m = TABLE_ROW_RE.match(line)
    if not m:
        return None
    return [c.strip() for c in m.group(1).split("|")]


def _table_blocks(lines):
    """把连续的 `| a | b |` 行聚成表格块，返回 [(header, rows)]。"""
    blocks, cur = [], []
    for line in lines:
        if _cells(line) is not None:
            cur.append(line)
        elif cur:
            blocks.append(cur)
            cur = []
    if cur:
        blocks.append(cur)
    out = []
    for block in blocks:
        rows = []
        for line in block:
            cells = _cells(line)
            if not cells:
                continue
            if all(set(c) <= set("-: ") for c in cells):
                continue
            rows.append(cells)
        if len(rows) >= 2:
            out.append((rows[0], rows[1:]))
    return out


def _table_modules(lines):
    """只解析真正的「模块划分表」：`| M1 用户模块 | 注册、登录、JWT | M1, M8 |`。

    术语表、表结构字段表、非功能措施表都不能当模块，所以要么表头写着"模块"，
    要么数据行整列都是 M 编号；其余表格一律跳过。
    """
    out = []
    for header, rows in _table_blocks(lines):
        head_text = " ".join(header).lower()
        ids_only = all(MODULE_ROW_RE.match(r[0]) for r in rows)
        if not ids_only and not any(w in head_text for w in ("模块", "module", "子系统")):
            continue
        for r in rows:
            cells = [c for c in r if c != ""]
            if not cells:
                continue
            first = cells[0]
            m = MODULE_ROW_RE.match(first)
            if m:
                mid, mname = m.group(1).upper(), m.group(2).strip()
            elif ids_only:
                continue
            else:
                mid, mname = "", first.strip()
            if not mname or len(mname) > 40:
                continue
            if any(x["name"] == mname for x in out):
                continue
            goal = cells[1].strip() if len(cells) > 1 else ""
            deps = []
            if len(cells) > 2:
                deps = sorted(set(re.findall(r"M\d{1,3}", cells[2].upper())))
            out.append({"id": mid or "M%d" % (len(out) + 1), "name": mname[:40],
                        "goal": goal[:240], "deps": deps})
    return out


def _features(lines):
    """`- F1 用户注册/登录（学号邮箱验证，限校内用户）` 这类功能点。"""
    out = []
    for line in lines:
        m = FEATURE_RE.match(line)
        if not m:
            continue
        text = m.group(2).strip().rstrip("；;")
        if 2 < len(text) < 160:
            out.append((m.group(1).upper(), text))
    return out


def _bigrams(text):
    text = re.sub(r"[\s，,。.;；:：()（）\[\]/、-]+", "", text or "")
    return {text[i:i + 2] for i in range(len(text) - 1)}


def _attach_features(modules, feats, trace):
    """功能点按字面重合度挂到最相关的模块上，作为验收条款的种子。"""
    if not modules or not feats:
        return 0
    pool = [(_bigrams(m["name"] + " " + m.get("goal", "")), m) for m in modules]
    hit = 0
    for fid, text in feats:
        fb = _bigrams(text)
        best, score = None, 0
        for mb, m in pool:
            sc = len(fb & mb)
            if sc > score:
                best, score = m, sc
        if not best or score <= 0:
            continue
        acc = best.setdefault("acceptance", [])
        if len(acc) >= 4:
            continue
        line = "支持 %s %s" % (fid, text[:80])
        if line not in acc:
            acc.append(line)
            hit += 1
    if hit:
        trace.append("%d 个功能点（F×）挂到相关模块的验收条款上" % hit)
    return hit


def _out_of_scope(lines):
    """`范围外（明确不做）：在线支付、物流发货` → 约束里写成"不做：…"。"""
    out, grabbing = [], False
    for raw in lines:
        sline = raw.strip()
        if any(w.lower() in sline.lower() for w in OUT_OF_SCOPE_WORDS):
            rest = re.sub(r"^[^:：]*[:：]\s*", "", sline)
            rest = re.sub(r"^[*_\s]+|[*_\s]+$", "", rest)
            grabbing = not rest
            if rest:
                out.extend(x.strip(" 。.;；") for x in re.split(r"[、,，;；]", rest) if x.strip())
            continue
        if grabbing:
            if not sline or sline.startswith("#"):
                grabbing = False
                continue
            item = BULLET_RE.match(sline)
            text = (item.group(1) if item else sline).strip(" 。.;；")
            if 1 < len(text) < 120:
                out.append(text)
    return ["不做：%s" % x for x in dict.fromkeys(out)][:8]


def _labeled_bullets(lines, labels):
    """`**非功能目标：**` 后面紧跟的项目符号（标签和内容可能同行）。"""
    out, grabbing = [], False
    for raw in lines:
        sline = raw.strip()
        low = sline.lower()
        if any(lab.lower() in low for lab in labels):
            rest = BULLET_RE.match(re.sub(r"^[*_\s]+", "", sline.split("：")[-1]
                                          if "：" in sline else sline.split(":")[-1]))
            grabbing = True
            if rest:
                out.append(rest.group(1).strip())
            continue
        if grabbing:
            item = BULLET_RE.match(sline)
            if item:
                out.append(item.group(1).strip())
                continue
            if not sline or sline.startswith("#"):
                grabbing = False
    return [x for x in out if 2 < len(x) < 200]


def parse_markdown(text, name=""):
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    trace = []
    sections = _sections(text)
    title = ""
    for level, head, _ in sections:
        if level == 1:
            title = head
            break
    name = (name or "").strip() or _clean_heading(title) or "导入的设计规格"
    trace.append("识别标题：%s" % (title or "（无一级标题，用文件名作为规格名）"))

    # 正文摘要：第一个有正文的块
    summary = ""
    for level, head, lines in sections:
        para = _paragraph(lines)
        if para:
            summary = para
            break
    if not summary:
        summary = name
    trace.append("摘要取自正文首段（%d 字）" % len(summary))

    # ---- 模块 ----
    modules = []
    used = [False] * len(sections)
    module_section_idx = []
    for i, (level, head, _) in enumerate(sections):
        low = (head or "").lower()
        if level in (1, 2) and any(w in low for w in MODULE_WORDS):
            module_section_idx.append(i)

    def add_module(title, body_lines):
        mod_name = _clean_heading(title)
        if not mod_name or len(mod_name) > 40:
            return
        if any(m["name"] == mod_name for m in modules):
            return
        acc = _acceptance_of(body_lines)
        goal = _paragraph([x for x in body_lines if not BULLET_RE.match(x.strip())]) \
            or (_bullets(body_lines)[0] if _bullets(body_lines) else "")
        modules.append({
            "id": "M%d" % (len(modules) + 1),
            "name": mod_name,
            "goal": goal[:240] or ("实现" + mod_name),
            "priority": _priority_of("\n".join(body_lines) + " " + mod_name),
            "acceptance": acc,
        })

    # 优先级 0：模块划分表（真实 SDD 最常见）
    table_mods = _table_modules([ln for _l, _h, lines in sections for ln in lines])
    if len(table_mods) >= 2:
        modules = [{
            "id": m["id"], "name": m["name"],
            "goal": m["goal"] or ("实现" + m["name"]),
            "priority": _priority_of(m["goal"] + " " + m["name"]),
            "acceptance": [], "deps": m["deps"],
        } for m in table_mods]
        dep_n = sum(1 for m in modules if m["deps"])
        trace.append("按「模块划分表」读到 %d 个模块%s"
                     % (len(modules), "，%d 个带依赖" % dep_n if dep_n else ""))

    if not modules and module_section_idx:
        for idx in module_section_idx:
            level, head, _ = sections[idx]
            children = []
            for j in range(idx + 1, len(sections)):
                if sections[j][0] <= level:
                    break
                children.append(j)
            if children:
                for j in children:
                    used[j] = True
                    add_module(sections[j][1], sections[j][2])
            else:
                add_module(head, sections[idx][2])
        trace.append("在「模块/Modules」段落里拆出 %d 个模块" % len(modules))
    elif not modules:
        for i, (level, head, lines) in enumerate(sections):
            if level in (2, 3) and head and i not in module_section_idx:
                if any(w in head.lower() for w in CONSTRAINT_WORDS) or len(head) > 40:
                    continue
                add_module(head, lines)
        if modules:
            trace.append("按标题层级拆出 %d 个模块" % len(modules))

    # 兜底：项目符号列表
    if not modules:
        candidates = []
        for level, head, lines in sections:
            if any(w in (head or "").lower() for w in CONSTRAINT_WORDS):
                continue
            candidates.extend(_bullets(lines))
        for item in candidates:
            if len(item) < 4 or len(modules) >= 12:
                continue
            label, _, desc = item.partition("：") if "：" in item else item.partition(":")
            desc = desc or item
            modules.append({
                "id": "M%d" % (len(modules) + 1),
                "name": _clean_heading(label)[:30] or ("模块 %d" % (len(modules) + 1)),
                "goal": desc.strip()[:240],
                "priority": _priority_of(item),
                "acceptance": [],
            })
        if modules:
            trace.append("没有小标题，按项目符号列表拆出 %d 个模块" % len(modules))

    # ---- 全局约束 ----
    constraints = []
    for level, head, lines in sections:
        low = (head or "").lower()
        if any(w in low for w in CONSTRAINT_WORDS):
            items = _bullets(lines)
            if not items:
                # 没有项目符号时读表格：「性能 | 列表分页…」→「性能：列表分页…」
                for hdr, rows in _table_blocks(lines):
                    if all("措施" in h or "要求" in h or "说明" in h for h in hdr[1:2]) or len(hdr) == 2:
                        items.extend("%s：%s" % (r[0], r[1]) for r in rows if len(r) > 1)
                    else:
                        items.extend(" ".join(r) for r in rows)
            if not items:
                items = [x.strip() for x in lines
                         if x.strip() and not x.strip().startswith("|")]
            constraints.extend(i.rstrip("；;，,") for i in items if 2 < len(i) < 200)
    for line in text.splitlines():
        s = line.strip()
        if re.match(r"^(约束|限制|非功能)[:：]", s):
            constraints.append(re.sub(r"^[^:：]*[:：]\s*", "", s))
    all_lines = text.splitlines()
    constraints.extend(_out_of_scope(all_lines))
    constraints.extend(_labeled_bullets(all_lines, ("非功能目标", "非功能需求", "性能目标",
                                                    "质量目标", "安全目标")))
    constraints = list(dict.fromkeys(constraints))[:16]
    if constraints:
        trace.append("抽取全局约束 %d 条%s" % (
            len(constraints),
            "（含 %d 条「不做」）" % len([c for c in constraints if c.startswith("不做：")])
            if any(c.startswith("不做：") for c in constraints) else ""))

    feats = _features(all_lines)
    if feats:
        trace.append("识别功能点 F×%d（%s）" % (len(feats), "、".join(f[0] for f in feats[:10])))
        _attach_features(modules, feats, trace)
    for m in modules:
        m.setdefault("deps", [])
        if not m["acceptance"] and m.get("goal"):
            m["acceptance"] = ["满足「%s」职责：%s" % (m["name"], m["goal"][:60])]
    if any(m["deps"] for m in modules):
        trace.append("模块依赖已记录，分解任务时会据此排先后（并自动断环）")

    frs = sorted({int(m.group(1)) for m in FR_RE.finditer(text)})
    acs = sorted({int(m.group(1)) for m in AC_RE.finditer(text)})
    if frs:
        trace.append("识别需求编号 FR×%d（%s）" % (len(frs), "、".join("FR-%02d" % n for n in frs[:8])))
    if acs:
        trace.append("识别验收编号 AC×%d（%s）" % (len(acs), "、".join("AC-%02d" % n for n in acs[:8])))

    if not modules:
        modules = [{"id": "M1", "name": name, "goal": summary[:200],
                    "priority": "P1", "acceptance": []}]
        trace.append("没找到模块结构，整份文档按单模块导入")
    if "|" in text and not module_section_idx:
        trace.append("检测到表格：当前按标题/列表解析，表格里的列不会自动拆成字段")

    total_acc = sum(len(m["acceptance"]) for m in modules)
    return {
        "name": name[:60],
        "summary": summary[:200],
        "body": text.strip()[:MAX_BODY],
        "modules": modules,
        "constraints": constraints,
        "trace": trace,
        "stats": {"modules": len(modules), "acceptance": total_acc,
                  "constraints": len(constraints),
                  "chars": len(text), "frs": len(frs), "acs": len(acs)},
    }


def parse_any(text, name=""):
    """JSON 规格（本系统导出的 spec）与 Markdown 都认。"""
    raw = (text or "").strip()
    if not raw:
        raise ValueError("文档内容为空")
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
        except Exception as exc:
            raise ValueError("看起来是 JSON，但解析失败：%s" % exc)
        spec = data.get("spec") if isinstance(data, dict) and "spec" in data else data
        if not isinstance(spec, dict):
            raise ValueError("JSON 里没有规格对象")
        out = parse_markdown(spec.get("body") or "", spec.get("name") or name)
        if spec.get("modules"):
            out["modules"] = [{
                "id": m.get("id") or "M%d" % (i + 1),
                "name": m.get("name") or "模块 %d" % (i + 1),
                "goal": m.get("goal") or "",
                "priority": m.get("priority") or "P1",
                "acceptance": list(m.get("acceptance") or []),
            } for i, m in enumerate(spec["modules"])]
        if spec.get("constraints"):
            out["constraints"] = list(spec["constraints"])
        out["summary"] = spec.get("summary") or out["summary"]
        out["trace"].insert(0, "按 JSON 规格导入（%d 个模块）" % len(out["modules"]))
        return out
    return parse_markdown(raw, name)