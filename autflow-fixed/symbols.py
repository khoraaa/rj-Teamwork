"""通用符号切分器。

不写逐语言适配器：只依赖「括号成对 / 缩进 / 结束关键字」这三条通用结构规则，
外加一张声明关键字表（数据，不是代码）。Python 用缩进，SQL 用分号，
Ruby/Lua/Elixir 用 end，其余默认按花括号。

设计目标：结构准确 —— 块的起止落在真实声明的边界上。不承诺语义准确：
它不知道「这一段是不是这一步该写的」，只保证「这一段是一个完整的声明」。

对外只有一个函数：split_symbols(text, filename) -> dict
    {"symbols": [{name, kind, start, end, depth, parent, confidence}],
     "strategy": "brace" | "indent" | "endkw" | "sql" | "none",
     "confidence": "high" | "medium" | "low" | "none",
     "truncated": bool,
     "note": str}
行号一律 1 起、闭区间。
"""

import re

MAX_BYTES = 512 * 1024
MAX_SYMBOLS = 400

# 声明关键字 -> 符号类型。加语言只需要往这张表里加词。
DECL_KINDS = {
    "class": "class", "struct": "struct", "interface": "interface",
    "enum": "enum", "trait": "trait", "impl": "impl", "object": "class",
    "module": "module", "namespace": "module", "package": "module",
    "protocol": "interface", "record": "struct", "union": "union",
    "def": "function", "defp": "function", "defmodule": "module",
    "function": "function", "func": "function", "fn": "function",
    "fun": "function", "sub": "function", "proc": "function",
    "method": "function", "defmacro": "function", "defn": "function",
    "table": "table", "view": "view", "index": "index", "trigger": "trigger",
    "type": "type", "typealias": "type", "typedef": "type",
}

MODIFIERS = {
    "public", "private", "protected", "internal", "static", "final",
    "abstract", "sealed", "partial", "async", "await", "export", "default",
    "declare", "readonly", "override", "virtual", "extern", "inline",
    "synchronized", "native", "unsafe", "constexpr", "consteval", "operator",
    "suspend", "open", "data", "inner", "annotation", "transient",
}

CONTROL = {
    "if", "for", "while", "switch", "catch", "try", "else", "do", "return",
    "new", "throw", "case", "default", "using", "lock", "foreach", "with",
    "match", "when", "unless", "until",
}

_INDENT_EXT = {".py", ".pyi", ".yaml", ".yml", ".toml", ".nim", ".coffee"}
_ENDKW_EXT = {".rb", ".lua", ".ex", ".exs", ".rake", ".gemspec"}
_SQL_EXT = {".sql", ".ddl"}
_HASH_COMMENT_EXT = {".py", ".pyi", ".yaml", ".yml", ".toml", ".rb", ".sh",
                     ".bash", ".ps1", ".pl", ".r", ".nim", ".coffee"}
_DASH_COMMENT_EXT = {".sql", ".ddl", ".lua", ".hs"}


def _ext(filename):
    name = (filename or "").lower()
    idx = name.rfind(".")
    return name[idx:] if idx >= 0 else ""


def _mask(text, ext, hash_comment=False, dash_comment=False):
    """把字符串和注释内容替换成空格，保留换行与代码字符。

    这样数括号时不会被 "(" 或 /* */ 里的字符带偏；行号完全不变。
    """
    out = list(text)
    n = len(text)
    i = 0
    while i < n:
        ch = text[i]
        if ch == "\n":
            i += 1
            continue
        if text.startswith("//", i) or (dash_comment and text.startswith("--", i)):
            j = text.find("\n", i)
            j = n if j < 0 else j
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        if hash_comment and ch == "#":
            j = text.find("\n", i)
            j = n if j < 0 else j
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        if text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        if text.startswith("'''", i) or text.startswith('"""', i):
            quote = text[i:i + 3]
            j = text.find(quote, i + 3)
            j = n if j < 0 else j + 3
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        if ch in "'\"`":
            quote = ch
            out[i] = " "
            i += 1
            while i < n:
                c = text[i]
                if c == "\\" and quote != "`":
                    out[i] = " "
                    i += 1
                    if i < n:
                        if out[i] != "\n":
                            out[i] = " "
                        i += 1
                    continue
                if c == quote:
                    out[i] = " "
                    i += 1
                    break
                if c == "\n" and quote != "`":
                    break
                if out[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        i += 1
    return "".join(out)


def _depths(masked):
    depths = [0] * len(masked)
    depth = 0
    for i, ch in enumerate(masked):
        depths[i] = depth
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth = max(0, depth - 1)
    return depths


def _dedent(line):
    return line.lstrip(" \t")


def _indent_of(line):
    expanded = line.replace("\t", "    ")
    return len(expanded) - len(expanded.lstrip(" "))


def _tokens(line):
    return re.findall(r"[A-Za-z_$][\w$]*|[(){}\[\];=<>:,.&*!+\-/%|]", line)


def _parse_brace_decl(line):
    """从一行里认出声明：关键字声明，或「返回类型 名字(」这种方法声明。"""
    stripped = _dedent(line).strip()
    if not stripped:
        return None
    if stripped[0] in "#@*":
        return None
    toks = _tokens(stripped)
    if not toks:
        return None
    idx = 0
    while idx < len(toks) and toks[idx] in MODIFIERS:
        idx += 1
    if idx >= len(toks):
        return None
    head = toks[idx]
    if head in DECL_KINDS:
        kind = DECL_KINDS[head]
        j = idx + 1
        # 跳过注解/泛型参数：<...>
        if j < len(toks) and toks[j] == "<":
            depth = 0
            while j < len(toks):
                if toks[j] == "<":
                    depth += 1
                elif toks[j] == ">":
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
        # 跳过接收者：func (r *R) Name
        if kind == "function" and j < len(toks) and toks[j] == "(":
            depth = 0
            while j < len(toks):
                if toks[j] == "(":
                    depth += 1
                elif toks[j] == ")":
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
        name = ""
        while j < len(toks):
            tok = toks[j]
            if re.match(r"^[A-Za-z_$][\w$]*$", tok) and tok not in DECL_KINDS:
                name = tok
                break
            if tok in ("{", ";", "=", ":"):
                break
            j += 1
        if not name:
            return None
        return {"name": name, "kind": kind}
    # 方法：第一个不是类型/控制词的「名字(」；取 "(" 前最后一个标识符
    for k, tok in enumerate(toks):
        if tok != "(":
            continue
        if k == 0:
            return None
        name = toks[k - 1]
        if not re.match(r"^[A-Za-z_$][\w$]*$", name):
            return None
        if name in MODIFIERS or name in CONTROL or name in DECL_KINDS:
            return None
        return {"name": name, "kind": "function"}
    return None


def _line_starts(text):
    starts = [0]
    for i, ch in enumerate(text):
        if ch == "\n":
            starts.append(i + 1)
    return starts


def _line_index(starts, offset):
    lo, hi = 0, len(starts) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if starts[mid] <= offset:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _brace_symbols(text, masked, filename, max_depth=1):
    lines = text.split("\n")
    starts = _line_starts(text)
    depths = _depths(masked)
    symbols = []
    for idx, line in enumerate(lines):
        if len(symbols) >= MAX_SYMBOLS:
            break
        if idx >= len(starts):
            break
        off = starts[idx]
        if off >= len(depths):
            continue
        depth = depths[off]
        if depth > max_depth:
            continue
        if not masked[off:off + len(line)].strip():
            continue
        decl = _parse_brace_decl(line)
        if not decl:
            continue
        window_end = starts[idx + 3] if idx + 3 < len(starts) else len(masked)
        brace_off = masked.find("{", off, window_end)
        if brace_off < 0:
            # 没有块：只有以 ; 结尾的声明才当符号（类型别名等）
            if ";" in line and "(" not in line.split(";")[0]:
                symbols.append({"name": decl["name"], "kind": decl["kind"],
                                "start": idx + 1, "end": idx + 1, "depth": depth,
                                "parent": "", "confidence": "medium"})
            continue
        cur = depth
        end_off = -1
        started = False
        for i in range(brace_off, len(masked)):
            ch = masked[i]
            if ch == "{":
                cur += 1
                started = True
            elif ch == "}":
                cur -= 1
                if started and cur <= depth:
                    end_off = i
                    break
        if end_off < 0:
            end_line = len(lines)
            confidence = "low"
        else:
            end_line = _line_index(starts, end_off) + 1
            confidence = "high"
        symbols.append({"name": decl["name"], "kind": decl["kind"],
                        "start": idx + 1, "end": end_line, "depth": depth,
                        "parent": "", "confidence": confidence})
    return symbols


def _indent_symbols(text, masked, max_depth=1):
    lines = text.split("\n")
    masked_lines = masked.split("\n")
    parsed = []
    for idx, line in enumerate(lines):
        if not masked_lines[idx].strip():
            continue
        m = re.match(r"^(\s*)(?:async\s+)?(def|class)\s+([A-Za-z_]\w*)",
                     masked_lines[idx])
        if not m:
            continue
        indent = len(m.group(1).replace("\t", "    "))
        if indent > max_depth * 4:
            continue
        parsed.append((idx, indent, m.group(2), m.group(3)))
    symbols = []
    for pos, (idx, indent, kw, name) in enumerate(parsed):
        end = len(lines)
        for j in range(idx + 1, len(lines)):
            stripped = lines[j].strip()
            if not stripped or stripped.startswith("#"):
                continue
            if _indent_of(lines[j]) <= indent:
                end = j
                break
        while end > idx + 1 and not lines[end - 1].strip():
            end -= 1
        symbols.append({"name": name,
                        "kind": "class" if kw == "class" else "function",
                        "start": idx + 1, "end": end,
                        "depth": 0 if indent == 0 else 1,
                        "parent": "", "confidence": "high"})
    return symbols


def _endkw_symbols(text, masked, max_depth=1):
    lines = masked.split("\n")
    decl_re = re.compile(r"^\s*(def|class|module|function)\s+([A-Za-z_][\w:?!]*)")
    open_re = re.compile(
        r"^\s*(?:def|class|module|function|if|unless|while|until|for|begin|case|do)\b")
    symbols = []
    stack = []
    for idx, line in enumerate(lines):
        if re.match(r"^\s*end\b", line):
            if stack:
                name, kind, start = stack.pop()
                if name and len(stack) <= max_depth:
                    symbols.append({"name": name, "kind": kind,
                                    "start": start + 1, "end": idx + 1,
                                    "depth": len(stack), "parent": "",
                                    "confidence": "medium"})
            continue
        decl = decl_re.match(line)
        if decl:
            stack.append((decl.group(2),
                          "class" if decl.group(1) == "class" else "function",
                          idx))
        elif open_re.match(line):
            stack.append((None, "", idx))
    return symbols


def _sql_symbols(text, masked):
    symbols = []
    # 只在括号深度 0 处按 ; 切分，避免函数体里的分号被切开
    depth = 0
    start = 0
    pieces = []
    for i, ch in enumerate(masked):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == ";" and depth == 0:
            pieces.append((start, i))
            start = i + 1
    if start < len(masked):
        pieces.append((start, len(masked)))
    starts = _line_starts(text)
    pattern = re.compile(
        r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW|INDEX|FUNCTION|PROCEDURE|TRIGGER|SCHEMA)"
        r"\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][\w.$]*|`[^`]+`|\"[^\"]+\")",
        re.I)
    for begin, end in pieces:
        chunk = masked[begin:end]
        m = pattern.match(chunk)
        if not m:
            continue
        name = m.group(2).strip("`\"")
        symbols.append({"name": name.split(".")[-1], "kind": m.group(1).lower(),
                        "start": _line_index(starts, begin) + 1,
                        "end": _line_index(starts, max(begin, end - 1)) + 1,
                        "depth": 0, "parent": "", "confidence": "high"})
        if len(symbols) >= MAX_SYMBOLS:
            break
    return symbols


def _parents(symbols):
    for idx, sym in enumerate(symbols):
        best = ""
        for other in symbols[:idx]:
            if other["depth"] < sym["depth"] and other["start"] <= sym["start"] <= other["end"]:
                if not best or other["depth"] > 0:
                    best = other["name"]
        sym["parent"] = best
    return symbols


def _strategy_for(filename, text):
    ext = _ext(filename)
    if ext in _SQL_EXT:
        return "sql"
    if ext in _ENDKW_EXT:
        return "endkw"
    if ext in _INDENT_EXT:
        return "indent"
    if "{" in text:
        return "brace"
    return "indent"


def split_symbols(text, filename=""):
    if not text:
        return {"symbols": [], "strategy": "none", "confidence": "none",
                "truncated": False, "note": "空文件"}
    truncated = len(text) > MAX_BYTES
    if truncated:
        text = text[:MAX_BYTES]
    ext = _ext(filename)
    masked = _mask(text, ext,
                   hash_comment=ext in _HASH_COMMENT_EXT,
                   dash_comment=ext in _DASH_COMMENT_EXT)
    strategy = _strategy_for(filename, text)
    note = ""
    symbols = []

    def run(name):
        if name == "sql":
            return _sql_symbols(text, masked)
        if name == "indent":
            return _indent_symbols(text, masked)
        if name == "endkw":
            return _endkw_symbols(text, masked)
        return _brace_symbols(text, masked, filename)

    symbols = run(strategy)
    if not symbols:
        # 主策略没切出来就换一条，换完还没有就整文件
        for alt in ("brace", "indent"):
            if alt == strategy:
                continue
            symbols = run(alt)
            if symbols:
                strategy, note = alt, "按 %s 策略回退切分" % alt
                break
    if not symbols:
        return {"symbols": [], "strategy": "none", "confidence": "none",
                "truncated": truncated,
                "note": "没有识别到声明，按整文件显示"}

    symbols.sort(key=lambda s: (s["start"], -s["end"]))
    dedup = []
    seen = set()
    for sym in symbols:
        key = (sym["name"], sym["start"], sym["end"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(sym)
        if len(dedup) >= MAX_SYMBOLS:
            break
    _parents(dedup)
    levels = {s["confidence"] for s in dedup}
    overall = "low" if "low" in levels else ("medium" if "medium" in levels else "high")
    if truncated:
        note = (note + "；" if note else "") + "文件超过 %d KB，只分析了前 %d KB" % (
            MAX_BYTES // 1024, MAX_BYTES // 1024)
    return {"symbols": dedup, "strategy": strategy, "confidence": overall,
            "truncated": truncated, "note": note}