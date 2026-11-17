#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""静态检查：HTML id 与 JS 引用一致性 + 画布布局比例复核。"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
js = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
css = (ROOT / "static" / "style.css").read_text(encoding="utf-8")

# 顶层用 display:flex/grid 的组件会盖掉浏览器默认的 [hidden]{display:none}，
# 必须有一条兜底规则，否则"隐藏"的横幅/面板会一直显示在页面上。
toggle = re.findall(r"\$\('#([A-Za-z0-9_-]+)'\)\.hidden", js)
has_fallback = bool(re.search(
    r"\[hidden\][^{]*\{[^}]*display\s*:\s*none\s*!important", css))
print("hidden 兜底规则:", "存在" if has_fallback else "缺失！被隐藏的横幅/面板会常驻")
print("JS 用 .hidden 控制显隐的元素:", sorted(set(toggle)))
flex_rules = [c for c in re.findall(r"\.([a-z-]+)\{[^}]*display\s*:\s*(?:flex|grid)", css)]
print("设为 flex/grid 的组件类:", sorted(set(flex_rules)))

ids = set(re.findall(r'id="([^"]+)"', html))
refs = set(re.findall(r"\$\('#([A-Za-z0-9_-]+)'\)", js))
# 这些 id 由 JS 动态插入（审批意见框等），HTML 里本来就没有
DYNAMIC = {"apComment"}
print("HTML id 数 =", len(ids), " JS #id 引用数 =", len(refs))
missing = sorted(refs - ids - DYNAMIC)
print("JS 引用但 HTML 中缺失:", missing if missing else "无")

dyn = sorted(set(re.findall(r"class:\s*'([^']+)'", js)))
print("JS 动态设置的 class:", dyn)

NW, NH, GAPX, GAPY, PAD = 200, 62, 56, 52, 44
depth = {'N1': 0, 'N2': 1, 'N3': 2, 'N4': 2, 'N5': 3, 'N6': 4, 'N7': 4, 'N8': 5, 'N9': 6, 'N10': 7}
layers = {}
for k, v in depth.items():
    layers.setdefault(v, []).append(k)
max_w = max(len(v) * NW + (len(v) - 1) * GAPX for v in layers.values())
W = max(max_w + PAD * 2, 760)
H = PAD * 2 + 8 * NH + 7 * GAPY
print("画布世界尺寸 %dx%d，最宽层 %dpx" % (W, H, max_w))
for sw, sh in [(1018, 890), (1018, 640), (900, 560)]:
    s = min((sw - 52) / W, (sh - 52) / H, 1.15)
    print("  舞台 %dx%d → 适配比例 %.2f，节点标题实际 %.1fpx" % (sw, sh, s, 12.8 * s))

failed = bool(missing) or not has_fallback
print("\n结果:", "FAIL" if failed else "PASS")
sys.exit(1 if failed else 0)
