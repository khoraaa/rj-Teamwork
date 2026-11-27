/* AutoFlow 前端：DAG 画布（只读视图 + 编辑态）+ 状态映射 + 人工干预
   数据来源：GET /api/state（运行快照）+ SSE /api/stream（增量补丁）
             GET /api/editor（草稿与版本）
   设计约束：运行状态只渲染服务端快照，不自行推演；编辑只改草稿，不影响在跑的版本。 */

const NS = 'http://www.w3.org/2000/svg';
const $ = (s) => document.querySelector(s);
const $$ = (s) => Array.from(document.querySelectorAll(s));

const STATUS = {
  PENDING:       { label: '等待',   color: '#94a3b8' },
  READY:         { label: '就绪',   color: '#3b82f6' },
  RUNNING:       { label: '运行中', color: '#2563eb' },
  SUCCESS:       { label: '成功',   color: '#16a34a' },
  FAILED:        { label: '失败',   color: '#dc2626' },
  BLOCKED:       { label: '阻断',   color: '#7c3aed' },
  WAITING_HUMAN: { label: '待人工', color: '#d97706' },
  SKIPPED:       { label: '跳过',   color: '#94a3b8' },
  CANCELLED:     { label: '已取消', color: '#64748b' },
  UNCERTAIN:     { label: '待核对', color: '#e11d48' }
};

const RUN_STATUS = {
  IDLE: '未启动', RUNNING: '运行中', PAUSED: '已暂停',
  WAITING_HUMAN: '等待人工', SUCCESS: '运行成功', FAILED: '运行失败', CANCELLED: '已取消',
  RECONCILING: '待核对'
};

const NODE_TYPES = ['start', 'agent', 'condition', 'approval', 'end'];
const NODE_ROLES = ['system', 'pm', 'architect', 'coder', 'qa', 'human'];
const PHASE_ORDER = ['设计', '编码', '测试', '评审', '交付'];

const NW = 200, NH = 62, GAPX = 56, GAPY = 52, PAD = 44;
const PHASE_GAP = 32, PHASE_PAD = 18, PHASE_ROW_GAP = 38;

const app = {
  wf: null, roles: {}, nodes: [], edges: [], run: {}, runId: null,
  nodeById: {}, nodeEls: {}, edgeEls: {}, pos: {},
  world: { w: 1000, h: 800 }, vb: { x: 0, y: 0, w: 1000, h: 800 },
  selected: null, selEdge: null, lastSeq: 0, renderedSeq: 0, built: false,
  es: null, poll: null, drag: null, moved: false, pressNode: false, needFit: true,
  version: 0,              // SSE 快照序号
  mode: 'view',            // view | edit
  viewVersion: 'draft',    // 画布当前查看的定义：draft | vN
  editor: null,            // /api/editor 结果
  versionDefs: {},         // 版本号 → 已发布定义
  runWorkflow: null,       // 运行中快照所绑定的定义
  frozen: false, histId: null,   // 历史回看：冻结实时刷新
  def: null, defKey: '', dirty: false, errors: [],
  specs: [], specId: null, specModules: [], tasks: [],   // 设计规格 → 任务清单
  specTemplate: null,      // 规格面板选中的流程模板（标准 SE / 敏捷精简 / 企业合规）
  // 分解预览：把"规格编排出来的图"临时画在画布上（只读、不落库），退出即还原
  preview: null, previewMode: 'view', specNewFrom: null,
  ais: [], parts: [], partMeta: {},   // 多 AI 接入点 + 任务分组（颜色/名称）
  taskPhaseOpen: {},
};

/* ------------------------------------------------------------------ utils */
function el(tag, attrs, parent) {
  const e = document.createElementNS(NS, tag);
  if (attrs) for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function fmtClock(sec) {
  sec = Math.max(0, Math.floor(sec || 0));
  const m = String(Math.floor(sec / 60)).padStart(2, '0');
  const s = String(sec % 60).padStart(2, '0');
  return m + ':' + s;
}
function fmtDur(v) { return (v || 0).toFixed(1) + 's'; }
function truncate(s, n) { s = s || ''; return s.length > n ? s.slice(0, n - 1) + '…' : s; }
function roleLabel(r) { return (app.roles[r] && app.roles[r].label) || r || '系统'; }
function roleColor(r) { return (app.roles[r] && app.roles[r].color) || '#64748b'; }

let toastTimer = null;
function toast(msg, isErr) {
  const t = $('#toast');
  t.textContent = msg;
  t.className = 'toast show' + (isErr ? ' err' : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.className = 'toast'; }, 3400);
}
function setFoot(txt) { $('#footLeft').textContent = txt; }

async function api(path, body, method) {
  let r;
  try {
    r = await fetch(path, {
      method: method || 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {})
    });
  } catch (e) {
    showNetError('连不上服务：' + (e && e.message ? e.message : '网络中断'));
    throw e;
  }
  const data = await r.json().catch(() => ({ ok: false, message: '响应解析失败（HTTP ' + r.status + '）' }));
  if (r.status >= 500) {
    showNetError('服务端错误（HTTP ' + r.status + '）：' + (data.message || data.error || '未知错误'));
  } else if (r.status === 404 && data.error) {
    showNetError('接口不存在：' + path);
  } else {
    clearNetError();
  }
  if (data.ok === false && !data.errors) toast(data.message || '操作未执行', true);
  return data;
}
async function getJSON(path) {
  let r;
  try {
    r = await fetch(path);
  } catch (e) {
    showNetError('连不上服务：' + (e && e.message ? e.message : '网络中断'));
    throw e;
  }
  if (!r.ok) {
    showNetError('请求失败（HTTP ' + r.status + '）：' + path);
    throw new Error('HTTP ' + r.status);
  }
  clearNetError();
  return r.json();
}

/* 顶部故障条：断线 / 请求失败有明确提示，可手动重试 */
function showNetError(msg) {
  const bar = $('#netBanner');
  if (!bar) return;
  bar.hidden = false;
  $('#netText').textContent = msg;
}
function clearNetError() {
  const bar = $('#netBanner');
  if (bar) bar.hidden = true;
}

/* ---------------------------------------------------------------- layout */
function workflowPhaseGroups(wf) {
  const tasks = wf.tasks || [];
  const taskById = {};
  const nodeByTask = {};
  tasks.forEach(t => { taskById[t.id] = t; });
  (wf.nodes || []).forEach(n => {
    if (n.task_id && taskById[n.task_id]) nodeByTask[n.task_id] = n;
  });
  const byPhase = {};
  tasks.forEach(t => {
    const node = nodeByTask[t.id];
    if (!node) return;
    const phase = t.phase || '编码';
    (byPhase[phase] = byPhase[phase] || { phase, tasks: [], nodes: [] }).tasks.push(t);
    byPhase[phase].nodes.push(node);
  });
  const phases = PHASE_ORDER.filter(p => byPhase[p])
    .concat(Object.keys(byPhase).filter(p => PHASE_ORDER.indexOf(p) < 0));
  return phases.map(phase => byPhase[phase]);
}

function computePhaseLayout(wf, groups) {
  const nodes = wf.nodes || [];
  if (!groups.length) return null;
  const nodeById = {};
  const incoming = {}, outgoing = {};
  nodes.forEach(n => {
    nodeById[n.id] = n;
    incoming[n.id] = [];
    outgoing[n.id] = [];
  });
  (wf.edges || []).forEach(e => {
    if (!outgoing[e.source] || !incoming[e.target]) return;
    outgoing[e.source].push(e.target);
    incoming[e.target].push(e.source);
  });

  const rank = {};
  const taskNodeIds = new Set();
  groups.forEach((group, i) => group.nodes.forEach(n => {
    rank[n.id] = i;
    taskNodeIds.add(n.id);
  }));
  nodes.forEach(n => {
    if (n.type === 'start') rank[n.id] = -1;
    if (n.type === 'end') rank[n.id] = groups.length;
  });
  for (let pass = 0; pass < nodes.length; pass++) {
    let changed = false;
    nodes.forEach(n => {
      if (rank[n.id] != null) return;
      const prev = incoming[n.id].map(id => rank[id]).filter(r => r >= 0 && r < groups.length);
      const next = outgoing[n.id].map(id => rank[id]).filter(r => r >= 0 && r < groups.length);
      if (prev.length) {
        rank[n.id] = Math.max.apply(null, prev);
        changed = true;
      } else if (next.length) {
        rank[n.id] = Math.min.apply(null, next);
        changed = true;
      }
    });
    if (!changed) break;
  }
  nodes.forEach(n => { if (rank[n.id] == null) rank[n.id] = 0; });

  const pos = {};
  const phaseX = PAD + (nodes.some(n => rank[n.id] === -1) ? NW + PHASE_GAP : 0);
  const phaseWidth = NW + PHASE_PAD * 2;
  const phaseStep = phaseWidth + PHASE_GAP;
  const rowStep = NH + PHASE_ROW_GAP;
  let maxRows = 1;
  groups.forEach((group, i) => {
    const items = nodes.filter(n => rank[n.id] === i);
    maxRows = Math.max(maxRows, items.length);
    items.forEach((n, row) => {
      pos[n.id] = {
        x: phaseX + i * phaseStep + PHASE_PAD,
        y: PAD + 42 + row * rowStep
      };
    });
  });
  const startNodes = nodes.filter(n => rank[n.id] === -1);
  const endNodes = nodes.filter(n => rank[n.id] === groups.length);
  startNodes.forEach((n, row) => {
    pos[n.id] = { x: PAD, y: PAD + 42 + row * rowStep };
  });
  const endX = phaseX + groups.length * phaseStep;
  const lastPhaseRight = phaseX + (groups.length - 1) * phaseStep + phaseWidth;
  endNodes.forEach((n, row) => {
    pos[n.id] = { x: endX, y: PAD + 42 + row * rowStep };
  });
  const auxRows = Math.max(startNodes.length, endNodes.length);
  maxRows = Math.max(maxRows, auxRows);
  const worldW = endNodes.length ? endX + NW + PAD : lastPhaseRight + PAD;
  const worldH = PAD * 2 + 42 + maxRows * NH + Math.max(0, maxRows - 1) * PHASE_ROW_GAP;
  return { pos, worldW: Math.max(worldW, 760), worldH, depth: rank };
}

function computeLayout(wf) {
  const nodes = wf.nodes, edges = wf.edges;
  const groups = workflowPhaseGroups(wf);
  const hasSavedPositions = nodes.some(n =>
    n.pos && typeof n.pos.x === 'number' && typeof n.pos.y === 'number');
  if (groups.length && !hasSavedPositions) {
    return computePhaseLayout(wf, groups);
  }
  const indeg = {}, adj = {};
  nodes.forEach(n => { indeg[n.id] = 0; adj[n.id] = []; });
  edges.forEach(e => { if (adj[e.source] && e.target in indeg) { adj[e.source].push(e.target); indeg[e.target]++; } });

  const depth = {}; nodes.forEach(n => depth[n.id] = 0);
  const q = nodes.filter(n => indeg[n.id] === 0).map(n => n.id);
  const order = [];
  const deg = Object.assign({}, indeg);
  while (q.length) {
    const cur = q.shift(); order.push(cur);
    (adj[cur] || []).forEach(nx => { if (--deg[nx] === 0) q.push(nx); });
  }
  (order.length ? order : nodes.map(n => n.id)).forEach(id => {
    (adj[id] || []).forEach(nx => { depth[nx] = Math.max(depth[nx] || 0, depth[id] + 1); });
  });

  const layers = {};
  nodes.forEach(n => { (layers[depth[n.id]] = layers[depth[n.id]] || []).push(n.id); });
  Object.keys(layers).forEach(k =>
    layers[k].sort((a, b) => parseInt(a.replace(/\D/g, '') || 0) - parseInt(b.replace(/\D/g, '') || 0)));

  const maxDepth = Math.max(...Object.keys(layers).map(Number));
  let maxWidth = 0;
  const layerWidth = {};
  Object.keys(layers).forEach(k => {
    const w = layers[k].length * NW + (layers[k].length - 1) * GAPX;
    layerWidth[k] = w; maxWidth = Math.max(maxWidth, w);
  });
  const worldW = Math.max(maxWidth + PAD * 2, 760);
  const worldH = PAD * 2 + (maxDepth + 1) * NH + maxDepth * GAPY;

  const pos = {};
  Object.keys(layers).forEach(k => {
    const ids = layers[k];
    const startX = (worldW - layerWidth[k]) / 2;
    ids.forEach((id, i) => {
      pos[id] = { x: startX + i * (NW + GAPX), y: PAD + Number(k) * (NH + GAPY) };
    });
  });
  // 手工坐标（编辑后写入 pos）优先于自动分层
  nodes.forEach(n => {
    if (n.pos && typeof n.pos.x === 'number' && typeof n.pos.y === 'number') {
      pos[n.id] = { x: n.pos.x, y: n.pos.y };
    }
  });
  return { pos, worldW, worldH, depth };
}

function defKey(def) {
  if (!def) return '';
  return JSON.stringify([
    def.version || '',
    def.nodes.map(n => [n.id, n.name, n.role, n.type, n.pos ? [Math.round(n.pos.x), Math.round(n.pos.y)] : 0]),
    def.edges.map(e => [e.source, e.target, e.label || ''])
  ]);
}

function edgePath(s, t) {
  if (t.x > s.x + 12) {
    const sx = s.x + NW, sy = s.y + NH / 2;
    const tx = t.x, ty = t.y + NH / 2;
    const dx = Math.max(30, (tx - sx) * 0.42);
    return `M ${sx} ${sy} C ${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx - 4} ${ty}`;
  }
  const sx = s.x + NW / 2, sy = s.y + NH;
  const tx = t.x + NW / 2, ty = t.y;
  const dy = Math.max(30, (ty - sy) * 0.42);
  return `M ${sx} ${sy} C ${sx} ${sy + dy}, ${tx} ${ty - dy}, ${tx} ${ty - 4}`;
}

function buildGraph(wf) {
  const { pos, worldW, worldH } = computeLayout(wf);
  app.pos = pos;

  // 世界尺寸按手工坐标扩展，避免节点被拖出画布
  let maxX = worldW - PAD, maxY = worldH - PAD;
  Object.keys(pos).forEach(id => {
    maxX = Math.max(maxX, pos[id].x + NW + PAD);
    maxY = Math.max(maxY, pos[id].y + NH + PAD);
  });
  app.world = { w: Math.max(worldW, maxX), h: Math.max(worldH, maxY) };

  const edgeLayer = $('#edgeLayer'), nodeLayer = $('#nodeLayer');
  edgeLayer.innerHTML = ''; nodeLayer.innerHTML = '';
  drawPhaseGroups(wf, pos);
  app.nodeEls = {}; app.edgeEls = {};

  wf.edges.forEach((e) => {
    const s = pos[e.source], t = pos[e.target];
    if (!s || !t) return;
    const key = e.source + '>' + e.target;
    const p = el('path', { d: edgePath(s, t), class: 'edge', 'marker-end': 'url(#arrow)' }, edgeLayer);
    const hit = el('path', { d: edgePath(s, t), class: 'edge-hit' }, edgeLayer);
    let label = null;
    if (e.label) {
      label = el('text', {
        x: (s.x + t.x) / 2 + NW / 2, y: (s.y + t.y) / 2 + NH / 2 + 2,
        class: 'edge-label', 'text-anchor': 'middle',
        stroke: '#f7f9fc', 'stroke-width': 4, 'paint-order': 'stroke'
      }, edgeLayer);
      label.textContent = e.label === 'true' ? 'TRUE' : 'FALSE';
    }
    hit.addEventListener('click', (ev) => { ev.stopPropagation(); selectEdge(key); });
    p.addEventListener('click', (ev) => { ev.stopPropagation(); selectEdge(key); });
    app.edgeEls[key] = { path: p, hit, label };
  });

  wf.nodes.forEach(n => {
    const p = pos[n.id];
    const g = el('g', { class: 'node', 'data-state': 'PENDING', 'data-id': n.id,
                        transform: `translate(${p.x},${p.y})` });

    el('rect', { class: 'outline', x: -4, y: -4, width: NW + 8, height: NH + 8, rx: 15 }, g);
    el('rect', { class: 'node-bg', x: 0, y: 0, width: NW, height: NH, rx: 12 }, g);
    el('rect', { class: 'role-bar', x: 0, y: 9, width: 4, height: NH - 18, rx: 2, fill: roleColor(n.role) }, g);
    if (n.part) {
      const pm = partMeta(n.part);
      el('rect', { class: 'part-strip', x: 10, y: 0, width: NW - 20, height: 3,
                   rx: 1.5, fill: pm.color, 'fill-opacity': 0.9 }, g);
    }
    el('rect', { class: 'state-strip', x: 1, y: NH - 16, width: NW - 2, height: 15, rx: 4, fill: '#94a3b8' }, g);

    const name = el('text', { class: 'n-name', x: 16, y: 25 }, g);
    name.textContent = truncate(n.name, 13);
    const meta = el('text', { class: 'n-meta', x: 16, y: 43 }, g);
    meta.textContent = n.id + ' · ' + roleLabel(n.role);

    const dur = el('text', { class: 'n-dur', x: 12, y: NH - 5 }, g);
    dur.textContent = '';
    const st = el('text', { class: 'n-state', x: NW - 12, y: NH - 4, 'text-anchor': 'end' }, g);
    st.textContent = '等待';

    const badge = el('g', { class: 'retry-badge', opacity: 0 }, g);
    el('rect', { class: 'badge-retry-bg', x: NW - 46, y: -10, width: 44, height: 17, rx: 8.5 }, badge);
    const bt = el('text', { class: 'badge-retry', x: NW - 24, y: 2, 'text-anchor': 'middle' }, badge);
    bt.textContent = '×0';

    const port = el('circle', { class: 'port', cx: NW / 2, cy: NH, r: 6 }, g);

    // 选中后出现在右上角的操作按钮：编辑（打开属性表单）/ 删除
    const editBtn = el('g', { class: 'node-btn' }, g);
    el('circle', { class: 'nb-bg', cx: NW - 34, cy: -8, r: 12 }, editBtn);
    el('path', {
      class: 'nb-icon',
      transform: `translate(${NW - 40},-14) scale(0.5)`,
      d: 'M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z'
    }, editBtn);

    const del = el('g', { class: 'node-btn del-btn' }, g);
    el('circle', { class: 'del-bg', cx: NW - 8, cy: -8, r: 12 }, del);
    const delText = el('text', { class: 'del-x', x: NW - 8, y: -3, 'text-anchor': 'middle' }, del);
    delText.textContent = '×';
    for (const btn of [editBtn, del]) {
      btn.addEventListener('pointerdown', (ev) => ev.stopPropagation());
    }
    editBtn.addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (!isEditable()) return;
      selectNode(n.id);
      const inp = $('#paneDetail').querySelector('.insp-row input');
      if (inp && inp.focus) inp.focus();
    });
    del.addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (!isEditable()) return;
      app.selected = n.id;
      deleteSelection();
    });

    g.addEventListener('pointerdown', (ev) => onNodePointerDown(ev, n.id));
    g.addEventListener('click', (ev) => { ev.stopPropagation(); selectNode(n.id); });
    nodeLayer.appendChild(g);
    app.nodeEls[n.id] = g;
    g.__badge = badge; g.__badgeText = bt; g.__dur = dur; g.__state = st;
    g.__name = name; g.__meta = meta; g.__port = port; g.__del = del; g.__edit = editBtn;
  });

  app.built = true;
  if (app.needFit) { app.needFit = false; fit(); }
}

function drawPhaseGroups(wf, positions) {
  const layer = $('#phaseLayer');
  if (!layer) return;
  layer.innerHTML = '';
  const groups = workflowPhaseGroups(wf).map((group, index) => {
    const xs = group.nodes.map(n => positions[n.id] && positions[n.id].x).filter(Number.isFinite);
    const ys = group.nodes.map(n => positions[n.id] && positions[n.id].y).filter(Number.isFinite);
    if (!xs.length || !ys.length) return null;
    const x = Math.min.apply(null, xs) - PHASE_PAD;
    const y = Math.min.apply(null, ys) - 38;
    const right = Math.max.apply(null, xs) + NW + PHASE_PAD;
    const bottom = Math.max.apply(null, ys) + NH + PHASE_PAD;
    return {
      phase: group.phase, count: group.tasks.length, index,
      x, y, width: right - x, height: bottom - y
    };
  }).filter(Boolean);
  const overlapping = new Set();
  groups.forEach((a, i) => groups.slice(i + 1).forEach((b, j) => {
    if (a.x < b.x + b.width && a.x + a.width > b.x
        && a.y < b.y + b.height && a.y + a.height > b.y) {
      overlapping.add(i);
      overlapping.add(i + j + 1);
    }
  }));
  const colors = ['#0ea5e9', '#2563eb', '#14b8a6', '#a855f7', '#64748b'];
  groups.forEach((group, i) => {
    if (overlapping.has(i)) return;
    const color = colors[group.index % colors.length];
    el('rect', {
      class: 'phase-box', 'data-phase': group.phase, 'data-task-count': group.count,
      x: group.x, y: group.y, width: group.width, height: group.height, rx: 15,
      fill: color, 'fill-opacity': 0.045, stroke: color,
      'stroke-opacity': 0.55, 'stroke-width': 1.5, 'stroke-dasharray': '6 4'
    }, layer);
    const label = el('text', {
      class: 'phase-label', x: group.x + 12, y: group.y + 21, fill: color
    }, layer);
    label.textContent = `阶段${group.index + 1} · ${group.phase} · ${group.count}项任务`;
  });
}

/* ---------------------------------------------------------------- render */
function render() {
  syncCanvas();
  renderTopbar();
  renderWfBar();
  renderNodes();
  renderEdges();
  renderRail();
  renderHud();
  renderDetail();
  renderBanner();
  $('#stEvents').textContent = app.run.seq || 0;
  $('#canvas').classList.toggle('editable', isEditable());
}

function isEditable() {
  return app.mode === 'edit' && app.viewVersion === 'draft' &&
         (!app.run.status || app.run.status === 'IDLE') && !app.preview;
}

function isPreviewing() { return !!app.preview; }

/* 决定画布显示哪份定义：预览 > 运行版本 > 草稿/所选版本 */
function syncCanvas() {
  if (app.preview) {
    // 预览期间画布只认预演出来的那份定义，不跟随草稿或运行快照
    app.def = app.preview.definition;
    app.wf = app.preview.definition;
    const pkey = defKey(app.def);
    if (pkey !== app.defKey) { app.defKey = pkey; buildGraph(app.def); }
    return;
  }
  const running = app.run.status && app.run.status !== 'IDLE';
  let def = null;
  if (running && app.runWorkflow) def = app.runWorkflow;
  else if (app.viewVersion === 'draft') def = app.editor && app.editor.draft && app.editor.draft.definition;
  else def = app.versionDefs[app.viewVersion];
  if (!def) return;
  app.def = def;
  app.wf = def;
  const key = defKey(def);
  if (key !== app.defKey) { app.defKey = key; buildGraph(def); }
}

function renderTopbar() {
  const s = app.run.status || 'IDLE';
  const chip = $('#runStatus');
  chip.textContent = RUN_STATUS[s] || s;
  chip.dataset.s = s;
  $('#runId').textContent = app.run.run_id || '—';
  $('#runClock').textContent = fmtClock(app.run.elapsed);
  $('#wfVer').textContent = (app.def && app.def.version) || '—';

  const active = ['RUNNING', 'PAUSED', 'WAITING_HUMAN', 'RECONCILING'].includes(s);
  $('#btnStart').disabled = active || app.frozen || isPreviewing();
  $('#btnStart').textContent = ['IDLE'].includes(s) ? '启动运行' : '重新运行';
  $('#btnPause').disabled =
    !['RUNNING', 'PAUSED', 'RECONCILING'].includes(s) || app.frozen || isPreviewing();
  $('#btnPause').textContent = (s === 'PAUSED' || s === 'RECONCILING') ? '继续' : '暂停';
  $('#btnReset').disabled = s === 'IDLE' || app.frozen || isPreviewing();
  $('#btnValidate').disabled = !!app.frozen || isPreviewing();
  $('#tglDemo').checked = !!app.run.demo;
  $('#tglFail').checked = app.run.fail_mode === 'persistent';
  $('#selSpeed').value = String(app.run.speed || 1);
}

function renderWfBar() {
  const e = app.editor;
  const pv = app.preview;
  $('#wfName').textContent = app.frozen
    ? ((app.run.workflow_name || '—') + ' · 历史回看')
    : (pv ? '预览 · ' + (pv.definition.name || '未命名')
          : (e ? e.name : '—'));
  const chip = $('#wfState');
  const runStatus = app.run.status || 'IDLE';
  const active = ['RUNNING', 'PAUSED', 'WAITING_HUMAN', 'RECONCILING'].includes(runStatus);
  const hasRun = runStatus !== 'IDLE';
  if (pv) {
    // 预览不是草稿、也不是已发布版本：必须显式标出来，否则用户会以为图已经保存了
    chip.textContent = '预览 · 未保存';
    chip.dataset.tone = 'draft';
  } else if (app.frozen) {
    chip.textContent = '历史 · ' + (app.run.workflow_version || '—');
    chip.dataset.tone = 'pub';
  } else if (runStatus === 'RECONCILING') {
    chip.textContent = '待核对 · 恢复自中断';
    chip.dataset.tone = 'run';
  } else if (active) {
    chip.textContent = '运行中 · ' + (app.run.workflow_version || '—');
    chip.dataset.tone = 'run';
  } else if (['SUCCESS', 'FAILED', 'CANCELLED'].includes(runStatus)) {
    chip.textContent = ({
      SUCCESS: '已完成', FAILED: '已失败', CANCELLED: '已取消'
    })[runStatus] + ' · ' + (app.run.workflow_version || '—');
    chip.dataset.tone = runStatus === 'SUCCESS' ? 'pub' : 'run';
  } else if (app.viewVersion === 'draft') {
    chip.textContent = '草稿 · 基于 ' + ((e && e.draft && e.draft.base_version) || '—');
    chip.dataset.tone = 'draft';
  } else {
    chip.textContent = '已发布 ' + app.viewVersion;
    chip.dataset.tone = 'pub';
  }
  // 当前草稿由哪个流程模板生成（换模板 = 换能力域词典与阶段编排）
  const tplChip = $('#wfTemplate');
  const curTpl = pv ? (pv.definition.template || '')
    : ((e && e.draft && e.draft.definition && e.draft.definition.template) || '');
  const tplMeta = (app.templates || []).find(t => t.id === curTpl);
  tplChip.hidden = !curTpl;
  tplChip.textContent = curTpl ? ('模板 · ' + (tplMeta ? tplMeta.name : curTpl)) : '';
  tplChip.title = (tplMeta && tplMeta.description) || '生成该草稿所用的流程模板';
  const req = app.frozen ? (app.run.requirement || '')
                         : ((e && e.requirement) || '');
  // 顶栏只显示"画布上这份定义从哪来"：预览 → 规格名 + 未保存；草稿 → 规格名或需求摘要
  const lbl = $('#reqLabel');
  const specId = pv ? (pv.spec && pv.spec.id)
    : ((e && e.draft && e.draft.definition && e.draft.definition.spec_id) || '');
  const specMeta = (app.specs || []).find(s => s.id === specId);
  if (pv) {
    lbl.textContent = '规格：' + truncate((pv.spec && pv.spec.name) || specId, 30) + ' · 预览中';
    lbl.title = '画布上是分解预览的结果，尚未保存为草稿；点「取消分解预览」回到原来的图';
  } else if (specId) {
    const plannedRev = (e && e.draft && e.draft.definition && e.draft.definition.spec_revision) || null;
    const stale = !!(specMeta && plannedRev && specMeta.revision && specMeta.revision !== plannedRev);
    lbl.textContent = '规格：' + truncate(specMeta ? specMeta.name : specId, 30)
      + (stale ? ' · 规格已更新' : '');
    lbl.title = '当前草稿由设计规格 ' + specId
      + (plannedRev ? ' r' + plannedRev : '') + ' 分解编排而来'
      + (stale ? '；当前规格已是 r' + specMeta.revision + '，请重新分解编排' : '');
  } else {
    lbl.textContent = '需求：' + (req ? truncate(req, 34) : '尚未填写，点此设计规格');
    lbl.title = req || '点击填写设计规格，或点「快速生成」用一句话需求';
  }
  $('#wfDirty').hidden = !app.dirty || app.frozen || !!pv;
  $('#wfErr').textContent = (app.errors && app.errors[0] && app.errors[0].msg) || '';
  $('#tglEdit').checked = app.mode === 'edit' && !pv;
  $('#tglEdit').disabled = hasRun || app.frozen || !!pv;
  $('#btnSave').disabled = !app.dirty || app.frozen || !!pv;
  $('#btnPublish').disabled = !(app.mode === 'edit') || hasRun || app.frozen || !!pv;
  $('#btnReq').disabled = app.frozen;
  $('#paletteBlock').hidden = app.mode !== 'edit' || app.frozen || !!pv;
  renderVersionSelect();
  syncSpecButtons();
}

function renderVersionSelect() {
  const sel = $('#selVersion');
  if (!sel || !app.editor) return;
  const sig = app.editor.versions.map(v => v.version).join(',') + '|' + app.viewVersion;
  if (sel.dataset.sig !== sig) {
    sel.dataset.sig = sig;
    sel.innerHTML =
      `<option value="draft">草稿 · 基于 ${esc(app.editor.draft.base_version)}</option>` +
      app.editor.versions.map(v =>
        `<option value="${v.version}">${v.version} · ${esc((v.note || '').slice(0, 12))}</option>`).join('');
    sel.value = app.viewVersion;
  }
  sel.disabled = (app.run.status && app.run.status !== 'IDLE') || isPreviewing();
}

function renderNodes() {
  const errIds = new Set((app.errors || []).map(x => x.node_id).filter(Boolean));
  // 编辑态按定义遍历：新加的节点不在运行快照里，否则选中高亮/状态刷新会漏掉它
  const list = (app.mode === 'edit' && app.def && app.def.nodes) ? app.def.nodes : app.nodes;
  list.forEach(src => {
    const n = app.nodeById[src.id] || src;
    const g = app.nodeEls[src.id];
    if (!g) return;
    const meta = STATUS[n.status] || STATUS.PENDING;
    g.setAttribute('data-state', n.status || 'PENDING');
    g.classList.toggle('selected', app.selected === src.id);
    g.classList.toggle('err', errIds.has(src.id));
    g.__state.textContent = meta.label;
    g.__state.setAttribute('fill', meta.color);
    g.querySelector('.state-strip').setAttribute('fill', meta.color);

    let left = '';
    if (n.status === 'RUNNING' || n.status === 'SUCCESS' || n.status === 'FAILED') left = fmtDur(n.duration);
    else if (n.status === 'WAITING_HUMAN') left = '待处理';
    else if (n.status === 'UNCERTAIN') left = '待核对';
    g.__dur.textContent = left;

    g.__name.textContent = truncate(n.name, 13);
    g.__meta.textContent = src.id + ' · ' + roleLabel(n.role);
    g.querySelector('.role-bar').setAttribute('fill', roleColor(n.role));

    if (n.retry_count > 0) {
      g.__badge.setAttribute('opacity', 1);
      g.__badgeText.textContent = '×' + n.retry_count;
    } else {
      g.__badge.setAttribute('opacity', 0);
    }
  });
}

/* 画布上的边来源：编辑态以草稿定义为准（新加的连线还没进服务端快照，
   否则拖拽重绘、选中高亮都会漏掉它们），其它情况用运行快照。 */
function canvasEdges() {
  if (app.mode === 'edit' && app.def && app.def.edges) {
    return app.def.edges.map(e => ({
      source: e.source, target: e.target, label: e.label, active: true
    }));
  }
  return app.edges || [];
}

function renderEdges() {
  const byId = app.nodeById;
  canvasEdges().forEach(e => {
    const rec = app.edgeEls[e.source + '>' + e.target];
    if (!rec) return;
    const p = rec.path;
    const s = (byId[e.source] || {}).status, t = (byId[e.target] || {}).status;
    let cls = 'edge', marker = 'url(#arrow)';
    if (!e.active) { cls += ' off'; }
    else if (s === 'SUCCESS' && t === 'SUCCESS') { cls += ' done'; marker = 'url(#arrow-done)'; }
    else if (s === 'SUCCESS' && (t === 'RUNNING' || t === 'READY')) { cls += ' flow'; marker = 'url(#arrow-flow)'; }
    else if (s === 'SUCCESS' && t === 'SKIPPED') { cls += ' off'; }
    if (app.selEdge === e.source + '>' + e.target) cls += ' sel';
    p.setAttribute('class', cls);
    p.setAttribute('marker-end', marker);
    if (rec.label) rec.label.style.opacity = e.active ? 1 : 0.32;
  });
}

function renderRail() {
  const c = {}, retry = app.nodes.reduce((a, n) => a + (n.retry_count || 0), 0);
  app.nodes.forEach(n => c[n.status] = (c[n.status] || 0) + 1);
  $('#stSuccess').textContent = c.SUCCESS || 0;
  $('#stRunning').textContent = (c.RUNNING || 0) + (c.READY || 0);
  $('#stRetry').textContent = retry;
  $('#stSkip').textContent = c.SKIPPED || 0;

  const legend = $('#legend');
  if (!legend.dataset.built) {
    legend.innerHTML = Object.keys(STATUS).map(k =>
      `<li><i style="background:${STATUS[k].color}"></i>${STATUS[k].label}<span class="cnt" data-k="${k}">0</span></li>`
    ).join('');
    legend.dataset.built = '1';
  }
  Object.keys(STATUS).forEach(k => {
    const s = legend.querySelector(`.cnt[data-k="${k}"]`);
    if (s) s.textContent = c[k] || 0;
  });
  renderPhaseTasks();
}

function renderPhaseTasks() {
  const listEl = $('#phaseTaskList');
  if (!listEl) return;
  const openPhases = Object.assign({}, app.taskPhaseOpen);
  listEl.querySelectorAll('details[data-phase]').forEach(group => {
    openPhases[group.dataset.phase] = group.open;
  });
  app.taskPhaseOpen = openPhases;

  const tasks = taskRows();
  const count = $('#taskListCount');
  const completed = tasks.filter(t => {
    const status = taskStatus(t);
    return status === 'SUCCESS' || status === 'SKIPPED';
  }).length;
  if (count) count.textContent = tasks.length ? `${completed}/${tasks.length}` : '';
  if (!tasks.length) {
    listEl.innerHTML = '<div class="phase-task-empty">当前流程暂无阶段任务</div>';
    return;
  }

  const groups = {};
  tasks.forEach(t => {
    const phase = t.phase || '编码';
    (groups[phase] = groups[phase] || []).push(t);
  });
  const phases = PHASE_ORDER.filter(phase => groups[phase])
    .concat(Object.keys(groups).filter(phase => PHASE_ORDER.indexOf(phase) < 0));
  listEl.innerHTML = phases.map((phase, index) => {
    const phaseTasks = groups[phase];
    const phaseIndex = PHASE_ORDER.indexOf(phase) + 1;
    const open = Object.prototype.hasOwnProperty.call(openPhases, phase)
      ? openPhases[phase] : true;
    const rows = phaseTasks.map(t => {
      const status = taskStatus(t);
      const statusMeta = STATUS[status] || STATUS.PENDING;
      const node = taskNodeFor(t.id);
      const selected = node && node.id === app.selected;
      return `<button type="button" class="phase-task-row${selected ? ' selected' : ''}"`
        + ` data-task-id="${esc(t.id)}" onclick="focusTask(this.dataset.taskId)"`
        + ` title="${esc(t.title || t.id)}">`
        + `<span class="phase-task-main"><span class="phase-task-id">${esc(t.id)}</span>`
        + `<span class="phase-task-name">${esc(t.title || t.id)}</span></span>`
        + `<span class="phase-task-status" style="--task-status:${statusMeta.color}">${statusMeta.label}</span>`
        + '</button>';
    }).join('');
    return `<details class="phase-task-group" data-phase="${esc(phase)}"${open ? ' open' : ''}>`
      + `<summary><span class="phase-dot p${phaseIndex || index + 1}"></span>`
      + `<span class="phase-task-phase">${esc(phase)}</span>`
      + `<span class="phase-task-total">${phaseTasks.length}</span></summary>`
      + `<div class="phase-task-items">${rows}</div></details>`;
  }).join('');
}

function renderHud() {
  const total = app.nodes.length || 1;
  const done = app.nodes.filter(n => n.status === 'SUCCESS' || n.status === 'SKIPPED').length;
  const pct = Math.round(done / total * 100);
  $('#hudText').textContent = `完成 ${done} / ${total}`;
  $('#hudPct').textContent = pct + '%';
  $('#hudBar').style.width = pct + '%';
}

function renderBanner() {
  const b = $('#banner');
  const waiting = app.nodes.find(n => n.status === 'WAITING_HUMAN');
  if (app.run.status === 'RECONCILING') {
    const uncertain = app.nodes.filter(n => n.status === 'UNCERTAIN');
    b.hidden = false;
    $('#bannerTitle').textContent = '运行已恢复 · 待核对';
    $('#bannerDesc').textContent = `进程曾中断，${uncertain.length} 个节点执行结果待核对，不会自动重复执行。请在右侧逐节点确认成功 / 重新执行 / 跳过。`;
    $('#bannerActions').innerHTML =
      `<button class="btn primary" onclick="doResume()">核对完成，继续运行</button>`;
    return;
  }
  if ((app.run.status === 'WAITING_HUMAN') && waiting) {
    b.hidden = false;
    if (waiting.type === 'approval') {
      $('#bannerTitle').textContent = '上线审批 · 等待人工签署';
      $('#bannerDesc').textContent = `${waiting.id} ${waiting.name}｜高风险节点，批准后流程继续，打回则保持挂起。`;
      $('#bannerActions').innerHTML =
        `<button class="btn primary" onclick="doApprove('${waiting.id}','approve')">批准上线</button>
         <button class="btn danger" onclick="doApprove('${waiting.id}','reject')">打回修改</button>`;
    } else {
      $('#bannerTitle').textContent = `节点 ${waiting.id} 需要人工处理`;
      $('#bannerDesc').textContent = `${waiting.name}｜${waiting.message || '重试次数耗尽'}`;
      $('#bannerActions').innerHTML =
        `<button class="btn warn" onclick="doRetry('${waiting.id}')">修正后重试</button>
         <button class="btn ghost" onclick="doSkip('${waiting.id}')">跳过该节点</button>`;
    }
  } else {
    b.hidden = true;
  }
}

/* ---------------------------------------------------------------- detail */
function showDetailTab() {
  const tabs = $$('.tab');
  if (!tabs.length) return;
  tabs.forEach(x => x.classList.toggle('active', x.dataset.tab === 'detail'));
  $('#paneDetail').hidden = false;
  $('#paneTimeline').hidden = true;
  if ($('#paneFiles')) $('#paneFiles').hidden = true;
}

function selectNode(id) {
  if (app.nodeEdit && app.nodeEdit !== id) app.nodeEdit = null;
  app.selected = id;
  app.selEdge = null;
  showDetailTab();
  renderNodes();
  renderEdges();
  renderPhaseTasks();
  renderDetail(true);
}

function focusTask(taskId) {
  const node = taskNodeFor(taskId);
  if (!node) {
    toast('该任务尚未生成画布节点', true);
    return;
  }
  selectNode(node.id);
  const pos = app.pos[node.id];
  if (pos) {
    app.vb.x = pos.x + NW / 2 - app.vb.w / 2;
    app.vb.y = pos.y + NH / 2 - app.vb.h / 2;
    applyVB();
  }
}
function selectEdge(key) {
  app.selEdge = key;
  app.selected = null;
  showDetailTab();
  renderNodes();
  renderEdges();
  renderPhaseTasks();
  renderDetail(true);
}

function gateChips(n) {
  const names = { format: '格式门', compile: '编译门', review: '评审门', human: '人工门' };
  const g = n.gate || {};
  const keys = Object.keys(names);
  return keys.map(k => {
    const v = g[k] || 'skip';
    const cls = v === 'pass' ? 'pass' : v === 'fail' ? 'fail' : 'skip';
    const txt = v === 'pass' ? '通过' : v === 'fail' ? '未通过' : '未启用';
    return `<span class="chip ${cls}">${names[k]} · ${txt}</span>`;
  }).join('');
}

function jsonHTML(obj) {
  const s = String(JSON.stringify(obj, null, 2))
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return s.replace(/"([^"]+)":/g, '<span class="k">"$1"</span>:')
          .replace(/: "([^"]*)"/g, ': <span class="s">"$1"</span>')
          .replace(/: (\d+(?:\.\d+)?|true|false|null)/g, ': <span class="n">$1</span>');
}

function renderDetail(force) {
  const pane = $('#paneDetail');
  if (app.selEdge) return renderEdgeDetail(pane);
  // 编辑态以定义为准（可能是运行快照里还没有的新节点）
  const draftNode = (app.def && app.selected)
    ? app.def.nodes.find(x => x.id === app.selected) : null;
  const snapNode = app.selected ? app.nodeById[app.selected] : null;
  const dnode = app.mode === 'edit' ? draftNode : null;
  // 视图态：运行状态/产物/日志以快照为准，名称与配置以草稿为准——
  // 否则刚改完的节点名要等下一次运行才会显示
  const n = dnode || (snapNode && draftNode ? Object.assign({}, snapNode, {
    name: draftNode.name, role: draftNode.role, type: draftNode.type,
    deps: draftNode.deps, max_retries: draftNode.max_retries,
    expression: draftNode.expression,
  }) : (snapNode || draftNode));
  if (!n) {
    pane.innerHTML = app.mode === 'edit'
      ? '<div class="empty">编辑态：点击节点修改属性，从节点底部圆点拖到另一个节点即可连线。</div>'
      : '<div class="empty">点击画布中的节点，查看它的配置、质量门、产物与日志。</div>';
    pane.dataset.node = '';
    return;
  }
  // 正在输入（改名称或填审批意见）时不重绘面板，避免打断输入
  if (!force && pane.dataset.node === n.id && pane.contains(document.activeElement)) return;
  pane.dataset.node = n.id;

  if (app.nodeEdit === n.id) return renderNodeEditor(pane, n);
  if (app.mode === 'edit') return renderInspector(pane, n);

  const meta = STATUS[n.status] || STATUS.PENDING;
  const attempts = (n.attempts || []).length;
  const maxR = n.max_retries == null ? '—' : n.max_retries;
  let actions = '';
  if (app.frozen) {
    actions = '';                       // 历史回看只读，不提供操作
  } else if (n.status === 'UNCERTAIN') {
    actions = `<button class="btn primary" onclick="doConfirm('${n.id}')">确认成功（不重复执行）</button>
               <button class="btn warn" onclick="doRetry('${n.id}')">重新执行</button>
               <button class="btn ghost" onclick="doSkip('${n.id}')">跳过</button>`;
  } else if (n.status === 'WAITING_HUMAN' && n.type === 'approval') {
    actions = `<input id="apComment" class="ap-input" value="${esc(app.apComment || '')}"
                      oninput="app.apComment = this.value"
                      placeholder="审批意见（可选，会记入运行历史）">
               <div class="ap-btns">
                 <button class="btn primary" onclick="doApprove('${n.id}','approve')">批准上线</button>
                 <button class="btn danger" onclick="doApprove('${n.id}','reject')">打回修改</button>
               </div>`;
  } else if (n.status === 'WAITING_HUMAN' || n.status === 'FAILED') {
    actions = `<button class="btn warn" onclick="doRetry('${n.id}')">重试该节点</button>
               <button class="btn ghost" onclick="doSkip('${n.id}')">跳过</button>`;
  }

  const lastErr = (n.attempts || []).slice().reverse().find(a => a.error_code);
  const errBlock = (n.status === 'FAILED' || lastErr) ? `
    <div class="section-title">错误</div>
    <div class="errbox">
      <div><b class="mono">${esc((lastErr && lastErr.error_code) || 'E-UNKNOWN')}</b>
        · 第 ${(lastErr && lastErr.attempt) || attempts} 次尝试${n.retry_count ? ' · 已重试 ' + n.retry_count + ' 次' : ''}</div>
      <div class="errmsg">${esc(n.message || '未记录失败原因')}</div>
    </div>` : '';

  const ap = (n.type === 'approval' && n.output && n.output.decision) ? `
    <div class="section-title">人工操作结果</div>
    <div class="apbox ${n.output.decision === 'APPROVED' ? 'ok' : 'no'}">
      <b>${n.output.decision === 'APPROVED' ? '已批准' : '已打回'}</b>
      · ${esc(n.output.approver || '人工')}${n.output.signed_at != null ? ' · ' + fmtDur(n.output.signed_at) : ''}
      <div class="ap-cmt">${esc(n.output.comment || '（无意见）')}</div>
    </div>` : '';

  const attTable = attempts ? `
    <div class="section-title">尝试记录</div>
    <table class="att">
      <thead><tr><th>#</th><th>开始</th><th>结束</th><th>结果</th><th>错误码</th></tr></thead>
      <tbody>${(n.attempts || []).map(a => `<tr>
        <td>${a.attempt}</td>
        <td>${a.started != null ? fmtDur(a.started) : '—'}</td>
        <td>${a.ended != null ? fmtDur(a.ended) : '—'}</td>
        <td>${esc(a.result || '—')}</td>
        <td class="mono">${esc(a.error_code || '')}</td></tr>`).join('')}</tbody>
    </table>` : '';

  pane.innerHTML = `
    <div class="detail-head">
      <div class="dh-top">
        <div class="dh-name">${esc(n.name)}</div>
        ${nodeEditButton(n)}
      </div>
      <div class="dh-sub">
        <span class="mono">${esc(n.id)}</span>
        <span class="role-tag" style="background:${roleColor(n.role)}">${esc(roleLabel(n.role))}</span>
        <span>${esc(n.type)}</span>
      </div>
    </div>
    <div class="kv">
      <div><span>当前状态</span><b style="color:${meta.color}">${meta.label}</b></div>
      <div><span>耗时</span><b>${fmtDur(n.duration)}</b></div>
      <div><span>尝试次数</span><b>${attempts} 次</b></div>
      <div><span>重试上限</span><b>${maxR}</b></div>
    </div>
    ${errBlock}
    <div class="section-title">质量门</div>
    <div class="chips">${gateChips(n)}</div>
    <div class="section-title">执行说明</div>
    ${n.message
      ? `<div class="task-prose">${esc(n.message)}</div>`
      : `<div class="pending-box">${esc(pendingWhy(n))}</div>`}
    ${taskBlock(n)}
    ${ap}
    ${n.expression ? `<div class="section-title">判定表达式</div>
      <div class="mono" style="background:#f8fafc;border:1px solid var(--line);border-radius:8px;padding:7px 10px">${esc(n.expression)}${
        n.branch_taken ? `　→ 命中 <b style="color:${n.branch_taken === 'true' ? '#16a34a' : '#b45309'}">${n.branch_taken.toUpperCase()}</b>` : ''}</div>` : ''}
    <div class="section-title">产出内容</div>
    ${n.output ? outputBlock(n.output) : pendingOutputBlock(n)}
    ${attTable}
    <div class="section-title">执行日志</div>
    ${(n.logs || []).length
      ? `<ul class="logs">${n.logs.map(l => `<li>${esc(l)}</li>`).join('')}</ul>`
      : `<div class="pending-box">${esc(pendingWhy(n, true))}</div>`}
    ${actions ? `<div class="actions">${actions}</div>` : ''}
  `;
}

/* ---------------------------------------------------- 单个节点的就地编辑 */
function nodeEditButton(n) {
  if (app.frozen) return '<span class="panel-note">历史回看只读</span>';
  if (app.run.status && app.run.status !== 'IDLE') {
    return '<span class="panel-note">运行中：先重置运行再改节点</span>';
  }
  if (app.viewVersion !== 'draft') return '<span class="panel-note">已发布版本只读</span>';
  return `<button class="btn sm primary" onclick="beginNodeEdit()" title="只改这一个节点，写进草稿">编辑此节点</button>`;
}

function deleteNodeInline() {
  if (!app.def) return;
  if (app.nodeEdit) {
    const was = app.mode;
    app.mode = 'edit';
    deleteSelection();
    app.mode = was;
  }
  app.nodeEdit = null;
  renderDetail(true);
}

function beginNodeEdit() {
  if (!app.selected) { toast('先在画布上点一个节点', true); return; }
  if (!app.def) { toast('编辑器还没就绪', true); return; }
  app.nodeEdit = app.selected;
  renderDetail(true);
}

function endNodeEdit(force) {
  app.nodeEdit = null;
  if (force) renderDetail(true); else render();
}

function editTaskField(tid, field, val) {
  const t = ((app.def && app.def.tasks) || []).find(x => x.id === tid);
  if (!t) return;
  if (field === 'files') {
    t.files = String(val).split(/[\n,]/).map(x => x.trim()).filter(Boolean);
  } else if (field === 'acceptance') {
    t.acceptance = String(val).split('\n').map(x => x.trim()).filter(Boolean);
  } else if (field === 'steps') {
    t.steps = String(val).split('\n').map(line => {
      const parts = line.split('|');
      return { title: (parts[0] || '').trim(), how: (parts[1] || '').trim() };
    }).filter(x => x.title || x.how);
  } else if (field === 'estimate') {
    t.estimate = Math.max(1, Math.min(60, +val || 1));
  } else {
    t[field] = val;
  }
  const node = (app.def.nodes || []).find(x => x.task_id === tid);
  if (node && field === 'title') {
    node.name = val;
    const g = app.nodeEls[node.id];
    if (g && g.__name) g.__name.textContent = truncate(val, 13);
  }
  app.defKey = defKey(app.def);
  markDirty();
  renderPhaseTasks();
}

function renderNodeEditor(pane, n) {
  const d = (app.def.nodes || []).find(x => x.id === n.id) || n;
  const t = d.task_id ? ((app.def.tasks || []).find(x => x.id === d.task_id) || null) : null;
  const typeOpts = NODE_TYPES.map(x =>
    `<option value="${x}"${d.type === x ? ' selected' : ''}>${x}</option>`).join('');
  const roleOpts = NODE_ROLES.map(r =>
    `<option value="${r}"${d.role === r ? ' selected' : ''}>${roleLabel(r)}</option>`).join('');
  const stepsText = (t && t.steps || []).map(st => (st.title || '') + (st.how ? ' | ' + st.how : '')).join('\n');
  pane.innerHTML = `
    <div class="detail-head">
      <div class="dh-top">
        <div class="dh-name">编辑：${esc(d.name)}</div>
        <button class="btn sm" onclick="endNodeEdit(true)">完成</button>
      </div>
      <div class="dh-sub"><span class="mono">${esc(d.id)}</span>
        <span>只改这一个节点 · 改动写进草稿，点「保存草稿」生效</span></div>
    </div>
    <div class="insp">
      <div class="insp-row"><label>名称</label>
        <input value="${esc(d.name || '')}" oninput="editNode('${d.id}','name',this.value)"></div>
      <div class="insp-row"><label>类型</label>
        <select onchange="editNode('${d.id}','type',this.value)">${typeOpts}</select></div>
      <div class="insp-row"><label>角色</label>
        <select onchange="editNode('${d.id}','role',this.value)">${roleOpts}</select></div>
      <div class="insp-row"><label>重试上限</label>
        <input type="number" min="0" max="9" value="${d.max_retries == null ? 0 : d.max_retries}"
               oninput="editNode('${d.id}','max_retries',Math.max(0,Math.min(9,+this.value||0)))"></div>
      ${d.type === 'condition' ? `<div class="insp-row"><label>判定表达式</label>
        <input value="${esc(d.expression || '')}" placeholder="qa_score >= 80"
               oninput="editNode('${d.id}','expression',this.value)"></div>` : ''}
      <div class="insp-row"><label>前置依赖</label>
        <div class="insp-hint">${esc((d.deps || []).join('、') || '无')}（由连线维护）</div></div>
    </div>
    ${t ? `
    <div class="section-title">任务内容（会一起保存）</div>
    <div class="insp">
      <div class="insp-row"><label>任务标题</label>
        <input value="${esc(t.title || '')}" onchange="editTaskField('${t.id}','title',this.value)"></div>
      <div class="insp-row"><label>阶段</label>
        <select onchange="editTaskField('${t.id}','phase',this.value)">${PHASE_ORDER.map(p =>
          `<option value="${p}"${t.phase === p ? ' selected' : ''}>${p}</option>`).join('')}</select></div>
      <div class="insp-row"><label>优先级</label>
        <select onchange="editTaskField('${t.id}','priority',this.value)">${['P0', 'P1', 'P2'].map(p =>
          `<option value="${p}"${t.priority === p ? ' selected' : ''}>${p}</option>`).join('')}</select></div>
      <div class="insp-row"><label>预估人日</label>
        <input type="number" min="1" max="60" value="${t.estimate == null ? 1 : t.estimate}"
               onchange="editTaskField('${t.id}','estimate',this.value)"></div>
      <div class="insp-row col"><label>做什么</label>
        <textarea rows="3" onchange="editTaskField('${t.id}','what',this.value)"
          placeholder="这条任务要产出什么">${esc(t.what || '')}</textarea></div>
      <div class="insp-row col"><label>怎么做</label>
        <textarea rows="3" onchange="editTaskField('${t.id}','how',this.value)"
          placeholder="用什么实现、写哪个文件、跑什么命令">${esc(t.how || '')}</textarea></div>
      <div class="insp-row col"><label>落点文件</label>
        <input value="${esc((t.files || []).join(', '))}"
               onchange="editTaskField('${t.id}','files',this.value)"
               placeholder="src/auth.py, tests/test_auth.py"></div>
      <div class="insp-row col"><label>子步骤</label>
        <textarea rows="3" onchange="editTaskField('${t.id}','steps',this.value)"
          placeholder="每行一步：标题 | 做法">${esc(stepsText)}</textarea></div>
      <div class="insp-row col"><label>验收条款</label>
        <textarea rows="3" onchange="editTaskField('${t.id}','acceptance',this.value)"
          placeholder="每行一条可验证的验收">${esc((t.acceptance || []).join('\n'))}</textarea></div>
    </div>` : ''}
    <div class="actions">
      <button class="btn primary" onclick="doSave()">保存草稿</button>
      <button class="btn" onclick="endNodeEdit(true)">完成编辑</button>
      <button class="btn danger" onclick="deleteNodeInline()">删除该节点</button>
    </div>
    <div class="insp-hint">改完保存后，画布与运行都按新内容执行；不想留就点「完成编辑」再刷新页面。</div>`;
}

/* 没执行过的节点：把"为什么是空的"讲清楚，而不是留一片空白 */
function pendingWhy(n, forLogs) {
  const st = n.status || 'PENDING';
  if (st === 'SKIPPED') {
    return forLogs
      ? '该节点没有执行（' + (n.message || '条件分支未命中') + '），所以没有日志。'
      : '该节点没有执行：' + (n.message || '条件分支未命中');
  }
  if (st === 'BLOCKED') return '上游没有成功，这个节点被阻断，还没轮到它执行。';
  if (st === 'READY' || st === 'RUNNING') {
    return forLogs ? '正在执行，日志会陆续出现在这里。' : '正在执行…';
  }
  if (st === 'FAILED') return '执行失败，详情见上方错误块。';
  return forLogs
    ? '还没运行：点右上角「启动运行」，跑完这个节点就会写日志。'
    : '还没运行：点右上角「启动运行」，跑完这里会写它实际做了什么。';
}

/* 没产出时显示"预计产出"：协议、角色、落点文件、验收条款 —— 有事可看，不是空白 */
function pendingOutputBlock(n) {
  const t = n.task_id ? ((app.def && app.def.tasks) || []).find(x => x.id === n.task_id) : null;
  const rows = [];
  if (n.out_schema) rows.push(['预计产物', `<span class="mono">${esc(n.out_schema)}</span>`]);
  rows.push(['执行角色', esc(roleLabel(n.role))]);
  if (t && (t.files || []).length) {
    rows.push(['落点文件', t.files.map(f => `<div class="mono">${esc(f)}</div>`).join('')]);
  }
  if (t && (t.acceptance || []).length) {
    rows.push(['要满足的验收', t.acceptance.map(a => `<div>· ${esc(a)}</div>`).join('')]);
  }
  return `<div class="pending-box">${esc(pendingWhy(n))}</div>
    <div class="kv out-kv">${rows.map(([k, v]) => `<div><span>${k}</span><b>${v}</b></div>`).join('')}</div>`;
}

/* 产出内容：先按类型给人看，再给原始 JSON */
function outputBlock(o) {
  if (!o || typeof o !== 'object') return `<pre class="output">${esc(String(o))}</pre>`;
  const rows = [];
  const push = (k, v) => { if (v !== undefined && v !== null && v !== '') rows.push([k, v]); };
  if (o.artifact) push('产物协议', `<span class="mono">${esc(o.artifact)}</span>`);
  if (o.summary) push('摘要', esc(String(o.summary)));
  if (o.decision) push('人工决定', `<b>${esc(String(o.decision))}</b>`);
  if (o.approver) push('处理人', esc(String(o.approver)));
  if (o.comment) push('意见', esc(String(o.comment)));
  if (o.loc != null) push('代码行数', esc(String(o.loc)) + ' 行');
  if (o.passed != null || o.total != null) {
    const p = o.passed == null ? '—' : o.passed, t = o.total == null ? '—' : o.total;
    push('测试结果', `${p} / ${t} 通过`);
  }
  if (o.command) push('测试命令', `<span class="mono">${esc(String(o.command))}</span>`);
  if (o.merged != null) push('已合并', o.merged ? '是' : '否');
  if (o.security_score != null) push('安全评分', esc(String(o.security_score)));
  if (Array.isArray(o.files) && o.files.length) {
    push('产物文件', o.files.map(f => `<div class="mono">${esc(typeof f === 'string' ? f : (f.path || JSON.stringify(f)))}</div>`).join(''));
  }
  if (Array.isArray(o.acceptance_results) && o.acceptance_results.length) {
    push('验收结果', o.acceptance_results.map(r =>
      `<div class="${r.status === 'pass' ? 'acc-pass' : (r.status === 'fail' ? 'acc-fail' : '')}">${
        r.status === 'pass' ? '✓ ' : (r.status === 'fail' ? '✗ ' : '· ')}${esc(r.criterion || '')}</div>`).join(''));
  }
  const table = rows.length
    ? `<div class="kv out-kv">${rows.map(([k, v]) => `<div><span>${k}</span><b>${v}</b></div>`).join('')}</div>`
    : '<div class="empty" style="padding:10px">这条产出没有可读摘要，展开看原始 JSON</div>';
  const tail = o.output_tail
    ? `<div class="section-title">原始输出（尾部）</div><pre class="output">${esc(String(o.output_tail).slice(0, 2000))}</pre>` : '';
  return `${table}${tail}
    <details class="raw-json"><summary>原始 JSON</summary>
      <pre class="output">${jsonHTML(o)}</pre></details>`;
}

function renderInspector(pane, n) {
  const d = app.def.nodes.find(x => x.id === n.id) || n;
  const typeOpts = NODE_TYPES.map(t =>
    `<option value="${t}"${d.type === t ? ' selected' : ''}>${t}</option>`).join('');
  const roleOpts = NODE_ROLES.map(r =>
    `<option value="${r}"${d.role === r ? ' selected' : ''}>${roleLabel(r)}</option>`).join('');
  const deps = (d.deps || []).join('、') || '无';
  pane.innerHTML = `
    <div class="detail-head">
      <div class="dh-name">编辑节点 ${esc(d.id)}</div>
      <div class="dh-sub"><span class="mono">${esc(d.id)}</span><span>改动仅写入草稿</span></div>
    </div>
    <div class="insp">
      <div class="insp-row"><label>名称</label>
        <input value="${esc(d.name || '')}" oninput="editNode('${d.id}','name',this.value)"></div>
      <div class="insp-row"><label>类型</label>
        <select onchange="editNode('${d.id}','type',this.value)">${typeOpts}</select></div>
      <div class="insp-row"><label>角色</label>
        <select onchange="editNode('${d.id}','role',this.value)">${roleOpts}</select></div>
      <div class="insp-row"><label>重试上限</label>
        <input type="number" min="0" max="9" value="${d.max_retries == null ? 0 : d.max_retries}"
               oninput="editNode('${d.id}','max_retries',Math.max(0,Math.min(9,+this.value||0)))"></div>
      <div class="insp-row"><label>前置依赖</label><div class="insp-hint">${esc(deps)}　（由连线自动维护）</div></div>
      ${d.task_id ? `<div class="insp-row"><label>任务归属</label>
        <div class="insp-hint">${esc(d.task_id)} · ${esc(d.module || '—')} · ${esc(d.spec_ref || '')}<br>
        由设计规格分解生成；改名称不会断开与任务清单的对应关系。</div></div>` : ''}
      ${d.type === 'condition' ? `<div class="insp-row"><label>判定表达式</label>
        <input value="${esc(d.expression || '')}" placeholder="qa_score >= 80"
               oninput="editNode('${d.id}','expression',this.value)"></div>` : ''}
      <button class="btn danger insp-danger" onclick="deleteSelection()">删除该节点</button>
      <div class="insp-hint">拖拽节点可调整位置；从底部蓝色圆点拖到另一个节点建立依赖。</div>
    </div>`;
}

/* 任务清单：任务节点显示自己那条，pm 节点显示全表；点行跳到图上对应节点 */
function taskRows() { return (app.def && app.def.tasks) || []; }

/* pm 节点：模板里的类型是 agent，角色才是 pm，两种写法都认 */
function isPmNode(n) { return n.type === 'pm' || n.role === 'pm'; }

function taskNodeFor(tid) {
  return ((app.nodes || []).find(x => x.task_id === tid))
    || ((app.def && app.def.nodes || []).find(x => x.task_id === tid))
    || null;
}

function taskStatus(t) {
  const node = taskNodeFor(t.id);
  return (node && node.status) || 'PENDING';
}

function taskStatusLabel(t) {
  const status = taskStatus(t);
  return (STATUS[status] && STATUS[status].label) || status;
}

function taskAcceptanceHTML(t) {
  const node = taskNodeFor(t.id);
  const results = (node && node.output && node.output.acceptance_results) || [];
  return (t.acceptance || []).map((criterion, i) => {
    const hit = results.find(r => r && r.criterion === criterion);
    const source = (t.acceptance_sources || [])[i] || '';
    const status = hit ? hit.status : '';
    const cls = status === 'pass' ? 'acc-pass' : (status === 'fail' ? 'acc-fail' : '');
    const mark = status === 'pass' ? '✓ ' : (status === 'fail' ? '✗ ' : '· ');
    const tip = hit && hit.evidence ? ` title="证据：${esc(hit.evidence)}"` : '';
    return `<div class="acc-line ${cls}"${tip}>${mark}${esc(criterion)}`
      + (source ? ` <span class="acc-source">${esc(source)}</span>` : '') + '</div>';
  }).join('') || '<span class="muted">无验收条款</span>';
}

function taskRefHTML(t) {
  const rev = t.spec_revision ? ` · r${t.spec_revision}` : '';
  return `<span class="mono">${esc(t.spec_ref || '—')}</span><span class="muted">${rev}</span>`;
}

function syncTaskDependencies() {
  const list = taskRows();
  if (!list.length || !app.def) return;
  const nodeByTask = {};
  (app.def.nodes || []).forEach(n => { if (n.task_id) nodeByTask[n.task_id] = n; });
  list.forEach(t => {
    const n = nodeByTask[t.id];
    if (!n) return;
    t.depends_on = (n.deps || []).map(id => (nodeByTask[id] || {}).task_id)
      .filter(Boolean);
  });
}

function removeTaskForNode(node) {
  if (!node || !node.task_id || !app.def || !Array.isArray(app.def.tasks)) return;
  app.def.tasks = app.def.tasks
    .filter(t => t.id !== node.task_id)
    .map(t => Object.assign({}, t, {
      depends_on: (t.depends_on || []).filter(id => id !== node.task_id)
    }));
}

/* 这个节点用哪个 AI：空 = 按分组/默认 */
function aiSelectHTML(n) {
  if (!app.ais.length) return '默认 AI<span class="muted">（未接入其它 AI）</span>';
  const editable = isEditable();
  return `<select class="select sm" ${editable ? '' : 'disabled'}`
    + ` onchange="setNodeAI('${esc(n.id)}', this.value)">`
    + aiOptions(n.ai || '', '（按分组 / 默认）') + '</select>';
}

function setNodeAI(id, aiId) {
  const n = app.def && (app.def.nodes || []).find(x => x.id === id);
  if (!n) return;
  n.ai = aiId || '';
  markDirty();
  renderDetail(true);
  toast(aiId ? '这个节点改用 ' + aiId : '这个节点改回默认 AI');
}

function partSelectHTML(n) {
  const editable = isEditable();
  const opts = app.parts.map(p =>
    `<option value="${esc(p.id)}"${p.id === n.part ? ' selected' : ''}>${esc(p.label)}</option>`);
  if (!n.part) opts.unshift('<option value="" selected>（未分组）</option>');
  return `<select class="select sm" ${editable ? '' : 'disabled'}`
    + ` onchange="setNodePart('${esc(n.id)}', this.value)">${opts.join('')}</select>`;
}

function setNodePart(id, partId) {
  const n = app.def && (app.def.nodes || []).find(x => x.id === id);
  if (!n) return;
  n.part = partId || '';
  n.part_label = partMeta(partId).label;
  markDirty();
  renderCanvas();
  renderDetail(true);
  toast('已归到「' + partMeta(partId).label + '」');
}

function taskBlock(n) {
  const list = taskRows();
  if (!list.length) return '';
  const t = n.task_id ? list.find(x => x.id === n.task_id) : null;
  const info = t ? `
    <div class="section-title">任务信息</div>
    <div class="kv">
      <div><span>任务</span><b>${esc(t.id)} · ${esc(t.module || '—')}</b></div>
      <div><span>阶段 / 角色</span><b>${esc(t.phase || '—')} · ${esc(t.role || '—')}</b></div>
      <div><span>优先级</span><b>${esc(t.priority || '—')}</b></div>
      <div><span>预估</span><b>${t.estimate} 人日</b></div>
      <div><span>规格出处</span><b>${taskRefHTML(t)}</b></div>
      <div><span>执行器</span><b>${esc(t.agent || '—')}${t.kind ? ' · ' + esc(t.kind) : ''}</b></div>
      <div><span>任务分组</span><b>${partSelectHTML(n)}</b></div>
      <div><span>执行 AI</span><b>${aiSelectHTML(n)}</b></div>
    </div>
    ${t.what ? `<div class="section-title">做什么</div>
      <div class="task-prose">${esc(t.what)}</div>` : ''}
    ${t.how ? `<div class="section-title">怎么做</div>
      <div class="task-prose">${esc(t.how)}</div>` : ''}
    ${(t.files || []).length ? `<div class="section-title">落点文件</div>
      <div class="chips">${t.files.map(f => `<span class="chip file">${esc(f)}</span>`).join('')}</div>` : ''}
    ${(t.steps || []).length ? `<div class="section-title">子步骤（${t.steps.length}）</div>
      <ol class="step-list">${t.steps.map((st, i) => `<li><div class="step-copy"><b>${esc(st.title || '')}</b>${
        st.how ? '<span>' + esc(st.how) + '</span>' : ''}</div>
        <button class="btn sm ghost step-preview-btn" onclick="previewStepCode('${esc(t.id)}', ${i})"
          ${!(t.files || []).length ? 'disabled title="此任务未关联目标文件"' : 'title="查看本次运行已生成的真实代码（只读）"'}>查看代码</button></li>`).join('')}</ol>` : ''}
    <div class="section-title">验收条款</div>
    <div class="task-acceptance">${taskAcceptanceHTML(t)}</div>` : '';
  if (!isPmNode(n) && !t) return info;
  const rows = list.map(x => `
    <tr class="${t && x.id === t.id ? 'cur' : ''}" onclick="gotoTask('${esc(x.id)}')"
        data-s="${esc(taskStatus(x))}">
      <td class="task-state" data-s="${esc(taskStatus(x))}">${esc(taskStatusLabel(x))}</td>
      <td class="mono">${esc(x.id)}</td>
      <td>${esc(x.title)}</td>
      <td class="task-acc-cell">${taskAcceptanceHTML(x)}</td>
      <td class="task-source-cell">${taskRefHTML(x)}</td>
    </tr>`).join('');
  return `${info}
    <div class="section-title">任务清单（PM Agent 分解）</div>
    <div class="task-mini-wrap">
      <table class="task-mini">
        <thead><tr><th>状态</th><th>ID</th><th>任务</th><th>验收标准</th><th>规格出处</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    <div class="insp-hint">点一行跳到图上对应的节点。</div>`;
}

function gotoTask(tid) {
  const list = (app.def && app.def.nodes) || [];
  const hit = list.find(x => x.task_id === tid);
  if (hit) selectNode(hit.id);
}

async function previewStepCode(taskId, stepIndex) {
  const dialog = $('#stepCodeDialog');
  const path = $('#stepCodePath');
  const status = $('#stepCodeStatus');
  const code = $('#stepCodeBody');
  path.textContent = '正在确定目标文件…';
  status.textContent = '正在读取本次运行的代码（没有落盘时会生成一次，之后走缓存）…';
  code.textContent = '';
  if (!dialog.open) dialog.showModal();
  try {
    const result = await api('/api/tasks/preview-code', {
      task_id: taskId, step_index: stepIndex,
      run_id: (app.run && app.run.run_id) || ''
    });
    if (!result.ok) {
      path.textContent = result.path || '代码不可用';
      status.textContent = result.message || '读取代码失败';
      return;
    }
    path.textContent = result.path;
    const notes = {
      workspace: '来自本次运行的工作区 · 只读',
      cache: '本次查看生成的结果 · 已缓存，不再调模型',
      generated: '运行没有落盘，已按该子步骤生成一次并缓存 · 未执行、未参与测试'
    };
    status.textContent = (notes[result.source] || notes.workspace)
      + (result.truncated ? ' · 文件较大，已截断显示' : '');
    code.textContent = result.code || '';
  } catch (error) {
    status.textContent = '请求失败：' + (error && error.message ? error.message : '网络错误');
  }
}

function renderEdgeDetail(pane) {
  const [src, dst] = String(app.selEdge || '').split('>');
  const inEdit = isEditable();
  pane.innerHTML = `
    <div class="detail-head">
      <div class="dh-name">连线 ${esc(src)} → ${esc(dst)}</div>
      <div class="dh-sub"><span>依赖关系：${esc(dst)} 依赖 ${esc(src)}</span></div>
    </div>
    ${inEdit ? `<div class="insp"><button class="btn danger insp-danger" onclick="deleteSelection()">删除该连线</button>
      <div class="insp-hint">删除连线会同时移除 ${esc(dst)} 对 ${esc(src)} 的依赖。</div></div>`
      : '<div class="empty" style="padding:14px">只读视图：进入编辑态后可删除连线。</div>'}`;
}

/* -------------------------------------------------------------- timeline */
function appendEvents(events) {
  const tl = $('#timeline');
  events.forEach(e => {
    if (e.seq <= app.renderedSeq) return;
    app.renderedSeq = e.seq;
    const div = document.createElement('div');
    div.className = 'ev';
    div.dataset.type = e.type;
    div.innerHTML = `<div class="t">${fmtClock(e.ts)}</div><div class="d"></div>
      <div class="m">${e.node_id ? `<b>${esc(e.node_id)}</b> ` : ''}${esc(e.message)}</div>`;
    tl.insertBefore(div, tl.firstChild);
  });
  while (tl.children.length > 260) tl.removeChild(tl.lastChild);
}

/* ----------------------------------------------------------------- state */
function applySnapshot(s) {
  if (app.frozen) return;          // 历史回看期间不被实时状态覆盖
  if (app.preview) {               // 预览期间画布是人造的，不接受实时快照改图
    app.run = Object.assign({}, app.run, (s && s.run) || {});
    renderTopbar();
    return;
  }
  app.roles = s.roles || {};
  if (s.settings) { app.settings = s.settings; renderModeChip(); }
  if (s.workflow) { app.wf = s.workflow; app.runWorkflow = s.workflow; }

  app.run = s.run || {};
  app.runId = app.run.run_id || app.runId;
  renderModeChip();
  renderFileBadge();
  app.nodes = s.nodes || [];
  app.edges = s.edges || [];
  app.nodeById = {};
  app.nodes.forEach(n => app.nodeById[n.id] = n);

  // 快照即全量：时间线以快照为准重建（兼容「重置后事件序号归零」）
  app.events = s.events || [];
  app.lastSeq = app.events.length ? app.events[app.events.length - 1].seq : 0;
  app.renderedSeq = 0;
  $('#timeline').innerHTML = '';
  appendEvents(app.events);

  render();
}

function applyPatch(p) {
  if (app.frozen) return;
  if (app.preview) {               // 预览期间只同步顶栏时钟，不重绘画布
    if (p.run) app.run = Object.assign({}, app.run, p.run);
    if (p.version != null) app.version = p.version;
    renderTopbar();
    return;
  }
  if (p.version != null && app.version === p.version) return;
  app.version = p.version;

  const prevStatus = app.run.status;
  // 新的一次运行（run_id 变化）→ 清空时间线，避免旧序号屏蔽新事件
  if (p.run && p.run.run_id && p.run.run_id !== app.runId) {
    app.runId = p.run.run_id;
    app.lastSeq = 0;
    app.renderedSeq = 0;
    app.events = [];
    $('#timeline').innerHTML = '';
    app.selected = null;
  }

  app.run = Object.assign({}, app.run, p.run || {});
  renderModeChip();
  renderFileBadge();
  if (p.nodes) {
    app.nodes = p.nodes;
    app.nodeById = {};
    app.nodes.forEach(n => app.nodeById[n.id] = n);
  }
  if (p.edges) app.edges = p.edges;
  if (p.events && p.events.length) {
    app.lastSeq = Math.max(app.lastSeq, p.events[p.events.length - 1].seq);
    appendEvents(p.events);
  }

  // 运行开始/结束时，画布要切换定义（运行版本 ↔ 草稿）
  if (prevStatus === 'IDLE' && app.run.status !== 'IDLE') { refresh(); return; }
  if (prevStatus !== 'IDLE' && app.run.status === 'IDLE') { refresh(); return; }
  render();
}

async function refresh() {
  try {
    const s = await getJSON('/api/state');
    applySnapshot(s);
  } catch (e) { /* ignore */ }
}

async function loadEditor() {
  try {
    const e = await getJSON('/api/editor');
    if (e && e.ok) { app.editor = e; app.defKey = ''; }
  } catch (err) { /* 兼容旧服务端 */ }
  syncTemplateSelect();
}

/* 流程模板：后端 templates/*.json 的索引，决定能力域词典与阶段编排。
   两处入口共用同一份清单：快速生成面板、设计规格面板（选标准 SE / 敏捷精简 / 企业合规）。 */
async function loadTemplates() {
  const sel = $('#selTemplate');
  const specSel = $('#selSpecTemplate');
  try {
    const r = await getJSON('/api/templates');
    const list = r.templates || [];
    app.templates = list;
    const opts = list.map(t =>
      `<option value="${esc(t.id)}" title="${esc(t.description || '')}">${esc(t.name)} · ${t.stages} 阶段</option>`
    ).join('');
    if (sel) { sel.innerHTML = opts; sel.hidden = !list.length; }
    if (specSel) { specSel.innerHTML = opts; specSel.hidden = !list.length; }
  } catch (e) {
    if (sel) sel.hidden = true;      /* 旧服务端没有该接口 */
    if (specSel) specSel.hidden = true;
  }
  syncTemplateSelect();
  syncSpecTemplate();
}

/* 下拉框跟随"当前草稿是哪个模板生成的" */
function syncTemplateSelect() {
  const sel = $('#selTemplate');
  const e = app.editor;
  const cur = e && e.draft && e.draft.definition && e.draft.definition.template;
  if (!sel || !cur || !(app.templates || []).some(t => t.id === cur)) return;
  sel.value = cur;
}

/* 规格面板的模板选择：默认跟草稿一致，但用户一旦显式选过就尊重他的选择 */
function syncSpecTemplate() {
  const sel = $('#selSpecTemplate');
  if (!sel) return;
  const def = app.editor && app.editor.draft && app.editor.draft.definition;
  const fromDraft = (def && def.template) || 'default';
  if (!app.specTemplate) app.specTemplate = fromDraft;
  if ((app.templates || []).some(t => t.id === app.specTemplate) && sel.value !== app.specTemplate) {
    sel.value = app.specTemplate;
  }
  renderSpecTplDesc();
}

/* 模板说明直接摊在面板里：光看名字分不清"标准 SE"和"企业合规"差在哪 */
function renderSpecTplDesc() {
  const box = $('#specTplDesc');
  if (!box) return;
  const sel = $('#selSpecTemplate');
  const id = (sel && sel.value) || app.specTemplate || 'default';
  const meta = (app.templates || []).find(t => t.id === id);
  box.textContent = meta
    ? `${meta.name} · ${meta.stages} 阶段 · 并行上限 ${meta.concurrency_limit || '—'}：${meta.description || ''}`
    : '';
}

/* 换模板：预览中就直接按新模板重画，别让人对着上一个模板的图做判断 */
async function changeSpecTemplate(value) {
  app.specTemplate = value || 'default';
  renderSpecTplDesc();
  if (app.preview) {
    if (app.specId) await previewSpec();
  }
}

/* ------------------------------------------------------------ 编辑操作 */
function markDirty() {
  app.dirty = true;
  $('#wfDirty').hidden = false;
  $('#btnSave').disabled = !isEditable();
  renderWfBar();
}

function toWorld(evt) {
  const svg = $('#canvas'), r = svg.getBoundingClientRect();
  return {
    x: app.vb.x + (evt.clientX - r.left) / r.width * app.vb.w,
    y: app.vb.y + (evt.clientY - r.top) / r.height * app.vb.h
  };
}

function onNodePointerDown(ev, id) {
  if (!isEditable() || ev.button !== 0) return;
  const svg = $('#canvas');
  // 指针会被画布捕获，随后的 click 可能改派到 SVG，所以选中必须在这里就完成
  app.pressNode = true;
  selectNode(id);
  if (ev.target.classList && ev.target.classList.contains('port')) {
    ev.preventDefault();
    startLink(ev, id);
    return;
  }
  ev.preventDefault();
  const p = app.pos[id] || { x: 0, y: 0 };
  const w = toWorld(ev);
  app.nodeDrag = { id, dx: w.x - p.x, dy: w.y - p.y, moved: false };
  try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
}

function startLink(ev, fromId) {
  const svg = $('#canvas');
  const p = app.pos[fromId] || { x: 0, y: 0 };
  const w = toWorld(ev);
  const path = el('path', { class: 'edge-linking', d: '' }, $('#edgeLayer'));
  app.link = { from: fromId, x1: p.x + NW / 2, y1: p.y + NH, x2: w.x, y2: w.y, path };
  updateLinkPath();
  try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
}

function updateLinkPath() {
  const l = app.link; if (!l) return;
  const dy = Math.max(24, Math.abs(l.y2 - l.y1) * 0.4);
  l.path.setAttribute('d', `M ${l.x1} ${l.y1} C ${l.x1} ${l.y1 + dy}, ${l.x2} ${l.y2 - dy}, ${l.x2} ${l.y2 - 4}`);
}

function onPointerMove(ev) {
  if (app.nodeDrag) {
    const w = toWorld(ev);
    const id = app.nodeDrag.id;
    const x = w.x - app.nodeDrag.dx, y = w.y - app.nodeDrag.dy;
    app.nodeDrag.moved = true;
    app.pos[id] = { x, y };
    const g = app.nodeEls[id];
    if (g) g.setAttribute('transform', `translate(${x},${y})`);
    repaintEdgesFor(id);
    return true;
  }
  if (app.link) {
    const w = toWorld(ev);
    app.link.x2 = w.x; app.link.y2 = w.y;
    updateLinkPath();
    return true;
  }
  return false;
}

function repaintEdgesFor(id) {
  canvasEdges().forEach(e => {
    if (e.source !== id && e.target !== id) return;
    const rec = app.edgeEls[e.source + '>' + e.target];
    if (!rec) return;
    const s = app.pos[e.source], t = app.pos[e.target];
    if (!s || !t) return;
    const d = edgePath(s, t);
    rec.path.setAttribute('d', d);
    rec.hit.setAttribute('d', d);
    if (rec.label) {
      rec.label.setAttribute('x', (s.x + t.x) / 2 + NW / 2);
      rec.label.setAttribute('y', (s.y + t.y) / 2 + NH / 2 + 2);
    }
  });
}

function onPointerUp(ev) {
  if (app.nodeDrag) {
    const id = app.nodeDrag.id;
    const moved = app.nodeDrag.moved;
    const g = app.nodeEls[id];
    if (g) g.classList.remove('dragging');
    if (moved) {
      const n = app.def.nodes.find(x => x.id === id);
      if (n) n.pos = { x: Math.round(app.pos[id].x), y: Math.round(app.pos[id].y) };
      drawPhaseGroups(app.def, app.pos);
      app.defKey = defKey(app.def);
      markDirty();
    }
    app.nodeDrag = null;
  }
  if (app.link) {
    const from = app.link.from;
    app.link.path.remove();
    app.link = null;
    let targetId = null;
    const hitEl = document.elementFromPoint(ev.clientX, ev.clientY);
    const g = hitEl && hitEl.closest ? hitEl.closest('.node') : null;
    if (g) targetId = g.getAttribute('data-id');
    if (targetId) connectNodes(from, targetId);
  }
}

function reachable(start, goal) {
  const seen = new Set(), q = [start];
  while (q.length) {
    const cur = q.shift();
    if (cur === goal) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    app.def.edges.filter(e => e.source === cur).forEach(e => q.push(e.target));
  }
  return false;
}

function connectNodes(src, dst) {
  if (!src || !dst) return;
  if (src === dst) { toast('不能连到自己', true); return; }
  if (app.def.edges.some(e => e.source === src && e.target === dst)) {
    toast('这条连线已存在', true); return;
  }
  if (reachable(dst, src)) { toast('该连线会形成环依赖，已阻止', true); return; }
  app.def.edges.push({ source: src, target: dst });
  const t = app.def.nodes.find(n => n.id === dst);
  if (t) { t.deps = t.deps || []; if (!t.deps.includes(src)) t.deps.push(src); }
  syncTaskDependencies();
  app.defKey = '';
  markDirty();
  render();
  toast(`${src} → ${dst} 已连接`);
}

function deleteSelection() {
  if (!isEditable() || !app.def) return;
  if (app.selEdge) {
    const [src, dst] = app.selEdge.split('>');
    app.def.edges = app.def.edges.filter(e => !(e.source === src && e.target === dst));
    const t = app.def.nodes.find(n => n.id === dst);
    if (t && t.deps) t.deps = t.deps.filter(d => d !== src);
    syncTaskDependencies();
    app.selEdge = null;
    toast(`已删除连线 ${src} → ${dst}`);
  } else if (app.selected) {
    const id = app.selected;
    const node = app.def.nodes.find(n => n.id === id);
    if (node && node.type === 'start' && app.def.nodes.filter(n => n.type === 'start').length <= 1) {
      toast('起点节点不能删除', true); return;
    }
    removeTaskForNode(node);
    app.def.nodes = app.def.nodes.filter(n => n.id !== id);
    app.def.edges = app.def.edges.filter(e => e.source !== id && e.target !== id);
    app.def.nodes.forEach(n => { if (n.deps) n.deps = n.deps.filter(d => d !== id); });
    syncTaskDependencies();
    app.selected = null;
    app.nodeById = {};
    toast(`已删除节点 ${id}`);
  } else {
    return;
  }
  app.defKey = '';
  markDirty();
  render();
}

function addNode(kind, role) {
  if (!isEditable() || !app.def) return;
  let k = 1;
  const ids = new Set(app.def.nodes.map(n => n.id));
  while (ids.has('N' + k)) k++;
  const id = 'N' + k;
  const tpl = {
    agent: { name: '新任务', role: role || 'coder', max_retries: 2, out_schema: 'default-v1' },
    condition: { name: '条件判定', role: 'qa', max_retries: 0, expression: 'qa_score >= 80' },
    approval: { name: '人工审批', role: 'human', max_retries: 0 },
    start: { name: '入口', role: 'system', max_retries: 0 },
    end: { name: '结束', role: 'system', max_retries: 0 }
  }[kind] || { name: '新节点', role: 'coder', max_retries: 0 };
  const cx = app.vb.x + app.vb.w / 2 - NW / 2;
  const cy = app.vb.y + app.vb.h / 2 - NH / 2;
  app.def.nodes.push(Object.assign({
    id, type: kind, deps: [], pos: { x: Math.round(cx), y: Math.round(cy) }
  }, tpl));
  app.defKey = '';
  markDirty();
  app.selected = id;
  render();
  toast(`已添加${tpl.name}（${id}），请连线并保存草稿`);
}

function editNode(id, field, val) {
  const n = app.def && app.def.nodes.find(x => x.id === id);
  if (!n) return;
  n[field] = val;
  if (n.task_id && app.def.tasks) {
    const t = app.def.tasks.find(x => x.id === n.task_id);
    if (t) {
      if (field === 'name') {
        const m = String(val).match(/^T\d+\s+(.+)$/);
        t.title = m ? m[1] : String(val);
      }
      if (field === 'role') {
        t.role = ({ architect: '架构师', coder: '后端', qa: '测试' })[val] || t.role;
        t.out_schema = ({ architect: 'design-v1', coder: 'code-result-v1', qa: 'test-v1' })[val]
          || t.out_schema;
        n.out_schema = t.out_schema;
      }
    }
  }
  const g = app.nodeEls[id];
  if (g) {
    if (field === 'name') g.__name.textContent = truncate(val, 13);
    if (field === 'role') {
      g.querySelector('.role-bar').setAttribute('fill', roleColor(val));
      g.__meta.textContent = id + ' · ' + roleLabel(val);
    }
  }
  if (field === 'type') { renderDetail(true); }
  app.defKey = defKey(app.def);
  markDirty();
}

/* ------------------------------------------------------------ 版本操作 */
function setMode(on) {
  app.nodeEdit = null;
  if (on) {
    if (app.run.status !== 'IDLE') { toast('运行中不可编辑，请先重置', true); $('#tglEdit').checked = false; return; }
    if (app.viewVersion !== 'draft') {
      toast('已发布版本不可编辑，请先切回草稿', true);
      $('#tglEdit').checked = false; return;
    }
    const def = app.editor && app.editor.draft && app.editor.draft.definition;
    if (!def) { toast('编辑器未就绪', true); return; }
    // 冻结当前自动布局，之后按手工坐标渲染
    const { pos } = computeLayout(def);
    def.nodes.forEach(n => { if (!n.pos) n.pos = { x: Math.round(pos[n.id].x), y: Math.round(pos[n.id].y) }; });
    app.mode = 'edit';
    app.defKey = '';
    app.needFit = true;
  } else {
    app.mode = 'view';
  }
  render();
  setFoot(app.mode === 'edit'
    ? '编辑态：拖拽移动、圆点连线、Delete 删除，改完点「保存草稿」'
    : '只读视图：勾选「编辑画布」可修改草稿');
}

async function doSave() {
  if (!app.editor || !app.def) return;
  app.errors = [];
  const r = await api(`/api/workflows/${app.editor.workflow_id}`,
                      { definition: app.def, name: app.editor.name }, 'PUT');
  if (r.ok) {
    app.dirty = false;
    await loadEditor();
    app.errors = [];
    toast('草稿已保存（基于 ' + app.editor.draft.base_version + '）');
  } else {
    app.errors = r.errors || [];
    const first = app.errors[0];
    toast(first ? `校验未通过：${first.msg}` : (r.message || '保存失败'), true);
  }
  render();
}

async function doPublish() {
  if (!app.editor || !app.def) return;
  app.errors = [];
  const r = await api(`/api/workflows/${app.editor.workflow_id}/publish`,
                      { note: '画布编辑发布' });
  if (r.ok) {
    app.dirty = false;
    app.versionDefs = {};
    await loadEditor();
    app.viewVersion = 'draft';
    app.defKey = '';
    toast('已发布 ' + r.version + '，后续运行可指定该版本');
    await refresh();
  } else {
    app.errors = r.errors || [];
    const first = app.errors[0];
    toast(first ? `发布前校验失败：${first.msg}` : (r.message || '发布失败'), true);
  }
  render();
}

async function switchVersion(label) {
  if (label === app.viewVersion) return;
  if (app.dirty && !confirm('草稿有未保存修改，切换后将丢失，继续？')) {
    $('#selVersion').value = app.viewVersion;
    return;
  }
  if (label === 'draft') {
    await loadEditor();
    app.viewVersion = 'draft';
    app.mode = app.mode;           // 保持编辑态开关
    app.defKey = '';
    app.needFit = true;
    render();
    return;
  }
  let def = app.versionDefs[label];
  if (!def && app.editor) {
    const wf = await getJSON('/api/workflows/' + app.editor.workflow_id);
    ((wf.workflow || {}).versions || []).forEach(v => app.versionDefs[v.version] = v.definition);
    def = app.versionDefs[label];
  }
  if (!def) { toast('版本加载失败', true); return; }
  app.viewVersion = label;
  app.mode = 'view';              // 已发布版本只读
  app.selected = null;
  app.selEdge = null;
  app.defKey = '';
  app.needFit = true;
  render();
  setFoot('只读预览：已发布版本 ' + label);
}

/* -------------------------------------------------------------- controls */
async function doStart() {
  const demo = $('#tglDemo').checked;
  if (app.dirty) { toast('草稿有未保存修改，请先保存再运行', true); return; }
  const payload = {
    speed: parseFloat($('#selSpeed').value),
    demo: demo,
    qa_mode: demo ? 'auto' : 'pass'
  };
  let r;
  if (app.viewVersion !== 'draft' && app.editor) {
    r = await api(`/api/workflows/${app.editor.workflow_id}/runs`,
                  Object.assign({ version: app.viewVersion }, payload));
  } else {
    r = await api('/api/run/start', payload);
  }
  if (r.ok === false) {
    if (r.errors && r.errors[0]) toast('定义校验未通过：' + r.errors[0].msg, true);
    else if (r.message) toast(r.message, true);
    return;
  }
  app.selected = null;
  app.selEdge = null;
  app.mode = 'view';
  setFoot('运行已启动：' + (demo ? '演示模式（首次失败 + 评审不通过）' : '顺利模式（全部通过）'));
  await refresh();
}
async function doPauseResume() {
  const s = app.run.status;
  if (s === 'RUNNING') { await api('/api/run/pause'); setFoot('已暂停：停止派发新任务，在途任务继续收敛'); }
  else if (s === 'PAUSED' || s === 'RECONCILING') { await doResume(); }
}
async function doResume() {
  const r = await api('/api/run/resume');
  if (r && r.ok === false) toast(r.message || '暂不能恢复', true);
  else { setFoot('运行已恢复'); refresh(); }
}
async function doReset() {
  await api('/api/run/reset');
  app.selected = null; app.selEdge = null;
  app.needFit = true;
  await refresh();
  setFoot('已重置为未启动状态');
}
async function doApprove(id, decision) {
  const box = $('#apComment');
  const comment = String((box ? box.value : app.apComment) || '').trim();
  app.apComment = '';           // 提交后清空，避免带到下一个审批节点
  const r = await api(`/api/nodes/${id}/approval`, { decision, comment });
  if (r && r.ok === false) toast(r.message || '审批未执行', true);
  else { toast(decision === 'approve' ? '已批准，流程继续' : '已打回，等待修改'); refresh(); }
}
function doRetry(id) { api(`/api/nodes/${id}/retry`); }
function doSkip(id) { api(`/api/nodes/${id}/skip`); }
async function doConfirm(id) {
  const r = await api(`/api/nodes/${id}/confirm`);
  if (r && r.ok === false) toast(r.message || '核对未执行', true);
  else { toast('已确认该节点成功（未重复执行）'); refresh(); }
}

async function doValidate() {
  if (!app.def) return;
  const r = await api('/api/validate', { workflow: app.def });
  const st = r.stats || {};
  // 负例：让首尾成环，验证环检测（图里没有终点节点时跳过负例）
  let negMsg = '';
  const tail = app.def.nodes.find(n => n.type === 'end');
  const head = app.def.nodes.find(n => n.type === 'start') || app.def.nodes[0];
  if (tail && head) {
    const bad = JSON.parse(JSON.stringify(app.def));
    bad.nodes.find(n => n.id === head.id).deps.push(tail.id);
    bad.edges.push({ source: tail.id, target: head.id });
    const rb = await api('/api/validate', { workflow: bad });
    const negErr = rb.errors[0] || {};
    negMsg = '　｜　负例（尾→头成环）已拦截：' +
      (negErr.code ? negErr.code + ' ' : '') + (negErr.msg || '检测到环依赖');
  }
  const msg = r.valid
    ? `校验通过：${st.nodes} 节点 / ${st.edges} 边 / 关键路径 ${(st.critical_path || []).length} 层` + negMsg
    : '校验未通过：' + (r.errors[0] || {}).msg;
  toast(msg, !r.valid);
  app.errors = r.valid ? [] : (r.errors || []);
  renderNodes();
}

/* ------------------------------------------------------------ pan / zoom */
function applyVB() {
  $('#canvas').setAttribute('viewBox', `${app.vb.x} ${app.vb.y} ${app.vb.w} ${app.vb.h}`);
}
function fit() {
  const svg = $('#canvas');
  const r = svg.getBoundingClientRect();
  if (!r.width) return;
  const pad = 26;
  const s = Math.min((r.width - pad * 2) / app.world.w, (r.height - pad * 2) / app.world.h, 1.15);
  const w = r.width / s, h = r.height / s;
  app.vb = { x: app.world.w / 2 - w / 2, y: app.world.h / 2 - h / 2, w, h };
  applyVB();
}
function zoom(factor) {
  const svg = $('#canvas'), r = svg.getBoundingClientRect();
  const cx = app.vb.x + app.vb.w / 2, cy = app.vb.y + app.vb.h / 2;
  const w = Math.max(320, Math.min(app.world.w * 2.4, app.vb.w * factor));
  const h = w * (r.height / r.width);
  app.vb = { x: cx - w / 2, y: cy - h / 2, w, h };
  applyVB();
}

function bindCanvas() {
  const svg = $('#canvas');
  svg.addEventListener('wheel', (e) => {
    e.preventDefault();
    const r = svg.getBoundingClientRect();
    const px = app.vb.x + (e.clientX - r.left) / r.width * app.vb.w;
    const py = app.vb.y + (e.clientY - r.top) / r.height * app.vb.h;
    const k = e.deltaY > 0 ? 1.1 : 1 / 1.1;
    const nw = Math.max(320, Math.min(app.world.w * 2.4, app.vb.w * k));
    const ratio = nw / app.vb.w;
    const nh = app.vb.h * ratio;
    app.vb = { x: px - (px - app.vb.x) * ratio, y: py - (py - app.vb.y) * ratio, w: nw, h: nh };
    applyVB();
  }, { passive: false });

  svg.addEventListener('pointerdown', (e) => {
    if (e.target.closest('.node')) return;
    if (app.nodeDrag || app.link) return;
    app.pressNode = false;                 // 空白处起手 → 允许随后的 click 取消选中
    app.drag = { x: e.clientX, y: e.clientY, vb: Object.assign({}, app.vb) };
    svg.classList.add('dragging');
    try { svg.setPointerCapture(e.pointerId); } catch (err) {}
  });
  svg.addEventListener('pointermove', (e) => {
    if (onPointerMove(e)) return;
    if (!app.drag) return;
    const r = svg.getBoundingClientRect();
    const dx = (e.clientX - app.drag.x) / r.width * app.vb.w;
    const dy = (e.clientY - app.drag.y) / r.height * app.vb.h;
    app.vb.x = app.drag.vb.x - dx;
    app.vb.y = app.drag.vb.y - dy;
    applyVB();
  });
  const end = (e) => {
    if (app.nodeDrag || app.link) { onPointerUp(e); return; }
    if (app.drag) app.moved = Math.hypot(e.clientX - app.drag.x, e.clientY - app.drag.y) > 4;
    app.drag = null;
    svg.classList.remove('dragging');
  };
  svg.addEventListener('pointerup', end);
  svg.addEventListener('pointercancel', end);
  svg.addEventListener('click', (e) => {
    // 节点上的点击可能被指针捕获改派到这里，不能当成"点空白"
    if (app.pressNode) { app.pressNode = false; return; }
    if (e.target.closest('.node') || e.target.closest('.edge-hit') || e.target.closest('.edge')) return;
    if (app.moved) { app.moved = false; return; }   // 平移后不取消选中
    app.selected = null; app.selEdge = null;
    renderNodes(); renderEdges(); renderDetail(true);
  });
}

/* ------------------------------------------------------------------ bind */
function bindUI() {
  $('#btnStart').onclick = doStart;
  $('#btnPause').onclick = doPauseResume;
  $('#btnReset').onclick = doReset;
  $('#btnValidate').onclick = doValidate;
  $('#btnSave').onclick = doSave;
  $('#btnPublish').onclick = doPublish;
  $('#tglEdit').onchange = (e) => setMode(e.target.checked);
  $('#selVersion').onchange = (e) => switchVersion(e.target.value);
  $('#specSel').onchange = (e) => openSpecById(e.target.value);
  const specTpl = $('#selSpecTemplate');
  if (specTpl) specTpl.onchange = (e) => changeSpecTemplate(e.target.value);
  $('#btnZoomIn').onclick = () => zoom(0.82);
  $('#btnZoomOut').onclick = () => zoom(1.22);
  $('#btnFit').onclick = fit;
  $('#selSpeed').onchange = (e) => api('/api/config', { speed: parseFloat(e.target.value) });
  $('#tglDemo').onchange = (e) => api('/api/config', { demo: e.target.checked });
  $('#tglFail').onchange = (e) => api('/api/config', { fail_mode: e.target.checked ? 'persistent' : 'off' });

  $$('.pal').forEach(b => b.onclick = () => addNode(b.dataset.kind, b.dataset.role));

  $$('.tab').forEach(t => t.onclick = () => {
    $$('.tab').forEach(x => x.classList.toggle('active', x === t));
    const tab = t.dataset.tab;
    $('#paneDetail').hidden = tab !== 'detail';
    $('#paneTimeline').hidden = tab !== 'timeline';
    const files = $('#paneFiles');
    if (files) files.hidden = tab !== 'files';
    if (tab === 'files') loadRunFiles();
  });

  document.addEventListener('keydown', (e) => {
    const typing = /^(INPUT|SELECT|TEXTAREA)$/.test((e.target.tagName || ''));
    if (e.key === 'Escape') {
      app.selected = null; app.selEdge = null;
      renderNodes(); renderEdges(); renderDetail(true);
      return;
    }
    if (typing) return;
    if ((e.key === 'Delete' || e.key === 'Backspace') && isEditable()) {
      e.preventDefault();
      deleteSelection();
    }
  });
  window.addEventListener('resize', () => { if (app.built) applyVB(); });
  // 兜底：任何未捕获的请求失败都给出明确提示，而不是静默失败
  window.addEventListener('unhandledrejection', (ev) => {
    const r = ev && ev.reason;
    showNetError('请求失败：' + ((r && r.message) || r || '未知错误'));
  });
}

/* --------------------------------------------------------- 设计规格（spec） */
function openSpec() {
  syncDiscussUI();
  if (app.frozen) { toast('历史回看中，先点「返回当前」', true); return; }
  $('#specPanel').hidden = false;
  $('#reqPanel').hidden = true;
  $('#histPanel').hidden = true;
  syncSpecTemplate();               // 模板选择器与说明先按当前草稿对好
  if (!app.specs.length) loadSpecs();
  else syncSpecContext();
}
function closeSpec() {
  // 关面板 = "直接退出预览"：画布必须回到之前那张图，不能把预览留在屏幕上
  cancelPreview({ silent: true });
  $('#specPanel').hidden = true;
}

/* 面板里的规格和画布上的草稿可能不是同一份：说清楚当前顶栏显示的是谁 */
function syncSpecContext() {
  const note = $('#specNote');
  if (!note) return;
  if (app.preview) {
    note.textContent = '预览中 · 画布上是「' + ((app.preview.spec && app.preview.spec.name) || '')
      + '」的分解结果，尚未保存';
    return;
  }
  const def = app.editor && app.editor.draft && app.editor.draft.definition;
  const draftSpec = (def && def.spec_id) || '';
  if (!app.specId) return;
  if (!draftSpec) {
    note.textContent = '画布上还不是规格编排出来的图；点「分解预览」可以先看这份规格的流程图';
  } else if (app.specId !== draftSpec) {
    const meta = (app.specs || []).find(s => s.id === draftSpec);
    note.textContent = '画布上仍是「' + truncate(meta ? meta.name : draftSpec, 20)
      + '」编排的图；点「分解预览」看这份规格生成的流程图';
  } else {
    note.textContent = '';
  }
}

/* 面板底部按钮随状态变化：预览中，「分解预览」变成「取消分解预览」 */
function syncSpecButtons() {
  const d = $('#btnSpecDecompose');
  if (!d) return;
  const busy = app.run.status && app.run.status !== 'IDLE';
  d.textContent = app.preview ? '取消分解预览' : '分解预览';
  d.classList.toggle('warn', !!app.preview);
  d.title = app.preview
    ? '退出预览，画布回到之前那张图'
    : '把规格拆成任务，并把生成的流程图直接画在画布上（不保存）';
  d.disabled = !app.preview && !app.specId;
  const p = $('#btnSpecPlan');
  if (p) {
    p.textContent = app.preview ? '就按这个编排' : '分解并编排';
    p.disabled = busy;
    p.title = app.preview
      ? '把预览的这张图保存成草稿（可编辑、可发布）'
      : '按已保存的规格和当前模板生成 DAG 草稿';
  }
  const s = $('#btnSpecSave');
  if (s) s.disabled = !!app.preview;
}

async function loadSpecs(selectId) {
  try {
    const r = await getJSON('/api/specs');
    app.specs = r.specs || [];
  } catch (e) {
    app.specs = [];
    $('#specNote').textContent = '读取规格失败：' + (e && e.message ? e.message : e);
  }
  const sel = $('#specSel');
  sel.innerHTML = app.specs.map(s =>
    `<option value="${esc(s.id)}">${esc(s.name)} · ${s.module_count} 模块</option>`).join('');
  if (!app.specs.length) { newSpec(); return; }
  const pick = selectId && app.specs.some(s => s.id === selectId) ? selectId : app.specs[0].id;
  await openSpecById(pick);
}

async function openSpecById(id) {
  if (!id) return;
  if (app.preview) cancelPreview({ silent: true });   // 换规格 = 放弃当前预览
  try {
    const r = await getJSON('/api/specs/' + encodeURIComponent(id));
    renderSpec(r.spec);
  } catch (e) {
    toast('加载规格失败：' + (e && e.message ? e.message : e), true);
  }
}

function renderSpec(spec) {
  app.specId = spec.id || null;
  $('#specName').value = spec.name || '';
  $('#specSummary').value = spec.summary || '';
  $('#specBody').value = spec.body || '';
  $('#specConstraints').value = (spec.constraints || []).join('\n');
  syncConstraintCount();
  app.specModules = (spec.modules || []).map(m => ({
    id: m.id || '', name: m.name || '', goal: m.goal || '',
    priority: m.priority || 'P1', acceptance: m.acceptance || []
  }));
  renderSpecModules();
  $('#specMeta').textContent = spec.updated_at
    ? `r${spec.revision || 1} · 更新于 ` + spec.updated_at.replace('T', ' ')
    : '新规格（尚未保存）';
  // 退出"新建"状态：收起取消按钮、去掉临时占位项
  const back = $('#btnSpecBack');
  if (back) back.hidden = true;
  const ph = document.getElementById('specNewOpt');
  if (ph) ph.remove();
  clearTasks();
  const sel = $('#specSel');
  if (sel && spec.id) sel.value = spec.id;
  syncSpecTemplate();
  syncSpecButtons();
  syncSpecContext();
}

/* 从"新建"退回已保存的规格：新建表单本身不该是个没有出口的死胡同 */
async function backToSavedSpec() {
  const from = app.specNewFrom;
  app.specNewFrom = null;
  app.specId = null;
  const back = $('#btnSpecBack');
  if (back) back.hidden = true;
  if (!(app.specs || []).length) {
    toast('还没有已保存的规格，填完点「保存规格」即可', true);
    return;
  }
  await loadSpecs(from || undefined);
}

function newSpec() {
  if (app.preview) cancelPreview({ silent: true });
  const sel = $('#specSel');
  app.specNewFrom = app.specId || (sel && sel.value) || null;
  app.specId = null;
  app.specModules = [{ id: '', name: '', goal: '', priority: 'P0', acceptance: [] }];
  $('#specName').value = '';
  $('#specSummary').value = '';
  $('#specBody').value = '';
  $('#specConstraints').value = '';
  syncConstraintCount();
  $('#specMeta').textContent = '新规格（尚未保存）';
  $('#specNote').textContent = '';
  clearTasks();
  renderSpecModules();
  // 下拉框加一个占位项，否则它仍显示上一份规格，看起来像"新建没生效"
  if (sel) {
    let opt = document.getElementById('specNewOpt');
    if (!opt) {
      opt = document.createElement('option');
      opt.id = 'specNewOpt';
      opt.value = '';
      opt.textContent = '新建规格（未保存）';
      sel.insertBefore(opt, sel.firstChild);
    }
    sel.value = '';
  }
  const back = $('#btnSpecBack');
  if (back) back.hidden = false;
  syncSpecButtons();
  $('#specName').focus();
}

function renderSpecModules() {
  const box = $('#specModules');
  const list = app.specModules || [];
  if (!list.length) {
    box.innerHTML = '<div class="empty-mod">还没有模块。点下面「+ 添加模块」，每个模块写一行验收条款。</div>';
    return;
  }
  box.innerHTML = list.map((m, i) => `
    <div class="mod" data-i="${i}" data-id="${esc(m.id || '')}">
      <div class="mod-head">
        <span class="mod-no" title="稳定 ID：${esc(m.id || '保存后生成')}">M${i + 1}</span>
        <input class="field" data-f="name" maxlength="60" value="${esc(m.name || '')}"
               placeholder="模块名，例：账号与登录">
        <select class="select sm" data-f="priority" title="优先级">
          ${['P0', 'P1', 'P2'].map(p =>
            `<option${(m.priority || 'P1') === p ? ' selected' : ''}>${p}</option>`).join('')}
        </select>
        <button class="mod-x" title="删除该模块" onclick="removeSpecModule(${i})">×</button>
      </div>
      <input class="field" data-f="goal" maxlength="200" value="${esc(m.goal || '')}"
             placeholder="目标（可选）">
      <textarea class="field" data-f="acceptance" rows="2"
                placeholder="验收条款，每行一条">${esc((m.acceptance || []).join('\n'))}</textarea>
    </div>`).join('');
}

function collectSpecModules() {
  const rows = $('#specModules').querySelectorAll('.mod');
  return Array.prototype.map.call(rows, (row) => ({
    id: row.getAttribute('data-id') || '',
    name: row.querySelector('[data-f="name"]').value.trim(),
    priority: row.querySelector('[data-f="priority"]').value,
    goal: row.querySelector('[data-f="goal"]').value.trim(),
    acceptance: row.querySelector('[data-f="acceptance"]').value
  }));
}

function addSpecModule() {
  app.specModules = collectSpecModules().concat([
    { id: '', name: '', goal: '', priority: 'P1', acceptance: [] }
  ]);
  renderSpecModules();
  const rows = $('#specModules').querySelectorAll('.mod');
  if (rows.length) rows[rows.length - 1].querySelector('[data-f="name"]').focus();
}

function removeSpecModule(i) {
  const list = collectSpecModules();
  list.splice(i, 1);
  app.specModules = list;
  renderSpecModules();
}

async function saveSpec() {
  const payload = {
    name: $('#specName').value.trim(),
    summary: $('#specSummary').value.trim(),
    body: $('#specBody').value.trim(),
    constraints: $('#specConstraints').value,
    modules: collectSpecModules()
  };
  if (!payload.name) { toast('先给规格起个名字', true); $('#specName').focus(); return; }
  const btn = $('#btnSpecSave'), note = $('#specNote');
  btn.disabled = true; btn.textContent = '保存中…';
  try {
    const r = app.specId
      ? await api('/api/specs/' + encodeURIComponent(app.specId), payload, 'PUT')
      : await api('/api/specs', payload);
    if (!r.ok) { note.textContent = r.message || '保存失败'; return; }
    clearTasks();
    toast('设计规格已保存');
    await loadSpecs(r.spec.id);
    note.textContent = `已保存 · ${(r.spec.modules || []).length} 个模块 · ${r.spec.id}`;
    setFoot('规格「' + r.spec.name + '」已保存 · 下一步由 PM Agent 分解成任务');
  } catch (e) {
    note.textContent = '保存失败：' + (e && e.message ? e.message : e);
  } finally {
    btn.disabled = false; btn.textContent = '保存规格';
  }
}

/* ---------------- 规格 → 任务清单 + 流程图预览（PM Agent 分解预览） ----------------
 * 预览是"只读预演"：调 /preview 拿一份编排好的定义，直接画在画布上覆盖原来的图，
 * 服务器不落库、草稿和版本都不动，所以取消预览只需要把画布刷回服务端状态。
 */
async function decomposeSpec() {
  if (app.preview) { await cancelPreview(); return; }
  if (!app.specId) { toast('先保存规格，再预览', true); return; }
  // 预览会把画布换成预演图、退出时从服务端重建：先确保手上没有未保存的改动
  if (app.dirty) { toast('画布上有未保存的修改，先保存再预览', true); return; }
  if (app.run.status && app.run.status !== 'IDLE') {
    toast('运行进行中，画布在跟随运行状态，稍后再预览', true); return;
  }
  await previewSpec();
}

/* 真正去拉一次预演（也用于切换模板后重画） */
async function previewSpec() {
  const btn = $('#btnSpecDecompose'), note = $('#specNote');
  const tplSel = $('#selSpecTemplate');
  const template = (tplSel && tplSel.value) || app.specTemplate || 'default';
  btn.disabled = true; btn.textContent = app.preview ? '重新生成…' : '分解中…';
  try {
    const r = await api('/api/specs/' + encodeURIComponent(app.specId) + '/preview', { template });
    if (!r.ok) { note.textContent = r.message || '分解预览失败'; return; }
    const specName = (r.spec && r.spec.name) || '';
    const g = r.graph || {};
    const tplName = ((app.templates || []).find(t => t.id === template) || {}).name || template;
    startPreview(r);
    app.specTemplate = template;
    renderSpecTplDesc();
    note.textContent = `预览中 · ${r.tasks.length} 条任务 → ${g.nodes || 0} 节点 · ${tplName} · 尚未保存`;
    setFoot(`预览「${specName}」× ${tplName} · ${g.nodes || 0} 节点 / ${g.edges || 0} 边 · 未保存，可点「取消分解预览」撤回`);
  } catch (e) {
    note.textContent = '分解预览失败：' + (e && e.message ? e.message : e);
  } finally {
    btn.disabled = false;
    syncSpecButtons();
  }
}

/* 把预演出来的定义接到画布上：节点全部按"待运行"渲染，顶栏标成预览 */
function startPreview(r) {
  const def = r.definition;
  const wasPreviewing = !!app.preview;
  app.preview = {
    spec: r.spec || {}, definition: def, tasks: r.tasks || [],
    trace: r.trace || [], stats: r.stats || {}, template: r.template
  };
  app.tasks = r.tasks || [];
  if (!wasPreviewing) app.previewMode = app.mode;   // 只在首次进入预览时记住原模式（换模板重画不算）
  app.mode = 'view';               // 预览只读，不允许拖动/编辑
  app.dirty = false;
  app.defKey = '';
  app.selected = null;
  app.selEdge = null;
  // 节点全部按"待运行"渲染，同时保留定义里的字段（详情面板要读 out_schema 等）
  app.nodes = (def.nodes || []).map(n => Object.assign(
    { status: 'PENDING', message: '预览 · 尚未运行', duration: 0, retry_count: 0,
      gate: {}, output: null, logs: [], attempts: [], branch_taken: null }, n));
  app.nodeById = {};
  app.nodes.forEach(n => app.nodeById[n.id] = n);
  app.edges = (def.edges || []).map(e => ({
    source: e.source, target: e.target, label: e.label || '', active: true
  }));
  renderTasks({ tasks: r.tasks, stats: r.stats, trace: r.trace });
  syncSpecButtons();
  render();
}

/* 退出预览：画布回到原来的图（草稿或所选版本），任务清单一起收起 */
async function cancelPreview(opts) {
  const o = opts || {};
  if (!app.preview) return false;
  app.preview = null;
  app.mode = app.previewMode || 'view';
  app.defKey = '';
  app.dirty = false;
  app.selected = null;
  app.selEdge = null;
  clearTasks();
  syncSpecButtons();
  try { await refresh(); } catch (e) { /* 离线时保留当前画面 */ }
  render();                        // 从服务端快照重建 app.nodes / app.edges / app.def
  if (!o.silent) {
    setFoot('已取消分解预览 · 画布回到之前那张图');
    toast('已退出预览，画布已还原');
  }
  if ($('#specPanel') && !$('#specPanel').hidden) syncSpecContext();
  return true;
}

function taskRowHTML(t) {
  const files = (t.files || []).map(f => `<span class="tc-file">${esc(f)}</span>`).join('');
  const steps = (t.steps || []).map(s =>
    `<li><b>${esc(s.title || '')}</b> — ${esc(s.how || '')}</li>`).join('');
  const detail = steps
    ? `<details><summary>展开 ${(t.steps || []).length} 个子步骤</summary><ol>${steps}</ol></details>`
    : '';
  return `<tr>
      <td class="tt-state">待编排</td>
      <td class="tt-id">${esc(t.id)}</td>
      <td class="tt-title">${esc(t.title)}
        ${t.module ? `<div class="muted" style="font-size:10.5px">${esc(t.module)}</div>` : ''}
        ${t.part ? `<div><span class="part-tag" style="border-color:${esc(partMeta(t.part).color)}66;`
          + `color:${esc(partMeta(t.part).color)}">${esc(partMeta(t.part).label)}</span>`
          + `${t.ai ? `<span class="ai-tag">🤖 ${esc(t.ai)}</span>` : ''}</div>` : ''}</td>
      <td class="tt-role">${esc(t.role)}</td>
      <td class="tt-deps">${esc((t.depends_on || []).join(',') || '—')}</td>
      <td class="tt-est">${t.estimate}</td>
      <td class="tt-what">
        <div class="tc-what"><b>做什么：</b>${esc(t.what || '')}</div>
        <div class="tc-how"><b>怎么做：</b>${esc(t.how || '')}</div>
        ${files ? `<div class="tc-files">${files}</div>` : ''}
        ${detail}
      </td>
      <td class="tt-acc">${taskAcceptanceHTML(t)}</td>
      <td class="tt-source">${taskRefHTML(t)}</td>
    </tr>`;
}

function renderTasks(r) {
  const tasks = r.tasks || [], stats = r.stats || {};
  $('#specDecomp').hidden = false;
  const ph = stats.phases || {};
  $('#specStats').textContent =
    `${stats.modules} 模块 · ${stats.tasks} 条任务 · ${stats.steps || 0} 个子步骤 · `
    + `${stats.estimate} 人日 · 最长 ${stats.depth} 层`;
  const phText = PHASE_ORDER.filter(p => ph[p]).map(p => `${p} ${ph[p]}`).join(' · ');
  $('#specTrace').innerHTML = (r.trace || [])
    .map(t => `<div class="trace-line">· ${esc(t)}</div>`).join('')
    + (phText ? `<div class="trace-line">· 阶段：${esc(phText)}</div>` : '');

  // 按阶段分组：阶段 → 任务 → 子步骤，三层都在这一张表里
  const groups = {};
  tasks.forEach(t => {
    const p = t.phase || '编码';
    (groups[p] = groups[p] || []).push(t);
  });
  const order = PHASE_ORDER.filter(p => groups[p])
    .concat(Object.keys(groups).filter(p => PHASE_ORDER.indexOf(p) < 0));
  let rows = '';
  order.forEach(p => {
    const idx = PHASE_ORDER.indexOf(p) + 1;
    const days = groups[p].reduce((a, t) => a + (t.estimate || 0), 0);
    rows += `<tr class="tt-phase"><td colspan="9">
        <span class="phase-dot p${idx}"></span>${esc(p)}阶段
        <span class="muted">· ${groups[p].length} 条 · ${days} 人日</span>
      </td></tr>`;
    groups[p].forEach(t => { rows += taskRowHTML(t); });
  });
  $('#specTasks').innerHTML =
    '<thead><tr><th>状态</th><th>ID</th><th>任务</th><th>角色</th><th>依赖</th>'
    + '<th>人日</th><th>做什么 / 怎么做</th><th>验收标准</th><th>规格出处</th></tr></thead>'
    + `<tbody>${rows}</tbody>`;
  const box = $('#specDecomp');
  if (box.scrollIntoView) box.scrollIntoView({ block: 'nearest' });
}

function clearTasks() {
  $('#specDecomp').hidden = true;
  $('#specStats').textContent = '';
  $('#specTrace').innerHTML = '';
  $('#specTasks').innerHTML = '';
}

/* 规格 → 任务清单 → DAG 草稿：编排结果直接成为画布上的工作流（从预览里的"就按这个编排"进来） */
async function planSpec() {
  if (!app.specId) { toast('先保存规格，再编排', true); return; }
  const btn = $('#btnSpecPlan'), note = $('#specNote');
  btn.disabled = true; btn.textContent = '编排中…';
  try {
    const tplSel = $('#selSpecTemplate');
    const template = (tplSel && tplSel.value) || app.specTemplate || 'default';
    const r = await api('/api/specs/' + encodeURIComponent(app.specId) + '/plan',
                        { template });
    if (!r.ok) { note.textContent = r.message || '编排失败'; return; }
    if (app.preview) {            // 落库成功后预览就变成正式草稿，撤掉预览壳
      app.preview = null;
      app.mode = app.previewMode || 'view';
      app.defKey = '';
      clearTasks();
    }
    app.tasks = r.tasks || [];
    app.specTemplate = template;
    await loadEditor();
    closeSpec();
    await refresh();
    syncSpecTemplate();
    setFoot(`已按规格编排 ${r.tasks.length} 条任务 · ${r.stats.nodes} 节点 / ${r.stats.edges} 边 · 模板 ${
      ((app.templates || []).find(t => t.id === template) || {}).name || template}`);
    toast('已编排成工作流草稿');
  } catch (e) {
    note.textContent = '编排失败：' + (e && e.message ? e.message : e);
  } finally {
    btn.disabled = false;
    syncSpecButtons();
  }
}

/* ------------------------------------------------------------------ boot */
function connect() {
  if (app.es) { try { app.es.close(); } catch (e) {} }
  let es;
  try { es = new EventSource('/api/stream'); } catch (e) { startPoll(); return; }
  app.es = es;
  const onPatch = (ev) => {
    let p;
    try { p = JSON.parse(ev.data); } catch (e) { return; }
    if (p && p.version != null) applyPatch(p);
  };
  es.onopen = () => { stopPoll(); clearNetError(); setFoot('实时连接已建立 · 状态来自服务端事件流'); };
  // 服务端用命名事件 "patch" 推送增量，同时兼容默认 message
  es.addEventListener('patch', onPatch);
  es.onmessage = onPatch;
  es.onerror = () => {
    showNetError('实时连接已断开，正在自动重连；期间降级为 1.5s 轮询');
    startPoll();
  };
}

function startPoll() {
  if (app.poll) return;
  app.poll = setInterval(async () => {
    if (app.frozen) return;
    try {
      const s = await getJSON('/api/state');
      applySnapshot(s);
      clearNetError();
      stopPoll();
    } catch (e) { /* 继续轮询，故障条保持 */ }
  }, 1500);
}
function stopPoll() { if (app.poll) { clearInterval(app.poll); app.poll = null; } }

function retryNet() {
  clearNetError();
  setFoot('正在重试连接…');
  connect();
  refresh();
  loadEditor();
}

/* ------------------------------------------------------- 需求 / 生成工作流 */
function fmtTs(epoch) {
  if (!epoch) return '—';
  try { return new Date(epoch * 1000).toLocaleString('zh-CN', { hour12: false }); }
  catch (e) { return '—'; }
}

function openReq() {
  if (app.frozen) { toast('历史回看中，先点「返回当前」', true); return; }
  $('#reqPanel').hidden = false;
  $('#histPanel').hidden = true;
  const ta = $('#reqText');
  if (!ta.value) ta.value = (app.editor && app.editor.requirement) || '';
  ta.focus();
}
function closeReq() { $('#reqPanel').hidden = true; }
function fillReq(text) { $('#reqText').value = text; $('#reqText').focus(); }

function renderTrace(r) {
  const box = $('#reqTrace');
  const tags = ((r.matched || []).map(m => `<span class="trace-tag">${esc(m)}</span>`).join('') +
    (r.risk || []).map(x => `<span class="trace-tag risk">风险 · ${esc(x)}</span>`).join(''));
  const lines = (r.trace || []).map(t => `<div class="trace-item">${esc(t)}</div>`).join('');
  box.innerHTML = (tags ? `<div>${tags}</div>` : '') + lines ||
    (r.errors ? r.errors.map(e => `<div class="trace-item">${esc(e.code)}：${esc(e.msg)}</div>`).join('') : '');
}

async function doGenerate() {
  const ta = $('#reqText'), btn = $('#btnGen'), note = $('#reqNote');
  const text = (ta.value || '').trim();
  if (text.length < 6) { toast('需求至少写 6 个字，再具体一点', true); return; }
  if (app.dirty && !confirm('当前草稿有未保存修改，生成会覆盖草稿（已发布版本不受影响），继续？')) return;
  btn.disabled = true; btn.textContent = '生成中…'; note.textContent = '正在解析需求…';
  const tplSel = $('#selTemplate');
  const tpl = (tplSel && tplSel.value) || 'default';
  try {
    const r = await api('/api/workflows/generate', { requirement: text, template: tpl });
    renderTrace(r);
    if (!r.ok) {
      note.textContent = r.message || '生成失败';
      toast(r.message || '生成失败', true);
      return;
    }
    app.mode = 'view';
    app.viewVersion = 'draft';
    app.dirty = false;
    app.errors = [];
    $('#tglEdit').checked = false;
    // 服务端允许在终态后重新生成草稿；先回到 IDLE，避免画布仍冻结在上一轮运行快照。
    if (['SUCCESS', 'FAILED', 'CANCELLED'].includes(app.run.status)) {
      await api('/api/run/reset', {});
    }
    await loadEditor();
    await refresh();
    const n = (r.draft && r.draft.definition.nodes.length) || 0;
    note.textContent = `已生成 ${n} 个节点 / ${(r.draft && r.draft.definition.edges.length) || 0} 条边`;
    setFoot('已根据需求生成草稿 · 勾选「编辑画布」可调整，随后保存/发布/启动');
    toast('工作流草稿已生成');
  } catch (e) {
    note.textContent = '生成失败：' + (e && e.message ? e.message : e);
    showNetError('生成失败：' + (e && e.message ? e.message : '未知错误'));
  } finally {
    btn.disabled = false;
    btn.textContent = '生成工作流';
  }
}

/* ------------------------------------------------------------- 运行历史 */
async function openHist() {
  $('#histPanel').hidden = false;
  $('#reqPanel').hidden = true;
  $('#histList').innerHTML = '<div class="empty" style="padding:14px">加载中…</div>';
  try {
    const r = await getJSON('/api/runs');
    renderHistList(r.runs || []);
  } catch (e) {
    $('#histList').innerHTML = '<div class="empty" style="padding:14px">加载失败：' + esc(e.message) + '</div>';
  }
}
function closeHist() { $('#histPanel').hidden = true; }

function renderHistList(runs) {
  const box = $('#histList');
  if (!runs.length) {
    box.innerHTML = '<div class="empty" style="padding:14px">还没有已结束的运行。<br>启动一次运行并跑完（或取消），它会自动归档到这里。</div>';
    return;
  }
  const tone = { SUCCESS: 'pass', FAILED: 'fail', CANCELLED: 'skip', RUNNING: 'run', WAITING_HUMAN: 'wait' };
  box.innerHTML = runs.map(r => `
    <div class="hist-item" onclick="viewHistory('${esc(r.run_id)}')">
      <div class="hist-top">
        <span class="chip ${tone[r.status] || 'skip'}">${esc(RUN_STATUS[r.status] || r.status)}</span>
        <span class="hist-id">${esc(r.run_id)}</span>
        <span class="grow"></span>
        <span class="hist-sub">${esc(r.workflow_version || '—')}</span>
      </div>
      <div class="hist-sub">${fmtTs(r.ended_ts)} · 用时 ${fmtDur(r.elapsed)} · ${r.node_count} 节点 · ${r.event_count} 事件${
        (r.stats && r.stats.human_waits) ? ' · ' + r.stats.human_waits + ' 次人工操作' : ''}</div>
      <div class="hist-req">${esc(r.requirement || '（这次运行没有记录需求文本）')}</div>
    </div>`).join('');
}

async function viewHistory(runId) {
  try {
    const r = await getJSON('/api/runs/' + encodeURIComponent(runId));
    const it = r.run;
    if (!it || !it.definition) { toast('该记录缺少定义快照，无法回看', true); return; }
    app.frozen = true;
    app.histId = runId;
    app.mode = 'view';
    app.viewVersion = 'hist';
    app.selected = null; app.selEdge = null;
    app.roles = app.roles || {};
    app.def = it.definition;
    app.wf = it.definition;
    app.runWorkflow = it.definition;
    app.defKey = '';
    app.run = {
      run_id: it.run_id, workflow_name: it.workflow_name,
      workflow_version: it.workflow_version, requirement: it.requirement,
      status: it.status, elapsed: it.elapsed, started_at: it.started_at,
      finished_at: it.finished_at, stats: it.stats || {},
      concurrency: 3, speed: 1, demo: false, qa_mode: 'mix', fail_mode: 'off',
      seq: (it.events || []).length
    };
    app.nodes = it.nodes || [];
    app.nodeById = {};
    app.nodes.forEach(n => app.nodeById[n.id] = n);
    app.edges = it.edges || [];
    app.events = it.events || [];
    app.lastSeq = app.events.length ? app.events[app.events.length - 1].seq : 0;
    app.renderedSeq = 0;
    $('#timeline').innerHTML = '';
    appendEvents(app.events);
    app.needFit = true;
    render();
    $('#histBanner').hidden = false;
    $('#histBannerText').textContent =
      `正在回看 ${it.run_id} · ${it.workflow_version || '—'} · ${RUN_STATUS[it.status] || it.status} · ${fmtTs(it.ended_ts)}`;
    $('#histPanel').hidden = true;
    setFoot('历史回看为只读视图 · 点「返回当前」恢复实时状态');
  } catch (e) {
    toast('打开历史失败：' + (e && e.message ? e.message : e), true);
  }
}

async function exitHistory() {
  if (!app.frozen) return;
  app.frozen = false;
  app.histId = null;
  app.viewVersion = 'draft';
  app.defKey = '';
  app.selected = null; app.selEdge = null;
  app.needFit = true;
  $('#histBanner').hidden = true;
  await loadEditor();
  await refresh();
  setFoot('已返回当前状态');
}

/* ================================================================ 模型接入
 * 密钥只在服务端保存（data/settings.json），前端只拿到掩码提示；
 * 保存后 /api/state 的 settings.active 表示模型可用于讨论与真实执行；模拟运行不落盘代码。
 */
const PRESET_FALLBACK = [
  { id: 'openai', name: 'OpenAI', base_url: 'https://api.openai.com/v1', model: 'gpt-4o-mini' },
  { id: 'deepseek', name: 'DeepSeek', base_url: 'https://api.deepseek.com/v1', model: 'deepseek-chat' },
  { id: 'anthropic', name: 'Anthropic', base_url: 'https://api.anthropic.com', model: 'claude-sonnet-4-5' },
  { id: 'ollama', name: '本地 Ollama', base_url: 'http://127.0.0.1:11434/v1', model: 'qwen2.5-coder:14b' }
];

function presets() {
  const p = (app.settings && app.settings.presets) || null;
  return Array.isArray(p) && p.length ? p : PRESET_FALLBACK;
}

function openModel() {
  if (app.frozen) { toast('历史回看中，先点「返回当前」', true); return; }
  $('#modelPanel').hidden = false;
  $('#specPanel').hidden = true;
  $('#reqPanel').hidden = true;
  $('#histPanel').hidden = true;
  fillPresetSelect();
  if (app.settings) fillSettings(app.settings);
  else loadSettings();
  const aiSel = $('#aiProvider');
  if (aiSel && !aiSel.options.length) {
    aiSel.innerHTML = presets().map(p =>
      `<option value="${esc(p.id)}">${esc(p.label)}</option>`).join('');
  }
  loadAIs();
}

function closeModel() { $('#modelPanel').hidden = true; }

async function loadSettings() {
  try {
    const r = await getJSON('/api/settings');
    app.settings = r.settings;
    fillPresetSelect();
    fillSettings(r.settings);
    renderModeChip();
  } catch (e) {
    toast('读取模型配置失败：' + (e && e.message ? e.message : e), true);
  }
}

function fillPresetSelect() {
  const sel = $('#setProvider');
  if (!sel) return;
  const list = presets();
  const cur = (app.settings && app.settings.provider) || '';
  sel.innerHTML = list.map(p =>
    `<option value="${esc(p.id)}"${p.id === cur ? ' selected' : ''}>${esc(p.name)}</option>`).join('')
    + `<option value="custom"${list.some(p => p.id === cur) ? '' : ' selected'}>自定义</option>`;
}

function fillSettings(s) {
  if (!s) return;
  $('#setBaseUrl').value = s.base_url || '';
  $('#setModel').value = s.model || '';
  $('#setKey').value = '';
  $('#setKeyHint').textContent = s.has_key
    ? ('已保存 ' + (s.key_hint || '••••')) : '未保存（用于 AI 讨论与真实执行）';
  $('#setTemp').value = s.temperature != null ? s.temperature : 0.2;
  $('#setSteps').value = s.max_steps != null ? s.max_steps : 14;
  $('#setMaxTokens').value = s.max_tokens != null ? s.max_tokens : 4096;
  $('#setTimeout').value = s.timeout != null ? s.timeout : 180;
  $('#tglUseLLM').checked = s.mode === 'llm';
  const chip = $('#modelState');
  if (s.active) { chip.textContent = '已启用'; chip.className = 'mode-chip on'; }
  else if (s.has_key) { chip.textContent = '已配置 · 未启用'; chip.className = 'mode-chip'; }
  else { chip.textContent = '未配置密钥'; chip.className = 'mode-chip warn'; }
  renderAISection(s);
  const provSel = $('#setProvider');
  if (provSel && s.provider) {
    const hit = Array.prototype.some.call(provSel.options, o => o.value === s.provider);
    provSel.value = hit ? s.provider : 'custom';
  }
}

function applyPreset(id) {
  const p = presets().find(x => x.id === id);
  if (!p) return;
  $('#setBaseUrl').value = p.base_url || '';
  $('#setModel').value = p.model || '';
  if (!$('#setKey').value) $('#setKey').focus();
}

function onModeToggle() {
  if ($('#tglUseLLM').checked && !(app.settings && app.settings.has_key)
      && !$('#setKey').value.trim()) {
    $('#setKey').focus();
    toast('先填入 API Key（或选择本地 Ollama）再启用模型', true);
  }
}

/* ---------------- 多个 AI 接入 ---------------- */
const ROLE_AI_ROWS = [
  ['pm', '产品经理'], ['architect', '架构师'], ['coder', '开发者'],
  ['tester', '测试'], ['reviewer', '评审'],
];

function aiOptions(selected, emptyLabel) {
  const opts = [`<option value="">${esc(emptyLabel || '（用默认 AI）')}</option>`];
  app.ais.forEach(a => {
    opts.push(`<option value="${esc(a.id)}"${a.id === selected ? ' selected' : ''}>`
      + `${esc(a.label || a.model || a.id)}${a.has_key ? '' : '（未配密钥）'}</option>`);
  });
  return opts.join('');
}

function partMeta(pid) {
  return app.partMeta[pid] || { label: pid || '未分组', color: '#94a3b8' };
}

function renderAISection(s) {
  if (!s) return;
  app.ais = s.ais || app.ais;
  if (Array.isArray(s.parts)) {
    app.parts = s.parts;
    s.parts.forEach(p => { app.partMeta[p.id] = p; });
  }
  const list = $('#aiList');
  if (list) {
    list.innerHTML = app.ais.length ? app.ais.map(a => `
      <div class="ai-card" data-ai="${esc(a.id)}">
        <div>
          <div class="ai-name">${esc(a.label || a.model || a.id)}</div>
          <div class="ai-meta">${esc(a.provider || '')} · ${esc(a.model || '')}
            · ${esc(a.base_url || '')}</div>
          <div class="ai-key">${a.has_key ? '密钥 ' + esc(a.key_hint || '已保存') : '未保存密钥'}</div>
        </div>
        <div class="ai-actions">
          <button class="btn sm ghost" onclick="probeAI('${esc(a.id)}')">测试</button>
          <button class="btn sm ghost danger" onclick="deleteAI('${esc(a.id)}')">删除</button>
        </div>
      </div>`).join('')
      : '<div class="muted">还没有额外的 AI；下面可以直接加一个</div>';
  }
  const roleRows = $('#roleAiRows');
  if (roleRows) {
    const mapping = s.role_ais || {};
    roleRows.innerHTML = ROLE_AI_ROWS.map(([key, label]) => `
      <div class="ai-assign-row"><span>${esc(label)}</span>
        <select class="select sm" onchange="assignAI('role', '${key}', this.value)">
          ${aiOptions(mapping[key])}
        </select></div>`).join('');
  }
  const partRows = $('#partAiRows');
  if (partRows) {
    const mapping = s.part_ais || {};
    partRows.innerHTML = app.parts.map(p => `
      <div class="ai-assign-row"><span>${esc(p.label)}</span>
        <select class="select sm" onchange="assignAI('part', '${p.id}', this.value)">
          ${aiOptions(mapping[p.id])}
        </select></div>`).join('');
  }
}

async function loadAIs() {
  try {
    const r = await getJSON('/api/ais');
    if (r && r.ok) {
      app.ais = r.ais || [];
      app.parts = r.parts || [];
      (r.parts || []).forEach(p => { app.partMeta[p.id] = p; });
      renderAISection({ ais: r.ais, parts: r.parts, role_ais: r.role_ais,
                        part_ais: r.part_ais });
    }
  } catch (e) { /* 面板打不开也不影响主流程 */ }
}

function aiApplyPreset() {
  const id = ($('#aiProvider') || {}).value;
  const p = (app.settings && app.settings.presets ? app.settings.presets
             : (app.presets || [])).find(x => x.id === id);
  if (!p) return;
  $('#aiBaseUrl').value = p.base_url || '';
  $('#aiModel').value = p.model || '';
}

async function saveAI() {
  const payload = {
    label: ($('#aiLabel').value || '').trim(),
    provider: ($('#aiProvider').value || 'openai'),
    base_url: ($('#aiBaseUrl').value || '').trim(),
    model: ($('#aiModel').value || '').trim(),
    api_key: ($('#aiKey').value || '').trim(),
  };
  if (!payload.base_url || !payload.model) { toast('Base URL 和模型名都要填', true); return; }
  const r = await api('/api/ais', payload);
  if (!r.ok) { toast(r.message || '保存失败', true); return; }
  ['aiLabel', 'aiKey'].forEach(id => { const e = $('#' + id); if (e) e.value = ''; });
  app.settings = r.settings;
  renderAISection(r.settings);
  fillSettings(r.settings);
  toast('已保存 AI：' + (payload.label || payload.model));
}

async function probeAI(id) {
  toast('正在连这个 AI…');
  const r = await api('/api/ais/' + encodeURIComponent(id) + '/probe', {});
  if (r.ok) toast('连接成功 · ' + (r.probe.model || '') + ' · ' + r.probe.ms + 'ms');
  else toast('连接失败：' + (r.message || ''), true);
}

async function deleteAI(id) {
  if (!confirm('删除这个 AI 接入点？（用到它的角色/分组会回落到默认）')) return;
  const r = await api('/api/ais/' + encodeURIComponent(id) + '/delete', {});
  if (!r.ok) { toast(r.message || '删除失败', true); return; }
  app.settings = r.settings;
  renderAISection(r.settings);
  toast('已删除');
}

async function assignAI(scope, key, aiId) {
  const body = scope === 'role' ? { role_ais: { [key]: aiId || '' } }
                               : { part_ais: { [key]: aiId || '' } };
  const r = await api('/api/assign', body);
  if (!r.ok) { toast(r.message || '指派失败', true); return; }
  app.settings = r.settings;
  toast(aiId ? '已指派' : '已改回默认 AI');
}

async function clearKey() {
  const cur = app.settings || {};
  const fromEnv = cur.key_source === 'env';
  const ask = fromEnv
    ? '这个密钥来自环境变量 AUTFLOW_LLM_KEY。现在清除会把进程内的配置也停用，'
      + '但重启后环境变量还会生效——要永久清除请同时删掉那个环境变量。继续？'
    : '彻底清除已保存的 API Key？\n\n会从 data/settings.json 里删掉密钥字段、'
      + '并把执行模式切回模拟；之后不会再自己回来。';
  if (!confirm(ask)) return;
  const r = await api('/api/settings/clear-key', {});
  if (!r || !r.ok) { toast((r && r.message) || '清除失败', true); return; }
  app.settings = r.settings;
  const key = $('#setKey');
  if (key) key.value = '';
  fillSettings(r.settings);
  renderModeChip();
  syncDiscussUI();
  toast('已彻底清除密钥（磁盘与内存都不再保留），当前为模拟模式');
}

async function saveSettings() {
  const btn = $('#btnSaveSettings'), note = $('#modelNote');
  const payload = {
    provider: $('#setProvider').value,
    base_url: $('#setBaseUrl').value.trim(),
    model: $('#setModel').value.trim(),
    temperature: parseFloat($('#setTemp').value),
    max_steps: parseInt($('#setSteps').value, 10),
    max_tokens: parseInt($('#setMaxTokens').value, 10),
    timeout: parseInt($('#setTimeout').value, 10),
    mode: $('#tglUseLLM').checked ? 'llm' : 'mock'
  };
  const key = $('#setKey').value.trim();
  if (key) payload.api_key = key;
  if (payload.mode === 'llm' && !key && !(app.settings && app.settings.has_key)) {
    toast('启用模型需要 API Key', true);
    $('#setKey').focus();
    return;
  }
  btn.disabled = true; btn.textContent = '保存中…';
  try {
    const r = await api('/api/settings', payload);
    if (!r.ok) { note.textContent = r.message || '保存失败'; return; }
    app.settings = r.settings;
    fillSettings(r.settings);
    renderModeChip();
    note.textContent = r.settings.active
      ? '模型已就绪 · 可用于讨论与真实执行；模拟运行不会产生代码文件'
      : '已保存 · 模型尚未启用；工作流仍为模拟模式';
    toast(r.settings.active ? '模型已就绪' : '配置已保存');
  } catch (e) {
    note.textContent = '保存失败：' + (e && e.message ? e.message : e);
  } finally {
    btn.disabled = false; btn.textContent = '保存配置';
  }
}

async function probeModel() {
  const btn = $('#btnProbe'), box = $('#probeBox'), note = $('#modelNote');
  btn.disabled = true; btn.textContent = '测试中…';
  box.hidden = false; box.className = 'probe-box'; box.textContent = '正在请求模型…';
  try {
    const r = await api('/api/settings/probe', {}, 'POST');
    if (r.ok && r.probe) {
      const p = r.probe;
      box.className = 'probe-box ok';
      box.textContent = `连接成功 · ${p.provider} / ${p.model} · ${p.ms}ms\n`
        + `回复：${truncate(p.reply || '', 160)}`
        + (p.usage ? `\n用量：in ${p.usage.in || 0} / out ${p.usage.out || 0}` : '');
      note.textContent = '连接正常';
    } else {
      box.className = 'probe-box bad';
      box.textContent = (r.code ? `[${r.code}] ` : '') + (r.message || '连接失败');
      note.textContent = '连接失败';
    }
  } catch (e) {
    box.className = 'probe-box bad';
    box.textContent = '测试失败：' + (e && e.message ? e.message : e);
  } finally {
    btn.disabled = false; btn.textContent = '测试连接';
  }
}

function renderModeChip() {
  // 顶栏只有一个执行模式入口：它既是状态徽标，也是模型配置入口
  const chip = $('#btnModel');
  if (!chip) return;
  const s = app.settings || {};
  const live = app.run && app.run.mode === 'llm'
    && ['RUNNING', 'PAUSED', 'WAITING_HUMAN', 'RECONCILING'].indexOf(app.run.status) >= 0;
  // 标签里始终保留「模型」二字：它既是状态，也是模型配置入口
  if (live) { chip.textContent = '⚙ 模型 · 运行中'; chip.className = 'mode-chip btn sm on'; }
  else if (s.active) { chip.textContent = '⚙ 模型 · 已配置'; chip.className = 'mode-chip btn sm on'; }
  else if (s.has_key) { chip.textContent = '⚙ 模型 · 模拟（已配置未启用）'; chip.className = 'mode-chip btn sm'; }
  else { chip.textContent = '⚙ 模型 · 模拟'; chip.className = 'mode-chip btn sm'; }
  chip.title = s.active
    ? ('模型已配置：' + (s.provider || '') + ' / ' + (s.model || '')
       + ' — 可用于 AI 讨论与真实执行；模拟运行不落盘代码；点此修改配置')
    : '工作流默认模拟运行；配置模型后可启用 AI 讨论与真实执行';
  syncDiscussUI();
}

/* ================================================================ 规格文档导入 */
function toggleSpecPaste() {
  const t = $('#specPaste'), row = $('#pasteActions');
  const show = t.hidden;
  t.hidden = !show;
  if (row) row.hidden = !show;
  if (show) t.focus();
}

async function importSpecFile(input) {
  const file = input.files && input.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    await parseSpecToForm(text, file.name);
  } catch (e) {
    toast('读取文件失败：' + (e && e.message ? e.message : e), true);
  } finally {
    input.value = '';                    // 允许重复导入同一个文件
  }
}

async function parseSpecText() {
  const text = $('#specPaste').value;
  if (!text.trim()) { toast('先把规格正文粘贴进来', true); return; }
  await parseSpecToForm(text, '');
}

async function parseSpecToForm(text, name) {
  const note = $('#importNote'), specNote = $('#specNote');
  note.textContent = '解析中…';
  try {
    const r = await api('/api/specs/import', { text, name: name || '' });
    if (!r.ok) { note.textContent = r.message || '解析失败'; return; }
    const s = r.spec;
    if (app.preview) cancelPreview({ silent: true });
    app.specId = null;
    $('#specName').value = s.name || '';
    $('#specSummary').value = s.summary || '';
    $('#specBody').value = s.body || '';
    $('#specConstraints').value = (s.constraints || []).join('\n');
  syncConstraintCount();
    app.specModules = (s.modules || []).map((m, i) => ({
      id: '', name: m.name || '', goal: m.goal || '',
      priority: m.priority || 'P1', acceptance: m.acceptance || []
    }));
    renderSpecModules();
    const st = s.stats || {};
    note.textContent = `已读入 ${st.modules} 个模块 · ${st.acceptance} 条验收 · ${st.constraints} 条约束（${st.chars} 字）`;
    $('#specDecomp').hidden = false;
    $('#specStats').textContent = '导入解析结果';
    $('#specTrace').innerHTML = (s.trace || [])
      .map(t => `<div class="trace-line">· ${esc(t)}</div>`).join('');
    $('#specTasks').innerHTML = '<tbody><tr><td colspan="9" class="muted">'
      + '内容已填进表单；点「分解预览」把这份规格拆成任务</td></tr></tbody>';
    $('#specMeta').textContent = '导入待保存';
    if (specNote) {
      specNote.textContent = '文档已解析成表单，确认无误后点「保存规格」';
      if (!app.specId) syncSpecButtonsFromForm();
    }
    toast('已导入：' + st.modules + ' 个模块');
    if ($('#specPaste').hidden === false) $('#specPaste').hidden = true;
    if ($('#pasteActions')) $('#pasteActions').hidden = true;
  } catch (e) {
    note.textContent = '解析失败：' + (e && e.message ? e.message : e);
  }
}

/* 导入后还没保存，规格按钮要能直接用 */
function syncSpecButtonsFromForm() {
  const d = $('#btnSpecDecompose');
  if (d) { d.disabled = false; d.title = '保存规格后才能预览（会先保存当前表单）'; }
}

function bindSpecDrop() {
  ['dragover', 'drop'].forEach(ev => document.addEventListener(ev, (e) => {
    if (ev === 'dragover') { e.preventDefault(); return; }
    e.preventDefault();
    const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (!f) return;
    if (!/\.(md|markdown|txt|json)$/i.test(f.name)) {
      toast('只支持 .md / .txt / .json', true); return;
    }
    openSpec();
    f.text().then(t => parseSpecToForm(t, f.name)).catch(() => toast('读取失败', true));
  }));
}

/* ---------------------------------------------------------- 全局约束输入框 */
function syncConstraintCount() {
  const ta = $('#specConstraints');
  const label = $('#specConstraintCount');
  const lines = (ta.value || '').split('\n').map(x => x.trim()).filter(Boolean);
  if (label) label.textContent = lines.length ? lines.length + ' 条' : '0 条';
  // 随内容长高（最多 320px），避免长约束挤在一条窄缝里
  ta.style.height = 'auto';
  ta.style.height = Math.min(320, Math.max(120, ta.scrollHeight)) + 'px';
}

/* ================================================================ 多智能体讨论 */
app.discuss = app.discuss || { sid: null, timer: null, session: null, busy: false, shown: 0 };
app.nodeEdit = app.nodeEdit || null;
app.apComment = '';

const DISCUSS_ROLE_CLASS = {
  pm: 'r-pm', architect: 'r-arch', coder: 'r-coder',
  tester: 'r-qa', reviewer: 'r-rev'
};

function discussConfigured() {
  return !!(app.settings && app.settings.active);
}

function syncDiscussUI() {
  const btn = $('#btnDiscuss'), hint = $('#discussHint');
  if (!btn) return;
  const ok = discussConfigured();
  const busy = !!app.discuss.busy;
  btn.disabled = !ok || busy;
  btn.textContent = busy ? '讨论中…' : '让 AI 讨论并出任务清单';
  if (hint) {
    hint.textContent = !ok
      ? '当前是模拟模式：点顶栏「模拟」配置 Base URL 与 Key 之后，才能让几个角色围绕这份规格讨论'
      : (busy ? (app.discuss.session && app.discuss.session.stage) || '讨论中…'
              : '产品经理 / 架构师 / 开发者 / 测试 / 评审依次发言，两轮后汇总成任务清单');
  }
  const stop = $('#btnDiscussStop');
  if (stop) stop.hidden = !busy;
  const badge = $('#discussBadge');
  if (badge) {
    const s = app.discuss.session;
    if (busy && s) { badge.hidden = false; badge.textContent = s.stage || '进行中'; badge.className = 'discuss-badge on'; }
    else if (s && s.status === 'done') { badge.hidden = false; badge.textContent = '已完成 · ' + (s.tasks || []).length + ' 条任务'; badge.className = 'discuss-badge ok'; }
    else if (s && s.status === 'error') { badge.hidden = false; badge.textContent = '讨论失败'; badge.className = 'discuss-badge bad'; }
    else if (s && s.status === 'cancelled') { badge.hidden = false; badge.textContent = '已取消'; badge.className = 'discuss-badge'; }
    else badge.hidden = true;
  }
}

function resetDiscussLog() {
  const log = $('#discussLog');
  if (log) { log.innerHTML = ''; log.hidden = true; }
  app.discuss.shown = 0;
}

async function startDiscuss() {
  if (app.discuss.busy) return;
  if (!discussConfigured()) {
    toast('先配置真实模型：填 Base URL 与 API Key', true);
    openModel();
    return;
  }
  if (app.run.status && ['RUNNING', 'PAUSED', 'WAITING_HUMAN', 'RECONCILING'].indexOf(app.run.status) >= 0) {
    toast('运行进行中，稍后再开讨论会', true); return;
  }
  // 讨论的必须是"屏幕上这份规格"：先把表单存了，避免讨论的是旧版本
  await saveSpec();
  if (!app.specId) return;
  const template = ($('#selSpecTemplate') || {}).value || app.specTemplate || 'default';
  resetDiscussLog();
  const log = $('#discussLog');
  try {
    const r = await api('/api/specs/' + encodeURIComponent(app.specId) + '/discuss', { template });
    if (!r.ok) { if (log) { log.hidden = false; log.innerHTML = '<div class="dis-err">' + esc(r.message || '讨论没能开始') + '</div>'; } toast(r.message || '讨论没能开始', true); return; }
    app.discuss.sid = r.session_id;
    app.discuss.session = r.session;
    app.discuss.busy = true;
    discussSayRowVisible();
    if (log) log.hidden = false;
    syncDiscussUI();
    pollDiscuss();
  } catch (e) {
    toast('讨论启动失败：' + (e && e.message ? e.message : e), true);
  }
}

function pollDiscuss() {
  clearInterval(app.discuss.timer);
  app.discuss.timer = setInterval(refreshDiscuss, 800);
  refreshDiscuss();
}

async function refreshDiscuss() {
  const sid = app.discuss.sid;
  if (!sid) return;
  let data;
  try {
    const res = await fetch('/api/plan/' + encodeURIComponent(sid));
    data = await res.json();
  } catch (e) {
    return;                     // 网络抖动：下一次轮询接着来
  }
  if (!data || !data.session) return;
  app.discuss.session = data.session;
  renderDiscuss(data.session);
  syncDiscussUI();
  if (data.session.status !== 'running') {
    clearInterval(app.discuss.timer);
    app.discuss.timer = null;
    app.discuss.busy = false;
    syncDiscussUI();
  }
}

function renderDiscuss(s) {
  const log = $('#discussLog');
  if (!log) return;
  log.hidden = false;
  const minutes = s.minutes || [];
  for (let i = app.discuss.shown; i < minutes.length; i++) {
    const m = minutes[i];
    const div = document.createElement('div');
    const cls = m.kind === 'human' ? 'r-human' : (DISCUSS_ROLE_CLASS[m.role] || '');
    div.className = 'dis-line ' + cls;
    div.innerHTML = `<div class="dis-head">
        <span class="dis-who">${m.kind === 'human' ? '🧑 人工' : esc(m.label || m.role)}</span>
        <span class="dis-round">第 ${m.round || 1} 轮</span>
        ${m.why ? `<span class="dis-why">${esc(m.why)}</span>` : ''}
        <span class="dis-at">${esc(m.at || '')}</span>
      </div>
      <div class="dis-text">${esc(m.content || '')}</div>`
      + ((m.points || []).length
        ? '<ul class="dis-points">' + m.points.map(p => `<li>${esc(p)}</li>`).join('') + '</ul>'
        : '');
    log.appendChild(div);
    app.discuss.shown = i + 1;
    log.scrollTop = log.scrollHeight;
  }

  const tail = log.querySelector('.dis-tail');
  if (tail) tail.remove();

  const box = document.createElement('div');
  box.className = 'dis-tail';
  if (s.status === 'running') {
    box.innerHTML = `<div class="dis-wait">… ${esc(s.stage || '讨论中')}</div>`;
  } else if (s.status === 'error') {
    const err = s.error || {};
    box.innerHTML = `<div class="dis-err">${esc((err.code || 'E-AGENT') + '：' + (err.message || '讨论失败'))}
      <div class="dis-hint">检查一下模型地址、密钥和额度；也可以直接用「分解预览」走离线规则分解。</div></div>`;
  } else if (s.status === 'cancelled') {
    box.innerHTML = '<div class="dis-hint">讨论已取消，没有生成任务清单。</div>';
  } else if (s.status === 'done') {
    const fixes = s.fixes || [];
    box.innerHTML = `${consensusHTML(s)}<div class="dis-done">
        <b>讨论完成</b>：${(s.minutes || []).length} 条发言 → ${(s.tasks || []).length} 条任务
        ${s.usage && s.usage.prompt_tokens ? `<span class="dis-usage">token ${s.usage.prompt_tokens}↑ ${s.usage.completion_tokens}↓</span>` : ''}
      </div>
      ${(s.trace || []).map(t => `<div class="trace-line">· ${esc(t)}</div>`).join('')}
      ${fixes.length ? `<details class="dis-fixes"><summary>代码侧修正 ${fixes.length} 处</summary>`
        + fixes.map(f => `<div class="trace-line">· ${esc(f)}</div>`).join('') + '</details>' : ''}
      <div class="dis-actions">
        <button class="btn primary dis-apply" onclick="applyDiscussion()">就按这个编排生成工作流</button>
        ${(['blocked', 'partial'].indexOf((s.consensus || {}).status) >= 0 && !s.human_decision)
          ? `<button class="btn warn" onclick="decideDiscussion('accept')">接受当前清单</button>
             <button class="btn ghost" onclick="decideDiscussion('resume')">继续讨论分歧</button>` : ''}
        <span class="panel-note">生成后可继续在画布上手动调整</span>
      </div>`;
  }
  log.appendChild(box);
  log.scrollTop = log.scrollHeight;
}

/* 共识面板：谁同意、谁要改、谁有阻塞，以及人工介入了几次 */
function consensusHTML(s) {
  const c = s.consensus || {};
  const votes = (s.votes && s.votes.length) ? (s.votes[s.votes.length - 1].votes || []) : [];
  if (!votes.length && !c.status) return '';
  const chip = (label, stance) => `<span class="stance stance-${stance}">${esc(label)} · ${
    { agree: '同意', revise: '要改', block: '有阻塞' }[stance] || stance}</span>`;
  const humans = (s.humans || []).length;
  return `<div class="consensus-box">
    <div class="cons-head"><b>共识：${
      { reached: '已达成', adopted: '人工拍板采纳', partial: '部分同意', blocked: '有阻塞',
        none: '未表决' }[c.status] || c.status || '进行中'}</b>
      ${humans ? `<span class="panel-note">人工介入 ${humans} 次</span>` : ''}</div>
    <div class="stance-row">${votes.map(v => chip(v.label, v.stance)).join('')}</div>
    ${(c.unresolved || []).length
      ? `<div class="unresolved"><b>未解决</b>${c.unresolved.map(x =>
          `<div class="trace-line">· ${esc(x)}</div>`).join('')}</div>` : ''}
  </div>`;
}

function discussSayRowVisible() {
  const row = $('#discussSayRow');
  if (row) row.hidden = !app.discuss.sid;
}

async function sendDiscussSay() {
  const inp = $('#discussSay');
  const text = ((inp && inp.value) || '').trim();
  if (!text) { toast('先写一句要引导的话', true); return; }
  if (!app.discuss.sid) { toast('先开一场讨论', true); return; }
  const btn = $('#btnDiscussSay');
  if (btn) btn.disabled = true;
  try {
    const r = await api('/api/plan/' + encodeURIComponent(app.discuss.sid) + '/say',
                        { text: text });
    if (r.ok) {
      if (inp) inp.value = '';
      app.discuss.busy = true;
      pollDiscuss();
      toast('已插话：对口角色会在下一位回应');
    } else toast(r.message || '插话失败', true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function decideDiscussion(action) {
  if (!app.discuss.sid) return;
  const r = await api('/api/plan/' + encodeURIComponent(app.discuss.sid) + '/decide',
                      { action: action });
  if (!r.ok) { toast(r.message || '操作失败', true); return; }
  toast(action === 'accept' ? '已采纳当前清单，可以生成工作流' : '已让相关角色继续讨论');
  refreshDiscuss();
}

function discussApplyButton() {
  return document.querySelector('.dis-apply');
}

async function applyDiscussion() {
  if (!app.discuss.sid) return;
  const btn = discussApplyButton();
  if (btn) { btn.disabled = true; btn.textContent = '生成中…'; }
  const template = ($('#selSpecTemplate') || {}).value || app.specTemplate || 'default';
  try {
    const r = await api('/api/plan/' + encodeURIComponent(app.discuss.sid), { template });
    if (!r.ok) {
      toast(r.message || '生成失败', true);
      if (btn) { btn.disabled = false; btn.textContent = '就按这个编排生成工作流'; }
      return;
    }
    if (app.preview) {
      app.preview = null;
      app.mode = app.previewMode || 'view';
      app.defKey = '';
      clearTasks();
    }
    app.tasks = r.tasks || [];
    app.specTemplate = template;
    await loadEditor();
    closeSpec();
    await refresh();
    syncSpecTemplate();
    const d = r.discussion || {};
    setFoot(`已按 AI 讨论结果编排 ${(r.tasks || []).length} 条任务 · ${d.minutes || 0} 条发言 · `
      + `${r.stats.nodes} 节点 / ${r.stats.edges} 边`);
    toast('已按讨论结果生成工作流草稿');
  } catch (e) {
    toast('生成失败：' + (e && e.message ? e.message : e), true);
    if (btn) { btn.disabled = false; btn.textContent = '就按这个编排生成工作流'; }
  }
}

async function stopDiscuss() {
  if (!app.discuss.sid) return;
  try {
    await api('/api/plan/' + encodeURIComponent(app.discuss.sid) + '/cancel', {});
    toast('已请求停止讨论');
  } catch (e) { /* 忽略：下次轮询会拿到最终状态 */ }
}

/* ================================================================ 运行产物 */
function currentRunId() {
  return (app.run && app.run.run_id) || app.runId || null;
}

function renderFileBadge() {
  const badge = $('#fileCount');
  if (!badge) return;
  const n = ((app.run && app.run.artifacts) || []).length;
  badge.textContent = n;
}

async function loadRunFiles() {
  const list = $('#fileList'), hint = $('#filesHint');
  const rid = currentRunId();
  if (!rid) { list.innerHTML = '<div class="empty">还没有运行</div>'; return; }
  list.innerHTML = '<div class="empty">加载中…</div>';
  try {
    const r = await getJSON('/api/runs/' + encodeURIComponent(rid) + '/files');
    const files = r.files || [];
    const art = {};
    (r.artifacts || []).forEach(a => art[a.path] = a);
    if (!files.length) {
      list.innerHTML = '<div class="empty">这次运行还没有落盘文件'
        + (app.run.mode === 'llm' ? '' : '（当前是模拟模式，配置模型后才有真实产物）') + '</div>';
      hint.textContent = '工作区：' + (r.workspace || '未创建');
      return;
    }
    hint.textContent = `${files.length} 个文件 · 工作区 ${r.workspace || '—'}`;
    list.innerHTML = files.map(f => `
      <div class="file-row" onclick="openRunFile('${esc(f.path)}')" title="${esc(f.path)}">
        <span class="fr-path">${esc(f.path)}</span>
        ${art[f.path] ? '<span class="fr-badge">产物</span>' : ''}
        <span class="fr-size">${fmtBytes(f.bytes)}</span>
      </div>`).join('');
  } catch (e) {
    list.innerHTML = '<div class="empty">读取失败：' + esc(e && e.message ? e.message : e) + '</div>';
  }
}

function fmtBytes(n) {
  n = n || 0;
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1024 / 1024).toFixed(1) + ' MB';
}

async function openRunFile(path) {
  const rid = currentRunId();
  if (!rid) return;
  const view = $('#fileView');
  try {
    const r = await getJSON('/api/runs/' + encodeURIComponent(rid)
      + '/file?path=' + encodeURIComponent(path));
    if (!r.ok && r.error) { toast(r.error, true); return; }
    $('#fileViewPath').textContent = path + (r.truncated ? '（已截断）' : '');
    $('#fileViewBody').textContent = r.content || '';
    view.hidden = false;
  } catch (e) {
    toast('打开文件失败：' + (e && e.message ? e.message : e), true);
  }
}

function closeFileView() { $('#fileView').hidden = true; }

(async function boot() {
  bindUI();
  bindCanvas();
  bindSpecDrop();
  await loadEditor();
  await loadTemplates();
  await refresh();
  if (!app.settings) loadSettings();
  loadAIs();
  connect();
  setInterval(() => {
    if (app.run && !app.frozen) { $('#runClock').textContent = fmtClock(app.run.elapsed); }
  }, 500);
})();
