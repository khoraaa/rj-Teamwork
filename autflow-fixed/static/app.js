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

const NW = 200, NH = 62, GAPX = 56, GAPY = 52, PAD = 44;

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
  preview: null, previewMode: 'view', specNewFrom: null
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
function computeLayout(wf) {
  const nodes = wf.nodes, edges = wf.edges;
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
}

function selectNode(id) {
  app.selected = id;
  app.selEdge = null;
  showDetailTab();
  renderNodes();
  renderEdges();
  renderDetail(true);
}
function selectEdge(key) {
  app.selEdge = key;
  app.selected = null;
  showDetailTab();
  renderNodes();
  renderEdges();
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

/* ---------------- 节点产物可视化：架构/接口设计 → 节点图，评审 → 评分卡 ---------------- */
function _entityLabel(e, i) {
  if (e && typeof e === 'object') return { id: e.id || ('ENT' + i), label: e.name || e.id || ('实体' + i), kind: e.kind || 'model' };
  return { id: 'ENT' + i, label: String(e == null ? '实体' + i : e), kind: 'model' };
}
function _endpointLabel(e, i) {
  if (e && typeof e === 'object') return { id: e.id || ('EP' + i), method: e.method || '', path: e.path || '', entity: e.entity || e.target || null };
  const s = String(e || '').trim();
  const m = s.match(/^(\w+)\s+(.+)$/);
  return { id: 'EP' + i, method: m ? m[1] : '', path: m ? m[2] : s, entity: null };
}

function designGraphHTML(o) {
  const ents = (o.entities || []).map(_entityLabel);
  const eps = (o.endpoints || []).map(_endpointLabel);
  const entIdx = {}; ents.forEach((e, i) => { entIdx[e.id] = i; });
  const epIdx = {}; eps.forEach((e, i) => { epIdx[e.id] = i; });
  const links = []; const seen = {};
  (o.relations || []).forEach(r => {
    if (!r || !r.from || !r.to) return;
    const key = r.from + '>' + r.to;
    if (seen[key]) return; seen[key] = 1;
    if (entIdx[r.from] != null && epIdx[r.to] != null) links.push([entIdx[r.from], epIdx[r.to]]);
    else if (epIdx[r.from] != null && entIdx[r.to] != null) links.push([entIdx[r.to], epIdx[r.from]]);
  });
  eps.forEach((e, i) => {
    if (e.entity != null && entIdx[e.entity] != null) {
      const key = 'ent:' + e.entity + '>ep:' + e.id;
      if (!seen[key]) { seen[key] = 1; links.push([entIdx[e.entity], i]); }
    }
  });

  const nodeH = 28, gapY = 8, padX = 8, padY = 22, colW = 126, gutter = 26;
  const rows = Math.max(ents.length, eps.length, 1);
  const W = padX * 2 + colW * 2 + gutter;
  const H = padY + rows * nodeH + (rows - 1) * gapY + 8;
  const lx = padX, rx = padX + colW + gutter;
  const yOf = i => padY + i * (nodeH + gapY);

  let g = '<svg class="dgraph" viewBox="0 0 ' + W + ' ' + H + '" width="100%" role="img" aria-label="架构与接口节点图">';
  g += '<text x="' + lx + '" y="13" class="dg-col">数据模型 / 服务</text>';
  g += '<text x="' + rx + '" y="13" class="dg-col">接口</text>';
  links.forEach(l => {
    const y1 = yOf(l[0]) + nodeH / 2, y2 = yOf(l[1]) + nodeH / 2;
    g += '<line x1="' + (lx + colW) + '" y1="' + y1 + '" x2="' + rx + '" y2="' + y2 + '" class="dg-line"/>';
  });
  ents.forEach((e, i) => {
    const y = yOf(i);
    g += '<rect x="' + lx + '" y="' + y + '" width="' + colW + '" height="' + nodeH + '" rx="7" class="dg-ent"/>'
      + '<text x="' + (lx + colW / 2) + '" y="' + (y + nodeH / 2 + 4) + '" text-anchor="middle" class="dg-t">' + esc(e.label) + '</text>';
  });
  eps.forEach((e, i) => {
    const y = yOf(i);
    const m = e.method ? '<tspan class="dg-m">' + esc(e.method) + '</tspan> ' : '';
    g += '<rect x="' + rx + '" y="' + y + '" width="' + colW + '" height="' + nodeH + '" rx="7" class="dg-ep"/>'
      + '<text x="' + (rx + 10) + '" y="' + (y + nodeH / 2 + 4) + '" class="dg-t">' + m + esc(e.path) + '</text>';
  });
  g += '</svg>';

  let meta = '';
  if (o.tables != null || o.planned_cases != null) {
    meta = '<div class="oviz-meta">数据表 ' + (o.tables != null ? o.tables : '—')
      + ' · 计划用例 ' + (o.planned_cases != null ? o.planned_cases : '—') + '</div>';
  }
  return '<div class="oviz"><div class="oviz-title">' + esc(o.artifact || 'design-v1')
    + ' · 架构 / 接口节点图</div>' + g + meta + '</div>';
}

function reviewGaugeHTML(n, o) {
  const score = (o && o.qa_score != null) ? o.qa_score : null;
  const blocking = (o && Array.isArray(o.blocking)) ? o.blocking : [];
  const passed = (o && o.result) !== false;
  let ring = '';
  if (score != null) {
    const pct = Math.max(0, Math.min(100, Number(score) || 0));
    const color = pct >= 80 ? 'var(--green)' : 'var(--red)';
    const r = 26, c = 2 * Math.PI * r, dash = c * pct / 100;
    ring = '<svg class="gauge" width="72" height="72" viewBox="0 0 72 72" aria-hidden="true">'
      + '<circle cx="36" cy="36" r="' + r + '" class="gauge-bg"/>'
      + '<circle cx="36" cy="36" r="' + r + '" class="gauge-fg" transform="rotate(-90 36 36)" style="stroke:' + color + ';stroke-dasharray:' + dash + ' ' + c + '"/>'
      + '<text x="36" y="42" text-anchor="middle" class="gauge-num">' + Math.round(pct) + '</text></svg>';
  }
  const blocks = blocking.map(b => '<li>' + esc(b) + '</li>').join('');
  const expr = n.expression ? '<div class="oviz-sub">表达式：' + esc(n.expression) + '</div>' : '';
  const blk = blocking.length ? '<div class="oviz-sub">阻断项：</div><ul class="oviz-blocks">' + blocks + '</ul>' : '';
  return '<div class="oviz"><div class="oviz-title">' + esc(n.type === 'condition' ? '质量门判定' : '评审门')
    + ' · 输出可视化</div><div class="oviz-review ' + (passed ? 'ok' : 'no') + '">' + ring
    + '<div class="oviz-review-body"><b>' + (passed ? '通过' : '未通过') + '</b>' + expr + blk + '</div></div></div>';
}

function outputVisual(n) {
  const o = n && n.output;
  if (!o || typeof o !== 'object') return '';
  const art = o.artifact || n.out_schema;
  if (art === 'design-v1' && Array.isArray(o.entities) && Array.isArray(o.endpoints)) return designGraphHTML(o);
  if (n.type === 'condition' || o.qa_score != null) return reviewGaugeHTML(n, o);
  return '';
}

function renderDetail(force) {
  const pane = $('#paneDetail');
  if (app.selEdge) return renderEdgeDetail(pane);
  // 编辑态以定义为准（可能是运行快照里还没有的新节点）
  const dnode = (app.mode === 'edit' && app.def && app.selected)
    ? app.def.nodes.find(x => x.id === app.selected) : null;
  const n = dnode || (app.selected ? app.nodeById[app.selected] : null);
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
    actions = `<input id="apComment" class="ap-input" placeholder="审批意见（可选，会记入运行历史）">
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
      <div class="dh-name">${esc(n.name)}</div>
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
    <div style="font-size:12px;color:#475569">${esc(n.message || '—')}</div>
    ${taskBlock(n)}
    ${ap}
    ${n.expression ? `<div class="section-title">判定表达式</div>
      <div class="mono" style="background:#f8fafc;border:1px solid var(--line);border-radius:8px;padding:7px 10px">${esc(n.expression)}${
        n.branch_taken ? `　→ 命中 <b style="color:${n.branch_taken === 'true' ? '#16a34a' : '#b45309'}">${n.branch_taken.toUpperCase()}</b>` : ''}</div>` : ''}
    <div class="section-title">输出摘要</div>
    ${outputVisual(n)}
    ${n.output ? `<pre class="output">${jsonHTML(n.output)}</pre>` : '<div class="empty" style="padding:14px">尚未产生产物</div>'}
    ${attTable}
    <div class="section-title">执行日志</div>
    <ul class="logs">${(n.logs || []).map(l => `<li>${esc(l)}</li>`).join('') || '<li>暂无日志</li>'}</ul>
    ${actions ? `<div class="actions">${actions}</div>` : ''}
  `;
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

function taskBlock(n) {
  const list = taskRows();
  if (!list.length) return '';
  const t = n.task_id ? list.find(x => x.id === n.task_id) : null;
  const info = t ? `
    <div class="section-title">任务信息</div>
    <div class="kv">
      <div><span>任务</span><b>${esc(t.id)} · ${esc(t.module || '—')}</b></div>
      <div><span>规格出处</span><b>${taskRefHTML(t)}</b></div>
      <div><span>优先级</span><b>${esc(t.priority || '—')}</b></div>
      <div><span>预估</span><b>${t.estimate} 人日</b></div>
    </div>
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
  if (s.workflow) { app.wf = s.workflow; app.runWorkflow = s.workflow; }

  app.run = s.run || {};
  app.runId = app.run.run_id || app.runId;
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
  const comment = box ? String(box.value || '').trim() : '';
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
    $('#paneDetail').hidden = t.dataset.tab !== 'detail';
    $('#paneTimeline').hidden = t.dataset.tab !== 'timeline';
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

function renderTasks(r) {
  const tasks = r.tasks || [], stats = r.stats || {};
  $('#specDecomp').hidden = false;
  $('#specStats').textContent = `${stats.modules} 模块 · ${stats.tasks} 条任务 · ` +
    `${stats.estimate} 人日 · 最长 ${stats.depth} 层`;
  $('#specTrace').innerHTML = (r.trace || [])
    .map(t => `<div class="trace-line">· ${esc(t)}</div>`).join('');
  const rows = tasks.map(t => `
    <tr>
      <td class="tt-state">待编排</td>
      <td class="tt-id">${esc(t.id)}</td>
      <td class="tt-title">${esc(t.title)}</td>
      <td class="tt-role">${esc(t.role)}</td>
      <td class="tt-deps">${esc((t.depends_on || []).join(',') || '—')}</td>
      <td class="tt-est">${t.estimate}</td>
      <td class="tt-acc">${taskAcceptanceHTML(t)}</td>
      <td class="tt-source">${taskRefHTML(t)}</td>
    </tr>`).join('');
  $('#specTasks').innerHTML =
    '<thead><tr><th>状态</th><th>ID</th><th>任务</th><th>角色</th><th>依赖</th>'
    + '<th>人日</th><th>验收标准</th><th>规格出处</th></tr></thead>' +
    `<tbody>${rows}</tbody>`;
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

(async function boot() {
  bindUI();
  bindCanvas();
  await loadEditor();
  await loadTemplates();
  await refresh();
  connect();
  setInterval(() => {
    if (app.run && !app.frozen) { $('#runClock').textContent = fmtClock(app.run.elapsed); }
  }, 500);
})();
