/* 前端逻辑冒烟：用最小 DOM 桩在 Node 里加载 app.js，模拟编辑操作。
   目的不是验证像素，而是验证交互路径不抛异常、状态与接口调用正确。 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const ROOT = path.join(__dirname, '..');
const BASE = process.argv[2] || 'http://127.0.0.1:8010';
const html = fs.readFileSync(path.join(ROOT, 'static', 'index.html'), 'utf8');
const js = fs.readFileSync(path.join(ROOT, 'static', 'app.js'), 'utf8');

const OK = [];
function check(cond, msg) {
  OK.push(!!cond);
  console.log((cond ? '  ✓ ' : '  ✗ ') + msg);
}

/* ------------------------------- DOM 桩 ------------------------------- */
class ClassList {
  constructor(el) { this.el = el; this.set = new Set(); }
  add(...c) { c.forEach((x) => x && this.set.add(x)); }
  remove(...c) { c.forEach((x) => this.set.delete(x)); }
  toggle(c, force) {
    const want = force === undefined ? !this.set.has(c) : !!force;
    want ? this.set.add(c) : this.set.delete(c);
    return want;
  }
  contains(c) { return this.set.has(c); }
}

function makeEl(tag) {
  const el = {
    tagName: String(tag || 'div').toUpperCase(),
    children: [], attrs: {}, dataset: {}, style: {},
    _text: '', _html: '', hidden: false, disabled: false,
    value: '', checked: false, parentNode: null, _listeners: {},
    classList: null
  };
  el.classList = new ClassList(el);
  el.appendChild = (c) => { c.parentNode = el; el.children.push(c); return c; };
  el.insertBefore = (c) => { c.parentNode = el; el.children.unshift(c); return c; };
  el.removeChild = (c) => { el.children = el.children.filter((x) => x !== c); };
  el.remove = () => { if (el.parentNode) el.parentNode.removeChild(el); };
  el.setAttribute = (k, v) => {
    el.attrs[k] = String(v);
    if (k === 'class') { el.classList.set = new Set(String(v).split(/\s+/).filter(Boolean)); }
  };
  el.getAttribute = (k) => (k in el.attrs ? el.attrs[k] : null);
  el.hasAttribute = (k) => k in el.attrs;
  el.addEventListener = (t, f) => { (el._listeners[t] = el._listeners[t] || []).push(f); };
  el.removeEventListener = () => {};
  el.dispatchEvent = (ev) => { (el._listeners[ev.type] || []).forEach((f) => f(ev)); return true; };
  el.closest = (sel) => {
    let cur = el;
    const want = sel.replace(/^\./, '');
    while (cur) {
      if (sel[0] === '.' && cur.classList.contains(want)) return cur;
      cur = cur.parentNode;
    }
    return null;
  };
  el.contains = (x) => {
    let cur = x;
    while (cur) { if (cur === el) return true; cur = cur.parentNode; }
    return false;
  };
  el.getBoundingClientRect = () => ({ left: 0, top: 0, width: 1000, height: 800, right: 1000, bottom: 800 });
  const stubCache = {};
  el.querySelector = (sel) => {
    if (!stubCache[sel]) stubCache[sel] = makeEl('g');
    return stubCache[sel];
  };
  el.querySelectorAll = () => [];
  el.setPointerCapture = () => {};
  el.releasePointerCapture = () => {};
  el.focus = () => { globalThis.document.activeElement = el; };
  el.blur = () => { globalThis.document.activeElement = null; };
  Object.defineProperty(el, 'textContent', {
    get: () => el._text, set: (v) => { el._text = String(v); }
  });
  Object.defineProperty(el, 'innerHTML', {
    get: () => el._html,
    set: (v) => { el._html = String(v); if (v === '') el.children = []; }
  });
  Object.defineProperty(el, 'className', {
    get: () => el.attrs.class || '', set: (v) => el.setAttribute('class', v)
  });
  Object.defineProperty(el, 'firstChild', { get: () => el.children[0] || null });
  Object.defineProperty(el, 'lastChild', { get: () => el.children[el.children.length - 1] || null });
  el.setPointerCapture = () => {};
  return el;
}

const byId = {};
for (const m of html.matchAll(/id="([^"]+)"/g)) byId[m[1]] = makeEl('div');
const svgEl = byId['canvas'];
const stage = makeEl('section');

globalThis.document = {
  documentElement: makeEl('html'),
  body: makeEl('body'),
  activeElement: null,
  createElement: (t) => makeEl(t),
  createElementNS: (ns, t) => makeEl(t),
  getElementById: (id) => byId[id] || makeEl('div'),
  querySelector: (sel) => {
    if (sel.startsWith('#')) return byId[sel.slice(1)] || makeEl('div');
    return makeEl('div');
  },
  querySelectorAll: () => [],
  addEventListener: () => {},
  elementFromPoint: () => null
};
globalThis.window = { addEventListener: () => {}, innerWidth: 1280, innerHeight: 900 };
globalThis.confirm = () => true;
globalThis.EventSource = class {
  constructor(url) {
    this.url = url; this.handlers = {}; this.onopen = null; this.onerror = null; this.onmessage = null;
    globalThis.__es = this;
  }
  addEventListener(t, f) { (this.handlers[t] = this.handlers[t] || []).push(f); }
  emit(t, data) {
    const ev = { data: JSON.stringify(data) };
    (this.handlers[t] || []).forEach((f) => f(ev));
  }
  close() {}
};
const realFetch = globalThis.fetch;
globalThis.fetch = (url, opts) => realFetch(url.startsWith('http') ? url : BASE + url, opts);
svgEl.getBoundingClientRect = () => ({ left: 0, top: 0, width: 1000, height: 800, right: 1000, bottom: 800 });
stage.getBoundingClientRect = svgEl.getBoundingClientRect;

/* ------------------------------ 加载 app ------------------------------ */
const src = js + '\nglobalThis.__app = app; globalThis.__fn = { setMode, addNode, connect: connectNodes, editNode, deleteSelection, doSave, doPublish, switchVersion, doStart, doReset, selectNode, selectEdge, render, onPointerMove, onPointerUp, repaintEdgesFor, canvasEdges, doGenerate, openReq, closeReq, openHist, closeHist, viewHistory, exitHistory, refresh, renderWfBar, showNetError, clearNetError, decomposeSpec, cancelPreview, startPreview, newSpec, backToSavedSpec, isEditable, syncSpecButtons, openSpec, loadSpecs };\n';
const context = vm.createContext(globalThis);
vm.runInContext(src, context, { filename: 'app.js' });

const app = globalThis.__app;
const fn = globalThis.__fn;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  // 等 boot 完成（loadEditor + refresh）
  for (let i = 0; i < 50 && !app.editor; i++) await sleep(100);
  check(!!app.editor, '编辑器数据加载成功');
  check(app.def && app.def.nodes.length === 10, '画布加载草稿定义（10 节点）');
  check(app.built === true, '画布已构建');

  await fn.doReset();                 // 运行结束后需先重置才能编辑
  const vBefore = app.editor.versions.length;

  console.log('\n[编辑态]');
  fn.setMode(true);
  check(app.mode === 'edit', '进入编辑态（状态 ' + app.run.status + '）');
  check(app.def.nodes.every((n) => n.pos), '全部节点已固定坐标');
  check(app.def.nodes[0].pos && typeof app.def.nodes[0].pos.x === 'number', '坐标字段可用');

  console.log('\n[增删连线]');
  const before = app.def.nodes.length;
  fn.addNode('agent', 'qa');
  check(app.def.nodes.length === before + 1, '添加节点成功（' + app.def.nodes.length + '）');
  const newId = app.def.nodes[app.def.nodes.length - 1].id;
  const edgeCount = app.def.edges.length;

  fn.connect('N6', newId);
  check(app.def.edges.length === edgeCount + 1, '连线 N6 → ' + newId + ' 建立');
  check(app.def.nodes.find((n) => n.id === newId).deps.includes('N6'), '依赖自动写入 deps');

  fn.connect('N6', newId);
  check(app.def.edges.length === edgeCount + 1, '重复连线被忽略');

  fn.connect(newId, newId);
  check(app.def.edges.length === edgeCount + 1, '自连被拒绝');

  fn.connect('N10', 'N6');
  check(app.def.edges.length === edgeCount + 1, '成环连线被拒绝');

  console.log('\n[拖拽时连线跟随]');
  const rec = app.edgeEls['N6>' + newId];
  check(!!rec, '新连线已渲染出元素');
  const dBefore = rec ? rec.path.getAttribute('d') : null;
  // 模拟真实拖拽路径：pointerdown 建立 drag 状态 → pointermove 更新坐标并重绘边
  app.nodeDrag = { id: newId, dx: 10, dy: 10, moved: false };
  fn.onPointerMove({ clientX: 520, clientY: 430 });
  const dAfter = rec ? rec.path.getAttribute('d') : null;
  check(dAfter && dBefore && dAfter !== dBefore, '新连线随节点移动实时重绘');
  // 拖动的是目标节点：横向边落在目标左边界，纵向边落在目标顶部。
  const nums = String(dAfter || '').match(/-?\d+(?:\.\d+)?/g) || [];
  const endX = Number(nums[nums.length - 2]);
  const endY = Number(nums[nums.length - 1]);
  const target = app.pos[newId], source = app.pos.N6;
  const horizontal = target.x > source.x + 12;
  const expX = horizontal ? target.x - 4 : target.x + 100;
  const validY = horizontal
    ? endY > target.y && endY < target.y + 70
    : Math.abs(endY - (target.y - 4)) < 1.5;
  check(Math.abs(endX - expX) < 1.5 && validY,
        '重绘路径终点跟随节点（终点 ' + endX.toFixed(0) + ',' + endY.toFixed(0) +
        '，目标节点 ' + target.x.toFixed(0) + ',' + target.y.toFixed(0) + '）');
  fn.onPointerUp({ clientX: 520, clientY: 430 });
  check(app.def.nodes.find((n) => n.id === newId).pos &&
        typeof app.def.nodes.find((n) => n.id === newId).pos.x === 'number', '松手后坐标写入定义');

  // 既有节点（N6）拖着也要带着线走
  const recOld = app.edgeEls['N6>N8'];
  check(!!recOld, '已存在连线有元素（N6→N8）');
  const dOldBefore = recOld ? recOld.path.getAttribute('d') : null;
  app.nodeDrag = { id: 'N6', dx: 5, dy: 5, moved: false };
  fn.onPointerMove({ clientX: 600, clientY: 300 });
  const dOldAfter = recOld ? recOld.path.getAttribute('d') : null;
  check(dOldAfter && dOldBefore && dOldAfter !== dOldBefore, '已有节点的连线同样实时跟随');
  fn.onPointerUp({ clientX: 600, clientY: 300 });

  console.log('\n[属性编辑]');
  fn.editNode(newId, 'name', '交付前复核');
  check(app.def.nodes.find((n) => n.id === newId).name === '交付前复核', '节点改名生效');
  check(app.dirty === true, '标记为未保存');

  console.log('\n[保存与发布]');
  await fn.doSave();
  check(app.dirty === false, '保存后 dirty 复位');
  check(app.errors.length === 0, '保存无校验错误');

  await fn.doPublish();
  const versions = app.editor.versions.map((v) => v.version);
  check(app.editor.versions.length === vBefore + 1,
        '发布产生新版本（' + versions.join(', ') + '）');

  console.log('\n[选中与删除]');
  // 复刻浏览器真实事件序列：pointerdown 在节点上（随后 click 可能被指针捕获改派到画布）
  const gNew = app.nodeEls[newId];
  check(!!gNew, '新节点有对应元素');
  const evDown = (target) => ({
    type: 'pointerdown', button: 0, target, pointerId: 1, clientX: 520, clientY: 430,
    preventDefault() {}, stopPropagation() {}
  });
  const evUp = (target) => ({ type: 'pointerup', target, pointerId: 1, clientX: 520, clientY: 430 });
  const tap = (node) => { node.dispatchEvent(evDown(node)); svgEl.dispatchEvent(evUp(svgEl)); };

  fn.selectNode('N1');
  check(app.selected === 'N1', '先选中一个已有节点');
  tap(gNew);
  check(app.selected === newId, 'pointerdown 即选中新节点（不依赖被改派的 click）');
  check(gNew.classList.contains('selected'), '新节点显示选中高亮');

  // 被指针捕获改派到画布的 click 不应该把选中清掉
  svgEl.dispatchEvent({ type: 'click', target: svgEl, clientX: 520, clientY: 430 });
  check(app.selected === newId, '画布上的 click 不会误清除选中');

  // 点空白：pointerdown → pointerup → click，应当取消选中
  svgEl.dispatchEvent({ type: 'pointerdown', button: 0, target: svgEl, pointerId: 2,
                        clientX: 50, clientY: 50, preventDefault() {}, stopPropagation() {} });
  svgEl.dispatchEvent({ type: 'pointerup', target: svgEl, pointerId: 2, clientX: 50, clientY: 50 });
  svgEl.dispatchEvent({ type: 'click', target: svgEl, clientX: 50, clientY: 50 });
  check(app.selected === null, '点空白可取消选中');

  // 再选中另一个“老”节点，验证不是只有新节点能选中
  const g4 = app.nodeEls['N4'];
  tap(g4);
  check(app.selected === 'N4', '已有节点同样可以再次选中');
  check(g4.classList.contains('selected'), '已有节点显示选中高亮');

  // 选中后逐个节点操作：编辑按钮 → 打开属性表单
  tap(gNew);
  check(!!gNew.__edit, '节点带编辑按钮');
  gNew.__edit.dispatchEvent({ type: 'click', stopPropagation: () => {} });
  check(app.selected === newId, '编辑按钮保持选中该节点');
  const paneHtml = String(document.querySelector('#paneDetail').innerHTML);
  check(paneHtml.indexOf('编辑节点') >= 0 && paneHtml.indexOf(newId) >= 0,
        '右栏显示该节点的属性表单');

  check(app.def.nodes.length === 11, '删除前 11 个节点');
  gNew.__del.dispatchEvent({ type: 'click', stopPropagation: () => {} });
  check(!app.def.nodes.some((n) => n.id === newId), '点删除按钮后节点已删除');
  check(!app.def.edges.some((e) => e.source === newId || e.target === newId), '相关连线一并删除');
  check(app.def.nodes.every((n) => !(n.deps || []).includes(newId)), '其他节点的 deps 已清理');
  check(app.selected === null, '删除后选中态清空');

  // 键盘删除路径同样可用
  fn.selectNode('N4');
  fn.deleteSelection();
  check(!app.def.nodes.some((n) => n.id === 'N4'), 'Delete 键路径可删除节点（N4）');
  check(!app.def.edges.some((e) => e.source === 'N4' || e.target === 'N4'), 'N4 的连线已清理');

  await fn.doSave();
  check(app.dirty === false, '删除后保存成功');

  console.log('\n[版本切换]');
  await fn.switchVersion('v1');
  check(app.viewVersion === 'v1', '切换到 v1');
  check(app.mode === 'view', '已发布版本为只读');
  check(app.def.nodes.length === 10, 'v1 定义节点数 10（不随后续编辑变化）');

  await fn.switchVersion('draft');
  check(app.viewVersion === 'draft', '切回草稿');
  check(app.def.nodes.length === 9, '草稿节点数 9（删了 N11 和 N4，已落库）');

  console.log('\n[只读渲染]');
  fn.render();
  check(true, 'render 无异常');

  console.log('\n[需求输入 → 生成工作流]');
  fn.openReq();
  check(byId['reqPanel'].hidden === false, '需求面板打开');
  byId['reqText'].value = '做一个内部管理后台：登录鉴权、订单支付结算、报表导出，需要权限审计';
  await fn.doGenerate();
  check(byId['reqTrace'].innerHTML.indexOf('trace-item') >= 0, '生成依据已展示');
  check(app.def.nodes.length >= 10, '生成后画布展示新草稿（' + app.def.nodes.length + ' 节点）');
  check(app.def.nodes.some((n) => n.type === 'approval'), '生成图含人工审批节点');
  check(app.def.nodes.some((n) => n.type === 'condition'), '生成图含质量门条件节点');
  check(app.mode === 'view', '生成后处于只读展示');
  check(app.dirty === false, '生成后草稿已保存');
  check(byId['reqLabel'].textContent.indexOf('需求：') === 0, '工作流栏展示需求摘要');
  check(byId['wfName'].textContent.length > 0, '工作流名称已更新');

  console.log('\n[运行历史回看]');
  const fakeDef = {
    workflow_id: 'wf_login_001', name: '历史测试', version: 'v1',
    nodes: [
      { id: 'N1', type: 'start', role: 'system', name: '需求输入', deps: [] },
      { id: 'N2', type: 'agent', role: 'coder', name: '编译实现', deps: ['N1'], max_retries: 2 }
    ],
    edges: [{ source: 'N1', target: 'N2' }]
  };
  const fakeItem = {
    run_id: 'HIST-TEST', workflow_id: 'wf_login_001', workflow_name: '历史测试',
    workflow_version: 'v1', requirement: '历史需求：编译失败场景',
    status: 'FAILED', elapsed: 12.3, started_at: 1, finished_at: 2, ended_ts: 1760000000,
    stats: { dispatched: 2, retries: 1, skipped: 0, human_waits: 0 },
    nodes: [
      { id: 'N1', type: 'start', role: 'system', name: '需求输入', status: 'SUCCESS',
        deps: [], attempts: [{ attempt: 1, started: 0, ended: 0.1, result: 'success' }], logs: [] },
      { id: 'N2', type: 'agent', role: 'coder', name: '编译实现', status: 'FAILED', deps: ['N1'],
        max_retries: 2, retry_count: 1, message: '编译门：NameError', gate: { compile: 'fail' },
        attempts: [{ attempt: 1, started: 1, ended: 2, result: 'failed', error_code: 'E-COMPILE' }],
        logs: ['✗ 编译门未通过'] }
    ],
    edges: [{ source: 'N1', target: 'N2', active: true }],
    events: [{ seq: 1, ts: 0.1, type: 'node.status', node_id: 'N1', message: 'N1 → SUCCESS' }],
    definition: fakeDef
  };
  const origFetch = globalThis.fetch;
  globalThis.fetch = (url, opts) => String(url).indexOf('/api/runs/HIST-TEST') >= 0
    ? Promise.resolve({ ok: true, status: 200, json: async () => ({ run: fakeItem }) })
    : origFetch(url, opts);
  await fn.viewHistory('HIST-TEST');
  globalThis.fetch = origFetch;
  check(app.frozen === true, '进入历史回看并冻结实时刷新');
  check(app.def.name === '历史测试' && app.def.nodes.length === 2, '画布切换为该次运行的 DAG');
  check(byId['histBanner'].hidden === false, '历史提示条出现');
  check(byId['histBannerText'].textContent.indexOf('HIST-TEST') >= 0, '提示条标注运行 ID');

  await fn.refresh();                       // 冻结期间实时快照不应覆盖
  check(app.def.name === '历史测试', '冻结期间状态不被实时快照覆盖');

  fn.selectNode('N2');
  const errHtml = String(byId['paneDetail'].innerHTML);
  check(errHtml.indexOf('E-COMPILE') >= 0, '节点详情展示错误码');
  check(errHtml.indexOf('编译门：NameError') >= 0, '节点详情展示错误原因');
  check(errHtml.indexOf('尝试记录') >= 0, '节点详情展示尝试记录');

  await fn.exitHistory();
  check(app.frozen === false, '退出历史回看');
  check(byId['histBanner'].hidden === true, '历史提示条隐藏');
  check(app.viewVersion === 'draft' && app.def.name !== '历史测试', '恢复当前草稿');

  console.log('\n[分解预览：流程图直接覆盖画布，可撤销]');
  const draftNameBefore = app.def.name;
  const prevDef = {
    workflow_id: 'wf_login_001', name: '预览规格 · 任务编排', version: null,
    template: 'default', preview: true, spec_id: 'SP-PREVIEW',
    nodes: [
      { id: 'P1', type: 'start', role: 'system', name: '需求输入', deps: [] },
      { id: 'P2', type: 'agent', role: 'coder', name: '编译实现', deps: ['P1'],
        out_schema: 'code-result-v1', max_retries: 2 },
      { id: 'P3', type: 'end', role: 'system', name: '交付', deps: ['P2'] }
    ],
    edges: [{ source: 'P1', target: 'P2' }, { source: 'P2', target: 'P3' }]
  };
  const prevBody = {
    ok: true, preview: true, spec: { id: 'SP-PREVIEW', name: '预览规格', revision: 1 },
    definition: prevDef, template: 'default',
    tasks: [
      { id: 'T1', title: '登录接口', role: 'coder', depends_on: [], estimate: 2,
        acceptance: ['返回 200'], out_schema: 'code-result-v1', source: 'M1' }
    ],
    trace: ['M1 → T1'],
    stats: { modules: 1, tasks: 1, estimate: 2, depth: 2 },
    graph: { nodes: 3, edges: 2 }
  };
  const realSpecId = app.specId;
  app.specId = 'SP-PREVIEW';
  globalThis.fetch = (url, opts) => String(url).indexOf('/api/specs/SP-PREVIEW/preview') >= 0
    ? Promise.resolve({ ok: true, status: 200, json: async () => prevBody })
    : origFetch(url, opts);
  await fn.decomposeSpec();
  globalThis.fetch = origFetch;
  check(!!app.preview, '进入预览状态（只读预演）');
  check(app.def && app.def.name === '预览规格 · 任务编排',
        '预览的流程图直接覆盖了原来的画布');
  check(app.def.nodes.length === 3, '画布节点来自预演定义（3 节点）');
  check(app.nodes.every((n) => n.status === 'PENDING'), '预览节点全部按待运行渲染');
  check(byId['btnSpecDecompose'].textContent === '取消分解预览', '按钮变为「取消分解预览」');
  check(byId['btnSpecDecompose'].classList.contains('warn'), '按钮切换为警示样式');
  check(byId['wfState'].textContent.indexOf('预览') >= 0, '工作流栏标注「预览 · 未保存」');
  check(byId['reqLabel'].textContent.indexOf('预览中') >= 0, '来源标签标注预览中');
  check(byId['specDecomp'].hidden === false, '任务清单与流程图同时可见');
  check(fn.isEditable() === false, '预览期间画布不可编辑');

  await fn.cancelPreview();
  check(app.preview === null, '退出预览状态');
  check(byId['btnSpecDecompose'].textContent === '分解预览', '按钮恢复为「分解预览」');
  check(app.def.name === draftNameBefore, '画布回到之前那张图');
  check(byId['specDecomp'].hidden === true, '任务清单已收起');
  check(app.mode === 'view', '退出后仍为只读查看草稿');
  app.specId = realSpecId;          // 预览用的是假规格 ID，恢复真实选中项

  console.log('\n[新建规格可取消，不会是死胡同]');
  await fn.loadSpecs();                 // 规格列表在打开面板时才拉
  check((app.specs || []).length > 0, '已加载规格列表（' + (app.specs || []).length + ' 份）');
  const specBefore = app.specId;
  fn.newSpec();
  check(app.specId === null, '新建后不绑定任何已保存规格');
  check(byId['btnSpecBack'].hidden === false, '出现「取消新建」');
  check(byId['specSel'].value === '', '下拉框显示「新建规格（未保存）」占位');
  check(fn.isEditable() === true || app.mode === 'view', '新建表单可编辑');
  await fn.backToSavedSpec();
  check(byId['btnSpecBack'].hidden === true, '取消新建后按钮收起');
  check(!!app.specId && app.specId === specBefore, '回到原来那份规格');
  check(byId['btnSpecDecompose'].textContent === '分解预览', '预览按钮状态同步复位');

  console.log('\n[断线 / 请求失败提示]');
  fn.showNetError('实时连接已断开');
  check(byId['netBanner'].hidden === false, '故障条可见');
  check(byId['netText'].textContent.indexOf('断开') >= 0, '故障条显示原因');
  fn.clearNetError();
  check(byId['netBanner'].hidden === true, '恢复后故障条隐藏');

  console.log('\n[实时推送]');
  const es = globalThis.__es;
  check(!!es && es.url.indexOf('/api/stream') >= 0, '启动时已建立 SSE 连接');
  es.emit('patch', {
    version: 999,
    run: { status: 'RUNNING', elapsed: 1.25 },
    nodes: app.nodes, edges: app.edges, events: []
  });
  check(app.version === 999, '命名事件 patch 已被应用（不是 onmessage）');
  check(app.run.status === 'RUNNING', '运行状态由增量补丁驱动');
  await fn.refresh();                       // 与真实状态重新对齐

  const pass = OK.filter(Boolean).length;
  console.log('\n结果:', pass === OK.length ? 'PASS' : 'FAIL', '(' + pass + '/' + OK.length + ')');
  process.exit(pass === OK.length ? 0 : 1);
})().catch((e) => {
  console.error('运行时异常:', e && e.stack ? e.stack : e);
  process.exit(1);
});
