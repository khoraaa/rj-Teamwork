#!/usr/bin/env node
/* D 任务 · 真实浏览器验收：模型配置面板、设计规格文档导入、分层任务清单、运行产物页签。

覆盖：
  1. 页面零 console 错误加载，顶栏模式徽标默认显示「模拟」；
  2. 「模型」面板可填 Base URL / 模型 / 密钥并保存，徽标切换成真实模型，密钥只回掩码；
     连接一个不存在的地址时，「测试连接」给出明确错误而不是静默失败；
  3. 「设计规格」→ 粘贴 Markdown → 解析：模块、验收条款、约束、trace 都进了表单；
  4. 保存后「分解预览」：任务清单按阶段分组，任务写明做什么/怎么做/目标文件，子步骤可展开；
  5. 「运行产物」页签能打开并给出明确状态（没有产物时说清楚，不是空白）。

依赖：Node.js 22+（内置 fetch/WebSocket）、Chrome 或 Chromium（CHROME_PATH 可指向 Edge）。
用法：node tools/llm_ui_e2e.js [base_url] [artifact_dir]
*/
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const http = require('http');

const ROOT = path.join(__dirname, '..');
const BASE = process.argv[2] || 'http://127.0.0.1:8010';
const ARTIFACT_DIR = path.resolve(
  process.argv[3] || path.join(ROOT, 'acceptance-results', 'llm-ui-e2e'));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const checks = [];
const runtimeErrors = [];

function check(cond, msg) {
  checks.push(!!cond);
  console.log((cond ? '  ✓ ' : '  ✗ ') + msg);
  return cond;
}

function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/chromium',
    '/usr/bin/google-chrome',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
  ].filter(Boolean);
  return candidates.find((p) => {
    try { return fs.statSync(p).isFile(); } catch (e) { return false; }
  });
}

class CDP {
  constructor(wsUrl) {
    this.nextId = 1;
    this.pending = new Map();
    this.events = new Map();
    this.ws = new WebSocket(wsUrl);
    this.opened = new Promise((resolve, reject) => {
      this.ws.addEventListener('open', resolve, { once: true });
      this.ws.addEventListener('error', reject, { once: true });
    });
    this.ws.addEventListener('message', (event) => {
      let msg;
      try { msg = JSON.parse(String(event.data)); } catch (e) { return; }
      if (msg.id) {
        const item = this.pending.get(msg.id);
        if (!item) return;
        this.pending.delete(msg.id);
        if (msg.error) item.reject(new Error(msg.error.message || 'CDP error'));
        else item.resolve(msg.result || {});
        return;
      }
      if (msg.method) {
        for (const handler of (this.events.get(msg.method) || [])) {
          try { handler(msg.params || {}); } catch (e) { runtimeErrors.push(String(e)); }
        }
      }
    });
  }
  on(method, handler) {
    this.events.set(method, (this.events.get(method) || []).concat(handler));
  }
  async call(method, params = {}) {
    await this.opened;
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
      setTimeout(() => {
        if (!this.pending.has(id)) return;
        this.pending.delete(id);
        reject(new Error('CDP 超时: ' + method));
      }, 20000);
    });
  }
  close() { try { this.ws.close(); } catch (e) {} }
}

async function openBrowser() {
  const chrome = findChrome();
  if (!chrome) throw new Error('未找到 Chrome/Chromium；可设置 CHROME_PATH');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'autflow_llmui_'));
  const args = [
    '--headless=new', '--disable-gpu', '--disable-dev-shm-usage',
    '--no-first-run', '--no-default-browser-check', '--no-proxy-server',
    '--remote-debugging-port=0', '--remote-allow-origins=*',
    '--window-size=1440,960', '--user-data-dir=' + profile, 'about:blank',
  ];
  const stderr = [];
  const proc = spawn(chrome, args, { stdio: ['ignore', 'ignore', 'pipe'] });
  proc.stderr.on('data', (c) => { stderr.push(String(c)); if (stderr.length > 80) stderr.shift(); });
  const portFile = path.join(profile, 'DevToolsActivePort');
  const deadline = Date.now() + 20000;
  let port = null;
  while (Date.now() < deadline) {
    if (proc.exitCode != null) throw new Error('浏览器提前退出：' + stderr.join('').slice(-1500));
    try {
      const lines = fs.readFileSync(portFile, 'utf8').trim().split(/\r?\n/);
      if (lines[0]) { port = Number(lines[0]); break; }
    } catch (e) {}
    await sleep(100);
  }
  if (!port) throw new Error('浏览器未打开调试端口：' + stderr.join('').slice(-1500));
  const list = await fetch(`http://127.0.0.1:${port}/json/list`).then((r) => r.json());
  const page = list.find((i) => i.type === 'page');
  if (!page) throw new Error('浏览器未提供 page target');
  return { proc, profile, cdp: new CDP(page.webSocketDebuggerUrl) };
}

const TASKS = [
  { id: 'T1', title: '工程骨架与零依赖约束', phase: '设计', role: '架构师', priority: 'P0',
    estimate: 2, what: '搭出可运行骨架', how: '建 src/ 与 tests/，用标准库写入口',
    files: ['src/main.py'], steps: [{ title: '建目录', how: 'src/ tests/' }],
    acceptance: ['服务能启动'], depends_on: [] },
  { id: 'T2', title: '登录：账号密码与验证码', phase: '编码', role: '后端', priority: 'P0',
    estimate: 4, what: '实现登录并 5 次锁定', how: '在 src/auth.py 写 login()，密码存 sha256',
    files: ['src/auth.py'], steps: [{ title: '写校验', how: '比对 hash 并累计失败次数' }],
    acceptance: ['正确密码能登录', '错误 5 次锁定'], depends_on: ['T1'] },
  { id: 'T3', title: '登录单元测试', phase: '测试', role: '测试', priority: 'P0',
    estimate: 2, what: '覆盖成功/失败/锁定', how: 'tests/test_auth.py 用 unittest 写 3 条',
    files: ['tests/test_auth.py'], steps: [{ title: '写用例', how: 'unittest.TestCase' }],
    acceptance: ['3 条用例通过'], depends_on: ['T2'] },
  { id: 'T4', title: '登录逻辑评审', phase: '评审', role: '评审', priority: 'P1',
    estimate: 1, what: '评审密码存储与锁定策略', how: '逐条核对验收条款并给结论',
    files: [], steps: [{ title: '核对', how: '对照 AC 检查' }],
    acceptance: ['给出结论'], depends_on: ['T3'] },
];

function startFakeModel() {
  let voteSeq = 0;
  return new Promise((resolve) => {
    const srv = http.createServer((req, res) => {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        let payload = {};
        try { payload = JSON.parse(body || '{}'); } catch (e) {}
        const sys = (payload.messages || [])
          .filter((m) => m.role === 'system').map((m) => m.content || '').join('\n');
        const usr = (payload.messages || [])
          .filter((m) => m.role === 'user').map((m) => m.content || '').join('\n');
        let content;
        if (sys.indexOf('主持人') >= 0) {
          content = JSON.stringify({ tasks: TASKS });
        } else if (sys.indexOf('表态') >= 0) {
          voteSeq += 1;
          content = (voteSeq <= 5 && sys.indexOf('评审') >= 0)
            ? JSON.stringify({ stance: 'block', reason: '验收缺「锁定解除」' })
            : JSON.stringify({ stance: 'agree', reason: '分歧已收敛' });
        } else {
          const greet = (sys + usr).indexOf('人工指令') >= 0 ? '已按人工指令调整：' : '';
          if (sys.indexOf('你是评审专家') >= 0) {
            content = greet + '结论：锁定策略要写清阈值。\n要点：\n- 验收缺「解锁」\n- 报表不进本期';
          } else if (sys.indexOf('你是测试工程师') >= 0) {
            content = greet + '结论：用 unittest 跑三条路径。\n要点：\n- 成功/失败/锁定';
          } else if (sys.indexOf('你是开发者') >= 0) {
            content = greet + '结论：三个文件够用。\n要点：\n- src/auth.py 放登录';
          } else if (sys.indexOf('你是架构师') >= 0) {
            content = greet + '结论：全标准库实现。\n要点：\n- JSON 文件存用户';
          } else {
            content = greet + '结论：先做登录，再做报表。\n要点：\n- 登录是 P0';
          }
        }
        const out = JSON.stringify({
          choices: [{ message: { role: 'assistant', content }, finish_reason: 'stop' }],
          usage: { prompt_tokens: 25, completion_tokens: 12 },
        });
        setTimeout(() => {
          try {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(out);
          } catch (e) {}
        }, 450);
      });
    });
    srv.listen(0, '127.0.0.1', () => {
      resolve({ srv, url: 'http://127.0.0.1:' + srv.address().port + '/v1' });
    });
  });
}

const MD = [
  '# 运维工单系统 v1',
  '',
  '面向运维值班同学的工单系统，先做登录与工单流转。',
  '',
  '## 模块',
  '',
  '### 登录：账号密码 + 短信验证码',
  '账号密码登录，连续错误 5 次锁定。',
  '验收：错误提示明确',
  '验收：锁定 5 次后可自助解锁',
  '',
  '### 工单流转',
  '创建、指派、关闭工单，全程留痕。',
  '- [ ] 状态变更写入审计日志',
  '验收：越权指派被拒绝',
  '',
  '## 全局约束',
  '- 不新增第三方依赖',
  '- 密码只存 hash',
].join('\n');

async function main() {
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });
  console.log('模型配置 / 规格导入 / 分层任务 / 多智能体讨论 浏览器验收:', BASE);
  const fake = await startFakeModel();
  console.log('假模型端点:', fake.url);
  const browser = await openBrowser();
  const cdp = browser.cdp;

  const evaluate = async (expression) => {
    const result = await cdp.call('Runtime.evaluate', {
      expression, awaitPromise: true, returnByValue: true, userGesture: true,
    });
    if (result.exceptionDetails) {
      const d = result.exceptionDetails;
      throw new Error((d.exception && d.exception.description) || d.text || '页面脚本异常');
    }
    return result.result ? result.result.value : undefined;
  };
  const waitFor = async (expr, timeout = 20000, label = expr) => {
    const end = Date.now() + timeout;
    let last = '';
    while (Date.now() < end) {
      try { if (await evaluate(expr)) return true; } catch (e) { last = e.message; }
      await sleep(120);
    }
    throw new Error('等待超时：' + label + (last ? '；' + last : ''));
  };
  const text = (sel) => evaluate(
    `(document.querySelector(${JSON.stringify(sel)}) || {}).textContent || ''`);
  const shot = async (name) => {
    const png = await cdp.call('Page.captureScreenshot', { format: 'png', fromSurface: true });
    fs.writeFileSync(path.join(ARTIFACT_DIR, name + '.png'), Buffer.from(png.data, 'base64'));
  };

  let failed = false;
  try {
    await cdp.call('Page.enable');
    await cdp.call('Runtime.enable');
    await cdp.call('Log.enable');
    await cdp.call('Emulation.setDeviceMetricsOverride', {
      width: 1440, height: 960, deviceScaleFactor: 1, mobile: false,
    });
    cdp.on('Runtime.exceptionThrown', (p) => {
      const d = p.exceptionDetails || {};
      runtimeErrors.push(d.exception ? (d.exception.description || d.text) : (d.text || '异常'));
    });
    cdp.on('Log.entryAdded', (p) => {
      const e = p.entry || {};
      if (e.url && /favicon\.ico/.test(e.url)) return;
      if (e.level === 'error') runtimeErrors.push(e.text || 'log error');
    });

    await cdp.call('Page.navigate', { url: BASE + '/' });
    await waitFor("document.readyState === 'complete' && typeof app !== 'undefined'"
      + ' && !!app.editor && app.built === true', 30000, '页面加载完成');

    console.log('\n[1] 默认状态：模拟模式');
    check((await text('#btnModel')).indexOf('模型') >= 0
      && (await text('#btnModel')).indexOf('模拟') >= 0,
      '顶栏只有一个入口，且标签里保留「模型」二字（' + (await text('#btnModel')) + '）');
    check(await evaluate("!document.querySelector('#modeChip')"
      + " && document.querySelectorAll('[onclick*=\"openModel\"]').length === 1"),
      '顶栏没有重复的模型入口（只有 1 个入口，旧的徽标已合并）');

    check(await evaluate("(() => { const boxes = Array.from(document.querySelectorAll('#phaseLayer .phase-box'));"
      + " if (!boxes.length) return false;"
      + " return boxes.every((a, i) => boxes.slice(i + 1).every(b => {"
      + " const x = a.getBBox(), y = b.getBBox();"
      + " return x.x + x.width <= y.x || y.x + y.width <= x.x"
      + " || x.y + x.height <= y.y || y.y + y.height <= x.y; })); })()"),
      '画布按阶段绘制独立分区，分区框不互相重叠');
    check(await evaluate("document.querySelectorAll('.node .part-strip').length >= 3"),
      '节点顶部有分组同色细条');
    check(await evaluate("document.querySelectorAll('#phaseTaskList .phase-task-group').length > 0"
      + " && document.querySelectorAll('#phaseTaskList .phase-task-row').length > 0"),
      '左侧任务清单按阶段展示任务');
    check(await evaluate("(() => { const row = document.querySelector('#phaseTaskList .phase-task-row');"
      + " if (!row) return false; row.click();"
      + " const node = app.nodes.find(n => n.task_id === row.dataset.taskId)"
      + " || app.def.nodes.find(n => n.task_id === row.dataset.taskId);"
      + " return !!node && app.selected === node.id; })()"),
      '点击左侧任务会选中对应画布节点');

    console.log('\n[2] 模型配置面板');
    await evaluate("document.querySelector('#btnModel').click()");
    await waitFor("!document.querySelector('#modelPanel').hidden", 10000, '模型面板打开');
    await shot('01-model-panel');
    check(await evaluate("document.querySelectorAll('#setProvider option').length >= 4"),
      '服务商预设已填充');
    await evaluate(`(() => {
      const set = (id, v) => { document.querySelector(id).value = v; };
      set('#setBaseUrl', '${fake.url}');
      set('#setModel', 'ui-fake-model');
      set('#setKey', 'sk-ui-test-abcdef123456');
      document.querySelector('#tglUseLLM').checked = true;
    })()`);
    await evaluate("document.querySelector('#btnSaveSettings').click()");
    await waitFor("app.settings && app.settings.active === true", 15000, '配置保存并激活');
    check((await text('#btnModel')).indexOf('真实模型') >= 0,
      '入口切换为真实模型（' + (await text('#btnModel')) + '）');
    const keyHint = await text('#setKeyHint');
    check(keyHint.indexOf('sk-ui-test-abcdef123456') < 0 && keyHint.indexOf('…') >= 0,
      '密钥提示只显示掩码（' + keyHint.trim() + '）');
    check(await evaluate("app.settings.api_key === undefined"),
      '前端 app.settings 里没有明文 api_key');
    check(await evaluate("document.querySelectorAll('#roleAiRows select').length === 5"),
      '多 AI 面板：5 个讨论角色都能分工');
    check(await evaluate("document.querySelectorAll('#partAiRows select').length === 8"),
      '多 AI 面板：8 个任务分组都能指派');
    await evaluate(`(() => {
      document.querySelector('#aiLabel').value = '评审专用';
      document.querySelector('#aiBaseUrl').value = '${fake.url}';
      document.querySelector('#aiModel').value = 'review-model';
      document.querySelector('#aiKey').value = 'sk-ai-ui-777';
      document.querySelector('.ai-form .btn.primary').click();
    })()`);
    await waitFor("app.ais && app.ais.length === 1", 15000, '新增 AI 出现在列表');
    const aiCard = await evaluate("document.querySelector('#aiList').textContent");
    check(aiCard.indexOf('评审专用') >= 0 && aiCard.indexOf('sk-ai-ui-777') < 0,
      '新增 AI 只显示名称与掩码，不回明文');
    const newAiId = await evaluate("app.ais[0].id");
    await evaluate(`(() => { const sel = document.querySelectorAll('#roleAiRows select')[4];
      sel.value = '${newAiId}'; sel.dispatchEvent(new Event('change')); })()`);
    await waitFor(`app.settings && app.settings.role_ais
      && app.settings.role_ais.reviewer === '${newAiId}'`, 10000, '角色分工已保存');
    check(true, '评审角色已指派给新 AI（' + newAiId + '）');
    await evaluate("document.querySelector('#btnProbe').click()");
    await waitFor("(() => { const b = document.querySelector('#probeBox');"
      + " return !b.hidden && (b.classList.contains('ok') || b.classList.contains('bad')); })()",
      30000, '探测返回结果');
    const probeText = await text('#probeBox');
    check(!/失败|错误|E-/.test(probeText), '探测真的连上模型：'
      + probeText.trim().split('\n')[0].slice(0, 60));
    await shot('02-model-probe');
    await evaluate("closeModel(); document.querySelector('#tglDemo').checked = true;"
      + " fetch('/api/config', {method: 'POST', headers: {'Content-Type': 'application/json'},"
      + " body: JSON.stringify({agent: 'mock'}) });");

    console.log('\n[3] 设计规格文档导入');
    await evaluate("openSpec()");
    await waitFor("!document.querySelector('#specPanel').hidden", 10000, '规格面板打开');
    await evaluate("toggleSpecPaste()");
    await evaluate(`(() => {
      const t = document.querySelector('#specPaste');
      t.value = ${JSON.stringify(MD)};
    })()`);
    await evaluate("parseSpecText()");
    await waitFor("app.specModules.length >= 2", 20000, '解析出模块');
    check((await evaluate("app.specModules.length")) >= 2,
      '文档拆出 2 个模块（' + (await evaluate("app.specModules.length")) + '）');
    check((await evaluate("app.specModules[0].name")).indexOf('登录') >= 0,
      '模块名保留原文：' + (await evaluate("app.specModules[0].name")));
    const acc0 = await evaluate("(app.specModules[0].acceptance || []).length");
    check(acc0 >= 2, '第一模块抽到 ' + acc0 + ' 条验收条款');
    const consVal = await evaluate("document.querySelector('#specConstraints').value");
    check(consVal.indexOf('hash') >= 0, '全局约束进了表单：' + JSON.stringify(consVal));
    const traceVal = await text('#specTrace');
    check(traceVal.indexOf('模块') >= 0, '解析 trace 可见：' + JSON.stringify(traceVal.slice(0, 90)));
    check((await text('#importNote')).indexOf('模块') >= 0,
      '导入结果摘要：' + (await text('#importNote')).trim().slice(0, 40));
    await shot('03-spec-import');

    console.log('\n[4] 分层任务清单');
    await evaluate("document.querySelector('#specName').value = '运维工单系统 v1'");
    await evaluate("saveSpec()");
    await waitFor("!!app.specId", 20000, '规格保存完成');
    await evaluate("decomposeSpec()");
    await waitFor("!!app.preview && document.querySelectorAll('#specTasks tr.tt-phase').length > 0",
      30000, '分解预览渲染完成');
    const phases = await evaluate(
      "Array.from(document.querySelectorAll('#specTasks tr.tt-phase')).map(r => r.textContent.trim())");
    check(phases.length >= 5, '任务按 ' + phases.length + ' 个阶段分组');
    check(phases.join('|').indexOf('编码') >= 0 && phases.join('|').indexOf('测试') >= 0,
      '阶段中包含「编码」与「测试」');
    const rows = await evaluate("document.querySelectorAll('#specTasks tbody tr:not(.tt-phase)').length");
    check(rows >= 6, '任务条数 ' + rows + ' 条');
    const whatText = await text('#specTasks');
    check(whatText.indexOf('做什么：') >= 0 && whatText.indexOf('怎么做：') >= 0,
      '每条任务写明「做什么」与「怎么做」');
    check(await evaluate("document.querySelectorAll('#specTasks details').length >= 3"),
      '任务子步骤可展开');
    check(await evaluate("document.querySelectorAll('#specTasks .tc-file').length >= 3"),
      '任务列出目标文件');
    await shot('04-task-hierarchy');

    console.log('\n[5] 运行产物页签');
    await evaluate("document.querySelector('#specPanel').hidden = true");
    await evaluate("document.querySelector('.tab[data-tab=\\\"files\\\"]').click()");
    await waitFor("!document.querySelector('#paneFiles').hidden", 10000, '产物页签打开');
    await waitFor("(() => { const t = document.querySelector('#fileList').textContent || '';"
      + " return t.length > 0 && t.indexOf('加载中') < 0; })()", 15000, '产物列表给出结果');
    const filesText = await text('#fileList');
    check(filesText.trim().length > 0, '产物页签给出明确状态：'
      + filesText.trim().slice(0, 40));
    check((await text('#filesHint')).length > 0, '页签显示工作区提示');
    await shot('05-files-pane');

    console.log('\n[6] 多智能体讨论 → 生成工作流');
    await evaluate("openSpec()");
    await waitFor("!document.querySelector('#specPanel').hidden", 10000, '规格面板重新打开');
    check(await evaluate("!document.querySelector('#btnDiscuss').disabled"),
      '配好模型后「让 AI 讨论」按钮可用');
    await evaluate("document.querySelector('#btnDiscuss').click()");
    try {
      await waitFor("document.querySelectorAll('#discussLog .dis-line').length >= 5", 45000,
        '讨论发言陆续出现');
    } catch (e) {
      const diag = await evaluate("JSON.stringify({sid: app.discuss.sid, busy: app.discuss.busy,"
        + " shown: app.discuss.shown, status: (app.discuss.session || {}).status,"
        + " stage: (app.discuss.session || {}).stage,"
        + " err: (app.discuss.session || {}).error, mins: ((app.discuss.session || {}).minutes || []).length,"
        + " html: (document.querySelector('#discussLog') || {}).innerHTML,"
        + " toast: (document.querySelector('#toastHost') || {}).textContent})");
      console.log('DIAG:', diag, 'JS 错误:', runtimeErrors.slice(-3));
      throw e;
    }
    const lines = await evaluate("document.querySelectorAll('#discussLog .dis-line').length");
    check(lines >= 5, '讨论显示 ' + lines + ' 条发言');
    const who = await evaluate(
      "Array.from(document.querySelectorAll('#discussLog .dis-who')).map(function (e) {"
      + " return e.textContent; })");
    check(['产品经理', '架构师', '开发者', '测试', '评审'].every(r => who.indexOf(r) >= 0),
      '五个角色都发了言：' + who.slice(0, 9).join(' / '));
    check(await evaluate("document.querySelectorAll('#discussLog .dis-points li').length >= 6"),
      '发言要点被结构化渲染');
    check(await evaluate("!document.querySelector('#discussSayRow').hidden"),
      '讨论开始后出现「插话引导」输入框');
    await evaluate("document.querySelector('#discussSay').value = '报表放到二期，先做登录';"
      + " sendDiscussSay();");
    await waitFor("!!document.querySelector('#discussLog .dis-line.r-human')", 20000,
      '人工插话以独立样式进入纪要');
    check((await text('#discussLog .dis-line.r-human')).indexOf('报表放到二期') >= 0,
      '人工内容原样显示');
    await waitFor("Array.from(document.querySelectorAll('#discussLog .dis-text'))"
      + ".some(function (e) { return e.textContent.indexOf('已按人工指令调整') >= 0; })", 25000,
      '后续发言回应了人工引导');
    await waitFor("document.querySelector('#discussLog .dis-done')", 45000, '讨论完成');
    check(await evaluate("!!document.querySelector('.consensus-box')"),
      '完成区显示共识面板');
    const stances = await evaluate(
      "Array.from(document.querySelectorAll('#discussLog .stance')).map(function (e) {"
      + " return e.textContent; })");
    check(stances.length >= 5, '每个角色都有表态：' + stances.slice(0, 5).join(' / '));
    check(/共识：/.test(await text('#discussLog .consensus-box')),
      '共识结论可见：' + (await text('#discussLog .cons-head')).trim());
    const reasons = await evaluate(
      "Array.from(document.querySelectorAll('#discussLog .dis-why')).map(function (e) {"
      + " return e.textContent; })");
    check(reasons.length >= 3, '每条发言都显示主持人选人理由（' + reasons.length + ' 条）');
    const doneText = await text('#discussLog .dis-done');
    check(/\d+ 条任务/.test(doneText), '讨论完成并给出任务数：' + doneText.trim().slice(0, 40));
    check(await evaluate("!!document.querySelector('.dis-apply')"),
      '提供「就按这个编排生成工作流」');
    await shot('06-discussion');
    await evaluate("document.querySelector('.dis-apply').click()");
    await waitFor("document.querySelector('#specPanel').hidden && app.def && app.def.nodes"
      + " && app.def.nodes.some(n => (n.name || '').indexOf('登录') >= 0)", 30000,
      '按讨论结果生成并显示工作流');
    check((await evaluate("app.def.nodes.length")) >= 8,
      '画布上生成了 ' + (await evaluate("app.def.nodes.length")) + ' 个节点');
    const valid = await evaluate(
      "fetch('/api/workflow').then(r => r.json()).then(d => !!d.valid)");
    check(valid, '生成的图通过 DAG 校验');
    await shot('07-workflow-from-discussion');

    console.log('\n[7] 无脚本错误');
    check(runtimeErrors.length === 0,
      '控制台无未捕获错误' + (runtimeErrors.length ? '：' + runtimeErrors.slice(0, 2).join(' | ') : ''));
  } catch (err) {
    failed = true;
    check(false, '执行异常：' + (err && err.message ? err.message : err));
    try { await shot('99-failure'); } catch (e) {}
  } finally {
    cdp.close();
    try { browser.proc.kill(); } catch (e) {}
    try { fake.srv.close(); } catch (e) {}
  }

  const ok = checks.filter(Boolean).length;
  const total = checks.length;
  console.log('\n结果: ' + (!failed && ok === total ? 'PASS' : 'FAIL') + ' (' + ok + '/' + total + ')');
  process.exit(!failed && ok === total ? 0 : 1);
}

main();