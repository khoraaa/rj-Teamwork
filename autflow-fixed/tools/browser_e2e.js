#!/usr/bin/env node
/* C 任务 · 真实浏览器端到端验收。

使用 Chrome DevTools Protocol，不依赖 npm 包或 CDN。浏览器中真实执行：
需求输入 → 生成 DAG → 编辑保存/校验/发布 → 运行 → 审批 → 历史回看，
并验证 HTTP/SSE 故障提示。截图写入指定产物目录。

依赖：Node.js 22+（内置 fetch/WebSocket）、Chrome 或 Chromium。
用法：node tools/browser_e2e.js [base_url] [artifact_dir]
*/
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const ROOT = path.join(__dirname, '..');
const BASE = process.argv[2] || 'http://127.0.0.1:8010';
const ARTIFACT_DIR = path.resolve(
  process.argv[3] || path.join(ROOT, 'acceptance-results', 'browser-e2e'));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const checks = [];
const runtimeErrors = [];

function check(condition, message) {
  checks.push(!!condition);
  console.log((condition ? '  ✓ ' : '  ✗ ') + message);
  return condition;
}

function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
  ].filter(Boolean);
  return candidates.find((item) => {
    try { return fs.statSync(item).isFile(); } catch (e) { return false; }
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
        reject(new Error('CDP command timeout: ' + method));
      }, 20000);
    });
  }
  close() {
    try { this.ws.close(); } catch (e) {}
  }
}

async function openBrowser() {
  const chrome = findChrome();
  if (!chrome) throw new Error('未找到 Chrome/Chromium；可设置 CHROME_PATH');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'autflow_chrome_'));
  const args = [
    '--headless=new',
    '--disable-gpu',
    '--disable-dev-shm-usage',
    '--no-first-run',
    '--no-default-browser-check',
    '--no-proxy-server',
    '--remote-debugging-port=0',
    '--remote-allow-origins=*',
    '--window-size=1440,960',
    '--user-data-dir=' + profile,
    'about:blank',
  ];
  if (typeof process.getuid === 'function' && process.getuid() === 0) args.unshift('--no-sandbox');
  const stderr = [];
  const proc = spawn(chrome, args, { stdio: ['ignore', 'ignore', 'pipe'] });
  proc.stderr.on('data', (chunk) => {
    stderr.push(String(chunk));
    if (stderr.length > 80) stderr.shift();
  });

  const portFile = path.join(profile, 'DevToolsActivePort');
  const deadline = Date.now() + 20000;
  let port = null;
  while (Date.now() < deadline) {
    if (proc.exitCode != null) {
      throw new Error('Chrome 提前退出：' + stderr.join('').slice(-3000));
    }
    try {
      const lines = fs.readFileSync(portFile, 'utf8').trim().split(/\r?\n/);
      if (lines[0]) { port = Number(lines[0]); break; }
    } catch (e) {}
    await sleep(100);
  }
  if (!port) throw new Error('Chrome 未打开调试端口：' + stderr.join('').slice(-3000));

  const list = await fetch(`http://127.0.0.1:${port}/json/list`).then((r) => r.json());
  const page = list.find((item) => item.type === 'page');
  if (!page) throw new Error('Chrome 未提供 page target');
  return {
    proc,
    profile,
    cdp: new CDP(page.webSocketDebuggerUrl),
    stderr,
  };
}

async function main() {
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });
  console.log('浏览器验收:', BASE);
  const browser = await openBrowser();
  const cdp = browser.cdp;
  let currentStage = 'startup';

  const evaluate = async (expression) => {
    const result = await cdp.call('Runtime.evaluate', {
      expression, awaitPromise: true, returnByValue: true, userGesture: true,
    });
    if (result.exceptionDetails) {
      const detail = result.exceptionDetails.exception
        && result.exceptionDetails.exception.description;
      throw new Error(detail || result.exceptionDetails.text || '页面脚本异常');
    }
    return result.result ? result.result.value : undefined;
  };
  const waitFor = async (expression, timeout = 30000, label = expression) => {
    const deadline = Date.now() + timeout;
    let lastError = '';
    while (Date.now() < deadline) {
      try {
        if (await evaluate(expression)) return true;
      } catch (e) {
        lastError = e.message;
      }
      await sleep(120);
    }
    throw new Error('等待超时：' + label + (lastError ? '；' + lastError : ''));
  };
  const click = async (selector) => evaluate(`(() => {
    const el = document.querySelector(${JSON.stringify(selector)});
    if (!el) throw new Error('缺少元素 ${selector}');
    el.click();
    return true;
  })()`);
  const setValue = async (selector, value, eventName = 'input') => evaluate(`(() => {
    const el = document.querySelector(${JSON.stringify(selector)});
    if (!el) throw new Error('缺少输入元素 ${selector}');
    el.value = ${JSON.stringify(value)};
    el.dispatchEvent(new Event(${JSON.stringify(eventName)}, { bubbles: true }));
    return el.value;
  })()`);
  const screenshot = async (name) => {
    const shot = await cdp.call('Page.captureScreenshot', {
      format: 'png', fromSurface: true, captureBeyondViewport: false,
    });
    const file = path.join(ARTIFACT_DIR, name + '.png');
    fs.writeFileSync(file, Buffer.from(shot.data, 'base64'));
    return file;
  };

  try {
    await cdp.call('Page.enable');
    await cdp.call('Runtime.enable');
    await cdp.call('Network.enable');
    await cdp.call('Log.enable');
    await cdp.call('Emulation.setDeviceMetricsOverride', {
      width: 1440, height: 960, deviceScaleFactor: 1, mobile: false,
    });
    cdp.on('Runtime.exceptionThrown', (params) => {
      const text = params.exceptionDetails
        ? (params.exceptionDetails.text || '') : 'Runtime exception';
      runtimeErrors.push(text);
    });
    cdp.on('Log.entryAdded', (params) => {
      const entry = params.entry || {};
      if (entry.url && /favicon\.ico(?:$|\?)/.test(entry.url)) return;
      if (entry.level === 'error') runtimeErrors.push(entry.text || 'log error');
    });

    currentStage = '页面加载';
    await cdp.call('Page.navigate', { url: BASE + '/' });
    await waitFor(
      "document.readyState === 'complete' && typeof app !== 'undefined' && !!app.editor && app.built === true",
      30000, '编辑器与画布加载完成');
    check(true, '真实浏览器成功加载编辑器与画布');

    currentStage = '错误提示';
    await cdp.call('Network.emulateNetworkConditions', {
      offline: true, latency: 0, downloadThroughput: 0, uploadThroughput: 0,
    });
    await evaluate("api('/api/validate', {workflow: app.def}).catch(() => null)");
    await waitFor("document.querySelector('#netBanner') && !document.querySelector('#netBanner').hidden",
                  10000, '网络错误提示条出现');
    const netText = await evaluate("document.querySelector('#netText').textContent");
    check(/连不上|请求失败|网络/.test(netText), '网络失败显示明确错误提示');
    await screenshot('01-network-error');
    await cdp.call('Network.emulateNetworkConditions', {
      offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1,
    });
    await evaluate('retryNet(); true');
    await waitFor("document.querySelector('#netBanner').hidden === true",
                  15000, '网络恢复后错误提示消失');
    check(true, '网络恢复后故障提示可清除');
    runtimeErrors.length = 0;  // 离线故障是本用例主动注入，不作为未捕获错误。

    currentStage = '需求生成';
    await click('#btnReq');
    await waitFor("document.querySelector('#reqPanel').hidden === false", 5000,
                  '需求面板打开');
    const requirement = '浏览器验收：内部管理后台，支持登录鉴权、订单支付结算、报表导出和权限审计';
    await setValue('#reqText', requirement);
    await click('#btnGen');
    await waitFor("app.run.status === 'IDLE' && app.viewVersion === 'draft' && " +
                  "app.def && app.def.requirement === " + JSON.stringify(requirement) +
                  " && app.editor && app.editor.requirement === " + JSON.stringify(requirement),
                  30000, '需求生成并回填草稿');
    const generatedCount = await evaluate('app.def.nodes.length');
    check(generatedCount >= 10, `需求生成 ${generatedCount} 个节点`);
    check(await evaluate("document.querySelector('#reqTrace').innerHTML.indexOf('trace-item') >= 0"),
          '画布展示生成依据');
    check(await evaluate("document.querySelector('#reqLabel').textContent.indexOf('需求：') === 0"),
          '工作流栏显示需求摘要');
    await screenshot('02-generated');

    currentStage = '画布编辑';
    await evaluate(`(() => {
      const el = document.querySelector('#tglEdit');
      el.checked = true;
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()`);
    await waitFor("app.mode === 'edit'", 5000, '进入编辑态');
    const oldCount = await evaluate('app.def.nodes.length');
    await click('.pal[data-kind="agent"]');
    await waitFor(`app.def.nodes.length === ${oldCount + 1}`, 5000, '新增节点');
    const newNodeId = await evaluate('app.def.nodes[app.def.nodes.length - 1].id');
    await evaluate(`connectNodes('N3', ${JSON.stringify(newNodeId)}); true`);
    await waitFor(`app.def.edges.some(e => e.source === 'N3' && e.target === ${JSON.stringify(newNodeId)})`,
                  5000, '新增连线');
    await evaluate(`selectNode(${JSON.stringify(newNodeId)}); true`);
    await waitFor("document.querySelector('#paneDetail').innerHTML.indexOf('编辑节点') >= 0",
                  5000, '节点属性表单');
    await setValue('#paneDetail input', '浏览器验收复核');
    await waitFor(`app.def.nodes.some(n => n.id === ${JSON.stringify(newNodeId)} && n.name === '浏览器验收复核')`,
                  5000, '节点改名');
    check(await evaluate('app.dirty === true'), '编辑后显示未保存状态');

    await click('#btnSave');
    await waitFor('app.dirty === false', 15000, '保存草稿');
    check(true, '画布编辑保存成功');
    await click('#btnValidate');
    await waitFor("document.querySelector('#toast').textContent.indexOf('校验通过') >= 0",
                  10000, 'DAG 校验通过');
    check(await evaluate("document.querySelector('#toast').textContent.indexOf('E-CYCLE') >= 0"),
          '校验同时展示成环负例拦截结果');
    const versionsBefore = await evaluate('app.editor.versions.length');
    await click('#btnPublish');
    await waitFor(`app.editor.versions.length === ${versionsBefore + 1}`, 15000, '发布版本');
    check(true, '编辑后的草稿发布为新版本');
    await screenshot('03-edited-validated');

    currentStage = '运行与审批';
    await evaluate(`(() => {
      const demo = document.querySelector('#tglDemo');
      demo.checked = false;
      demo.dispatchEvent(new Event('change', { bubbles: true }));
      const speed = document.querySelector('#selSpeed');
      speed.value = '8';
      speed.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()`);
    await click('#btnStart');
    await waitFor("['RUNNING','WAITING_HUMAN','SUCCESS'].includes(app.run.status)", 15000,
                  '运行启动');
    check(await evaluate("document.querySelector('#runId').textContent.indexOf('RUN-') === 0"),
          '顶栏显示运行状态和 Run ID');
    await waitFor("app.run.status === 'WAITING_HUMAN'", 60000, '运行停在审批');
    check(true, '画布状态与服务端同步为待人工审批');
    const approvalId = await evaluate(
      "app.nodes.find(n => n.type === 'approval' && n.status === 'WAITING_HUMAN').id");
    await evaluate(`selectNode(${JSON.stringify(approvalId)}); true`);
    await waitFor("document.querySelector('#apComment') !== null", 5000, '审批意见输入框');
    const approvalComment = '浏览器验收：批准上线';
    await setValue('#apComment', approvalComment);
    await evaluate("document.querySelector('#paneDetail .actions .btn.primary').click(); true");
    await waitFor("app.run.status === 'SUCCESS'", 60000, '运行成功');
    check(await evaluate(`app.nodeById[${JSON.stringify(approvalId)}].output.comment === ${JSON.stringify(approvalComment)}`),
          '审批意见写入节点产物');
    check(await evaluate("document.querySelector('#wfState').textContent.indexOf('已完成') === 0"),
          '终态工作流标签显示已完成');
    await screenshot('04-approval-success');

    currentStage = '历史回看';
    await click('#btnHist');
    await waitFor("document.querySelectorAll('.hist-item').length > 0", 10000,
                  '历史列表加载');
    const histRequirement = await evaluate("document.querySelector('.hist-item .hist-req').textContent");
    check(histRequirement.indexOf('浏览器验收') >= 0, '历史列表显示本次需求原文');
    await click('.hist-item');
    await waitFor("app.frozen === true && document.querySelector('#histBanner').hidden === false",
                  15000, '进入历史只读视图');
    check(await evaluate("document.querySelector('#histBannerText').textContent.indexOf('RUN-') >= 0"),
          '历史提示条标明 Run ID');
    check(await evaluate(`app.def.nodes.some(n => n.id === ${JSON.stringify(newNodeId)})`),
          '历史回看保留编辑后的节点');
    await evaluate(`selectNode(${JSON.stringify(approvalId)}); true`);
    await waitFor("document.querySelector('#paneDetail').innerHTML.indexOf('人工操作结果') >= 0",
                  5000, '历史审批详情');
    const historyPane = await evaluate("document.querySelector('#paneDetail').innerHTML");
    check(historyPane.indexOf(approvalComment) >= 0, '历史详情保留审批意见');
    check(await evaluate("document.querySelector('#paneDetail .actions') === null"),
          '历史回看不提供审批操作');
    await screenshot('05-history');
    await click('#histBanner .btn');
    await waitFor("app.frozen === false && document.querySelector('#histBanner').hidden === true",
                  15000, '退出历史回看');
    check(true, '可返回当前实时状态');

    check(runtimeErrors.length === 0,
          '无未捕获浏览器运行时错误' +
          (runtimeErrors.length ? '：' + runtimeErrors.slice(0, 3).join('；') : ''));
    const passed = checks.filter(Boolean).length;
    const evidence = {
      result: passed === checks.length ? 'PASS' : 'FAIL',
      passed,
      total: checks.length,
      base: BASE,
      requirement,
      generated_nodes: generatedCount,
      inserted_node: newNodeId,
      approval_comment: approvalComment,
      runtime_errors: runtimeErrors,
      screenshots: fs.readdirSync(ARTIFACT_DIR).filter((x) => x.endsWith('.png')).sort(),
    };
    fs.writeFileSync(path.join(ARTIFACT_DIR, 'browser-evidence.json'),
                     JSON.stringify(evidence, null, 2));
    console.log(`\n结果: ${evidence.result} (${passed}/${checks.length})`);
    return passed === checks.length ? 0 : 1;
  } catch (error) {
    console.error('\n浏览器验收失败:', error.stack || error.message || error);
    console.error('阶段:', currentStage);
    try { await screenshot('failure-' + Date.now()); } catch (e) {}
    try {
      const state = await evaluate("({run: app && app.run, def: app && app.def, errors: app && app.errors})");
      fs.writeFileSync(path.join(ARTIFACT_DIR, 'browser-failure-state.json'),
                       JSON.stringify({stage: currentStage, error: String(error), state},
                                      null, 2));
    } catch (e) {}
    return 1;
  } finally {
    cdp.close();
    try { browser.proc.kill('SIGTERM'); } catch (e) {}
    await sleep(300);
    try { browser.proc.kill('SIGKILL'); } catch (e) {}
    fs.rmSync(browser.profile, { recursive: true, force: true });
  }
}

if (typeof WebSocket !== 'function') {
  console.error('需要 Node.js 22+（内置 WebSocket）');
  process.exit(2);
}
main().then((code) => process.exit(code)).catch((error) => {
  console.error(error.stack || error);
  process.exit(1);
});
