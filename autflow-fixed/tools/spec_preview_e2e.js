#!/usr/bin/env node
/* C 任务 · 真实浏览器验收：设计规格面板的三处交互。

覆盖：
  1. 「分解预览」把规格生成的流程图**直接画在画布上覆盖原来的图**（不是只列个任务清单）；
  2. 预览期间按钮变成「取消分解预览」，顶栏标注「预览 · 未保存」，画布只读；
  3. 再点一次（等于直接退出）画布**回到之前那张图**，任务清单收起；
  4. 「新建」规格有「取消新建」出口，不会卡在空白表单里。

依赖：Node.js 22+（内置 fetch/WebSocket）、Chrome 或 Chromium（可用 CHROME_PATH 指定，Edge 亦可）。
用法：node tools/spec_preview_e2e.js [base_url] [artifact_dir]
*/
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const ROOT = path.join(__dirname, '..');
const BASE = process.argv[2] || 'http://127.0.0.1:8010';
const ARTIFACT_DIR = path.resolve(
  process.argv[3] || path.join(ROOT, 'acceptance-results', 'spec-preview-e2e'));

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
    '/usr/bin/google-chrome',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
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
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'autflow_spec_'));
  const args = [
    '--headless=new', '--disable-gpu', '--disable-dev-shm-usage',
    '--no-first-run', '--no-default-browser-check', '--no-proxy-server',
    '--remote-debugging-port=0', '--remote-allow-origins=*',
    '--window-size=1440,960', '--user-data-dir=' + profile, 'about:blank',
  ];
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
    if (proc.exitCode != null) throw new Error('浏览器提前退出：' + stderr.join('').slice(-2000));
    try {
      const lines = fs.readFileSync(portFile, 'utf8').trim().split(/\r?\n/);
      if (lines[0]) { port = Number(lines[0]); break; }
    } catch (e) {}
    await sleep(100);
  }
  if (!port) throw new Error('浏览器未打开调试端口：' + stderr.join('').slice(-2000));
  const list = await fetch(`http://127.0.0.1:${port}/json/list`).then((r) => r.json());
  const page = list.find((item) => item.type === 'page');
  if (!page) throw new Error('浏览器未提供 page target');
  return { proc, profile, cdp: new CDP(page.webSocketDebuggerUrl), stderr };
}

async function main() {
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });
  console.log('设计规格面板验收:', BASE);
  const browser = await openBrowser();
  const cdp = browser.cdp;

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
      try { if (await evaluate(expression)) return true; } catch (e) { lastError = e.message; }
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
  const text = (selector) => evaluate(
    `(document.querySelector(${JSON.stringify(selector)}) || {}).textContent || ''`);
  const shot = async (name) => {
    const png = await cdp.call('Page.captureScreenshot', {
      format: 'png', fromSurface: true, captureBeyondViewport: false,
    });
    const file = path.join(ARTIFACT_DIR, name + '.png');
    fs.writeFileSync(file, Buffer.from(png.data, 'base64'));
    return file;
  };

  let failed = false;
  try {
    await cdp.call('Page.enable');
    await cdp.call('Runtime.enable');
    await cdp.call('Log.enable');
    await cdp.call('Emulation.setDeviceMetricsOverride', {
      width: 1440, height: 960, deviceScaleFactor: 1, mobile: false,
    });
    cdp.on('Runtime.exceptionThrown', (params) => {
      const d = params.exceptionDetails || {};
      runtimeErrors.push(d.exception ? (d.exception.description || d.text) : (d.text || '异常'));
    });
    cdp.on('Log.entryAdded', (params) => {
      const entry = params.entry || {};
      if (entry.url && /favicon\.ico(?:$|\?)/.test(entry.url)) return;
      if (entry.level === 'error') runtimeErrors.push(entry.text || 'log error');
    });

    await cdp.call('Page.navigate', { url: BASE + '/' });
    await waitFor("document.readyState === 'complete' && typeof app !== 'undefined'"
      + ' && !!app.editor && app.built === true', 30000, '编辑器与画布加载完成');
    check(true, '真实浏览器加载编辑器与画布');
    await shot('00-boot');

    console.log('\n[1] 打开设计规格面板');
    await click('#btnSpec');
    await waitFor("!document.querySelector('#specPanel').hidden"
      + ' && app.specs.length > 0', 20000, '规格面板与规格列表就绪');
    const baseName = await evaluate('app.def.name');
    const baseNodes = await evaluate('app.def.nodes.length');
    const baseDomNodes = await evaluate(
      "document.querySelectorAll('#nodeLayer .node').length");
    check(await evaluate("document.querySelector('#btnSpecDecompose').textContent.trim()")
      === '分解预览', '入口按钮是「分解预览」');
    check(baseDomNodes === baseNodes, '画布上 ' + baseDomNodes + ' 个节点与定义一致');
    await shot('01-spec-panel');

    console.log('\n[2] 分解预览：流程图直接覆盖画布');
    await click('#btnSpecDecompose');
    await waitFor('!!app.preview', 25000, '进入预览');
    const prevName = await evaluate('app.preview.definition.name');
    const prevNodes = await evaluate('app.preview.definition.nodes.length');
    const domNodes = await evaluate("document.querySelectorAll('#nodeLayer .node').length");
    check(prevName !== baseName, '预演定义与原草稿不同（' + prevName + '）');
    check(await evaluate('app.def.name') === prevName, '画布定义已切换为预演出来的图');
    check(domNodes === prevNodes, '画布重绘为 ' + domNodes + ' 个预览节点（覆盖原图）');
    check(await evaluate("document.querySelector('#btnSpecDecompose').textContent.trim()")
      === '取消分解预览', '按钮变为「取消分解预览」');
    check((await text('#wfState')).indexOf('预览') >= 0, '工作流栏标注「预览 · 未保存」');
    check((await text('#wfName')).indexOf('预览') === 0, '工作流名带预览前缀，不再显示上一次的编排');
    check((await text('#reqLabel')).indexOf('预览中') >= 0, '来源标签标注预览中');
    check(await evaluate("!document.querySelector('#specDecomp').hidden"), '任务清单同时可见');
    check(await evaluate("app.isEditable ? false : app.mode === 'view'"), '预览期间画布只读');
    await shot('02-spec-preview');

    console.log('\n[2b] 面板里可以选流程模板，换模板立即重画预览');
    const tplOptions = await evaluate(
      "Array.from(document.querySelectorAll('#selSpecTemplate option')).map(o => o.value)");
    check(tplOptions.indexOf('default') >= 0 && tplOptions.indexOf('enterprise') >= 0
      && tplOptions.indexOf('lean') >= 0,
      '模板下拉含标准 SE / 敏捷精简 / 企业合规（' + tplOptions.join(',') + '）');
    check((await text('#specTplDesc')).length > 0, '模板说明摊在面板里，不用猜差别');
    const setSelect = (selector, value) => evaluate(`(() => {
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) throw new Error('缺少元素 ${selector}');
      el.value = ${JSON.stringify(value)};
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return el.value;
    })()`);
    const beforeTpl = await evaluate('app.preview.template');
    await setSelect('#selSpecTemplate', 'enterprise');
    await waitFor("!!app.preview && app.preview.template === 'enterprise'", 20000,
      '按企业合规模板重画预览');
    const entNodes = await evaluate('app.preview.definition.nodes.length');
    check(beforeTpl !== 'enterprise', '切换前是标准 SE 模板（' + beforeTpl + '）');
    check((await text('#specTplDesc')).indexOf('企业合规') >= 0, '说明文字跟着模板变化');
    check(entNodes > prevNodes, '企业合规流程节点更多（' + prevNodes + ' → ' + entNodes + '）');
    check(await evaluate("document.querySelectorAll('#nodeLayer .node').length") === entNodes,
      '换模板后画布按新结构重画');
    check(await evaluate("document.querySelector('#btnSpecDecompose').textContent.trim()")
      === '取消分解预览', '换模板后仍处于预览态');
    await shot('02b-template-enterprise');

    console.log('\n[3] 取消分解预览：画布回到之前那张图');
    await click('#btnSpecDecompose');
    await waitFor('!app.preview', 25000, '退出预览');
    await waitFor(`document.querySelectorAll('#nodeLayer .node').length === ${baseNodes}`,
      15000, '画布节点恢复');
    check(await evaluate('app.def.name') === baseName, '画布定义恢复为预览前的草稿');
    check(await evaluate("document.querySelector('#btnSpecDecompose').textContent.trim()")
      === '分解预览', '按钮恢复为「分解预览」');
    check(await evaluate("document.querySelector('#specDecomp').hidden"), '任务清单已收起');
    check((await text('#wfState')).indexOf('预览') < 0, '工作流栏不再显示预览态');
    await shot('03-preview-cancelled');

    console.log('\n[4] 新建规格可以取消');
    const specBefore = await evaluate('app.specId');
    await click('#btnSpecNew');
    await waitFor("!document.querySelector('#btnSpecBack').hidden", 10000, '出现取消新建');
    check(await evaluate('app.specId === null'), '新建后不绑定已保存规格');
    check(await evaluate("document.querySelector('#specSel').value === ''"),
      '下拉框显示「新建规格（未保存）」占位');
    check((await evaluate("document.querySelector('#specName').value")) === '',
      '表单已清空');
    await shot('04-spec-new');
    await click('#btnSpecBack');
    await waitFor('!!app.specId', 20000, '回到已保存规格');
    check(await evaluate("document.querySelector('#btnSpecBack').hidden"),
      '取消新建后按钮收起');
    check(await evaluate('app.specId') === specBefore, '回到原来那份规格');
    check((await text('#specMeta')).indexOf('新规格') < 0, '状态行恢复为规格版本信息');
    await shot('05-spec-new-cancelled');

    check(runtimeErrors.length === 0,
      '页面无脚本异常' + (runtimeErrors.length ? '：' + runtimeErrors.slice(0, 3).join(' | ') : ''));
  } catch (err) {
    failed = true;
    console.error('运行时异常:', err && err.stack ? err.stack : err);
    try { await shot('99-failure'); } catch (e) {}
  } finally {
    cdp.close();
    try { browser.proc.kill(); } catch (e) {}
    try { fs.rmSync(browser.profile, { recursive: true, force: true }); } catch (e) {}
  }

  const pass = checks.filter(Boolean).length;
  console.log('\n结果:', !failed && pass === checks.length ? 'PASS' : 'FAIL',
              '(' + pass + '/' + checks.length + ')');
  console.log('截图目录:', ARTIFACT_DIR);
  process.exit(!failed && pass === checks.length ? 0 : 1);
}

main().catch((err) => {
  console.error('运行时异常:', err && err.stack ? err.stack : err);
  process.exit(1);
});