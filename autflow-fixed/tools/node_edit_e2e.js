#!/usr/bin/env node
/* F 任务 · 真实浏览器验收：规格面板布局 + 画布节点的"看内容 / 改单个节点"。

覆盖：
  1. 全局约束输入框足够高（≥120px）且能随内容长高，行数计数正确；
  2. 画布上点任意节点，右栏显示它的具体内容（任务阶段、做什么、怎么做、落点文件、子步骤、验收）；
  3. 视图态就能「编辑此节点」：改名 → 保存草稿 → 服务端草稿里真的变了，画布也跟着变；
  4. 跑完一轮之后，节点详情里的「产出内容」有可读摘要（不是只有一坨 JSON），原始 JSON 折叠可查。

用法：node tools/node_edit_e2e.js [base_url] [artifact_dir]
*/
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const ROOT = path.join(__dirname, '..');
const BASE = process.argv[2] || 'http://127.0.0.1:8010';
const ARTIFACT_DIR = path.resolve(
  process.argv[3] || path.join(ROOT, 'acceptance-results', 'node-edit-e2e'));

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
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'autflow_nodeedit_'));
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

const MD = ['# 工单系统 v1', '', '## 模块', '',
  '### 登录', '验收：错误提示明确', '### 工单流转',
  '- [ ] 状态变更写审计日志', '验收：越权指派被拒绝', '',
  '## 全局约束', '- 不新增第三方依赖', '- 密码只存 hash'].join('\n');

async function main() {
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });
  console.log('节点内容 / 单节点编辑 浏览器验收:', BASE);
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

    console.log('\n[1] 全局约束输入框够高、能长高');
    await evaluate("openSpec()");
    await waitFor("!document.querySelector('#specPanel').hidden", 10000, '规格面板打开');
    const box0 = await evaluate(`(() => {
      const ta = document.querySelector('#specConstraints');
      const cs = getComputedStyle(ta);
      return { h: ta.clientHeight, rows: ta.rows, resize: cs.resize, minH: cs.minHeight };
    })()`);
    check(box0.rows >= 5, '默认 rows = ' + box0.rows + '（原来只有 2）');
    check(box0.h >= 110, '可见高度 ' + box0.h + 'px（≥110px）');
    check(box0.resize === 'vertical', '可以手动拉高（resize: vertical）');
    check((await text('#specConstraintCount')).indexOf('条') >= 0,
      '显示约束条数：' + (await text('#specConstraintCount')).trim());

    console.log('\n[2] 导入规格后约束变多，框继续长高');
    await evaluate("toggleSpecPaste()");
    await evaluate(`document.querySelector('#specPaste').value = ${JSON.stringify(MD)}`);
    await evaluate("parseSpecText()");
    await waitFor("app.specModules.length >= 2", 20000, '解析出模块');
    check((await text('#specConstraintCount')).indexOf('2') >= 0,
      '导入后约束计数 = ' + (await text('#specConstraintCount')).trim());
    const longText = Array.from({ length: 9 },
      (_, i) => '约束 ' + (i + 1) + '：接口必须在 200ms 内返回').join('\n');
    await evaluate(`(() => { const ta = document.querySelector('#specConstraints');
      ta.value = ${JSON.stringify(longText)}; syncConstraintCount(); })()`);
    const box1 = await evaluate("document.querySelector('#specConstraints').clientHeight");
    check(box1 > box0.h, '内容变多后框高从 ' + box0.h + 'px 长到 ' + box1 + 'px');
    check(box1 <= 330, '但不会无限长（上限 320px，实际 ' + box1 + 'px）');
    await evaluate(`(() => { const ta = document.querySelector('#specConstraints');
      ta.value = ${JSON.stringify(MD.split('## 全局约束')[1].replace(/^\\n/, ''))};
      syncConstraintCount(); })()`);
    await evaluate("document.querySelector('#specName').value = '工单系统 v1'");
    await evaluate("saveSpec()");
    await waitFor("!!app.specId", 20000, '规格保存');
    await evaluate("planSpec()");
    await waitFor("(app.def.nodes || []).some(x => x.task_id)", 30000, '按规格编排出任务节点');
    await waitFor("document.querySelector('#specPanel').hidden", 10000, '编排后规格面板收起');
    check((await evaluate("(app.def.nodes || []).filter(x => x.task_id).length")) >= 5,
      '编排后有 ' + (await evaluate("(app.def.nodes || []).filter(x => x.task_id).length")) + ' 个任务节点');
    await shot('01-constraints');

    console.log('\n[3] 点节点：显示它自己的具体内容');
    const pick = await evaluate(`(() => {
      const n = (app.def.nodes || []).find(x => x.task_id && x.role !== 'pm');
      return n ? { id: n.id, task: n.task_id, name: n.name } : null;
    })()`);
    check(!!pick, '草稿里有带任务的节点（' + (pick && pick.id) + ' · ' + (pick && pick.name) + '）');
    await evaluate(`(() => {
      const g = app.nodeEls[${JSON.stringify(pick.id)}];
      g.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    })()`);
    await waitFor("!!document.querySelector('#paneDetail .detail-head')", 10000, '右栏显示节点详情');
    const detail = await text('#paneDetail');
    check(detail.indexOf('任务信息') >= 0, '显示任务信息（阶段 / 优先级 / 规格出处）');
    check(detail.indexOf('做什么') >= 0 && detail.indexOf('怎么做') >= 0,
      '显示「做什么」与「怎么做」');
    check(detail.indexOf('落点文件') >= 0, '显示落点文件');
    check(detail.indexOf('子步骤') >= 0, '显示子步骤');
    check(detail.indexOf('验收条款') >= 0, '显示验收条款');
    check(await evaluate("!!document.querySelector('#paneDetail .dh-top .btn')"),
      '详情头部有「编辑此节点」按钮');
    // 没运行过时不能只留空白：要说清"为什么空"，并给出预计产出
    check((await text('#paneDetail')).indexOf('还没运行') >= 0,
      '未运行节点写明"还没运行"：' + ((await text('#paneDetail .pending-box')) || '').slice(0, 24));
    check((await text('#paneDetail')).indexOf('预计产物') >= 0, '未运行时给出预计产物协议');
    check((await evaluate("document.querySelectorAll('#paneDetail .out-kv > div').length")) >= 2,
      '未运行时列出落点文件 / 要满足的验收');
    const previewButton = await evaluate(`(() => {
      const btn = document.querySelector('#paneDetail .step-preview-btn:not(:disabled)');
      if (!btn) return false;
      window.__previewOriginalFetch = window.fetch;
      window.fetch = async (input, init) => {
        if (String(input).includes('/api/tasks/preview-code')) {
          return new Response(JSON.stringify({
            ok: true, path: 'src/preview.py', code: 'print("preview")',
            source: 'generated', saved: false, executed: false
          }), { status: 200, headers: { 'Content-Type': 'application/json' } });
        }
        return window.__previewOriginalFetch(input, init);
      };
      btn.click();
      return true;
    })()`);
    check(previewButton, '任务子步骤有可用的「查看代码」按钮');
    await waitFor("document.querySelector('#stepCodeDialog').open"
      + " && document.querySelector('#stepCodeBody').textContent === 'print(\"preview\")'",
    10000, '代码预览弹窗显示返回内容');
    check((await text('#stepCodePath')) === 'src/preview.py', '弹窗显示目标文件路径');
    check((await text('#stepCodeStatus')).indexOf('没有落盘') >= 0
      && (await text('#stepCodeStatus')).indexOf('未执行') >= 0,
    '弹窗明确标注未保存、未执行');
    await evaluate(`(() => {
      document.querySelector('#stepCodeDialog').close();
      window.fetch = window.__previewOriginalFetch;
      delete window.__previewOriginalFetch;
    })()`);
    await shot('02-node-content');

    console.log('\n[4] 视图态直接改这一个节点');
    await evaluate("document.querySelector('#paneDetail .dh-top .btn').click()");
    await waitFor("!!document.querySelector('#paneDetail .insp-row input')", 10000, '进入单节点编辑');
    check((await text('#paneDetail')).indexOf('只改这一个节点') >= 0, '明确提示只改这一个节点');
    check(await evaluate("document.querySelectorAll('#paneDetail textarea').length >= 3"),
      '任务字段（做什么/怎么做/子步骤/验收）都能直接改');
    const oldName = await evaluate(`app.def.nodes.find(n => n.id === ${JSON.stringify(pick.id)}).name`);
    const newName = '改名验证 · ' + Date.now().toString().slice(-4);
    await evaluate(`(() => {
      const inp = document.querySelector('#paneDetail .insp-row input');
      inp.value = ${JSON.stringify(newName)};
      inp.dispatchEvent(new Event('input', { bubbles: true }));
    })()`);
    await evaluate("document.querySelector('#paneDetail .actions .btn.primary').click()");
    await waitFor("app.dirty === false", 20000, '草稿保存完成');
    const saved = await evaluate(`fetch('/api/editor').then(r => r.json()).then(d => {
      const n = d.draft.definition.nodes.find(x => x.id === ${JSON.stringify(pick.id)});
      return n ? n.name : null;
    })`);
    check(saved === newName, '服务端草稿里的节点名已变：' + saved);
    check((await evaluate(`app.def.nodes.find(n => n.id === ${JSON.stringify(pick.id)}).name`)) === newName,
      '画布上的节点名也变了（' + oldName + ' → ' + newName + '）');
    await evaluate("document.querySelector('#paneDetail .actions .btn:not(.primary):not(.danger)').click()");
    await waitFor("!document.querySelector('#paneDetail .insp-row')", 10000, '退出编辑回到详情');
    check((await text('#paneDetail .dh-name')).indexOf('改名验证') >= 0,
      '回到详情仍显示新名字');
    await shot('03-node-edited');

    console.log('\n[4b] 非任务节点（结束/条件/审批）也能单独改');
    const plain = await evaluate(`(() => {
      const n = (app.def.nodes || []).find(x => !x.task_id && x.type !== 'start');
      return n ? { id: n.id, type: n.type, name: n.name } : null;
    })()`);
    check(!!plain, '有非任务节点可测（' + (plain && plain.type) + ' · ' + (plain && plain.name) + '）');
    await evaluate(`(() => {
      const g = app.nodeEls[${JSON.stringify(plain.id)}];
      g.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    })()`);
    await waitFor("!!document.querySelector('#paneDetail .dh-top .btn')", 10000, '非任务节点也有编辑入口');
    await evaluate("document.querySelector('#paneDetail .dh-top .btn').click()");
    await waitFor("!!document.querySelector('#paneDetail .insp-row input')", 10000, '进入编辑');
    check(await evaluate("document.querySelectorAll('#paneDetail .insp-row').length >= 3"),
      '能改名称 / 类型 / 角色 / 重试上限');
    await evaluate("document.querySelector('#paneDetail .actions .btn:not(.primary):not(.danger)').click()");
    await waitFor("!document.querySelector('#paneDetail .insp-row')", 10000, '退出编辑');

    console.log('\n[5] 跑一轮：产出内容可读');
    await evaluate("fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'},"
      + " body: JSON.stringify({speed: 6, demo: false})})");
    await evaluate("fetch('/api/run/reset', {method:'POST', headers:{'Content-Type':'application/json'}, body: '{}'})");
    await sleep(600);
    await evaluate("document.querySelector('#btnStart').click()");
    // 审批节点必须人工批准，测试里自动批准，直到运行结束
    const deadline = Date.now() + 150000;
    let status = '';
    while (Date.now() < deadline) {
      status = await evaluate("app.run.status");
      if (status === 'SUCCESS' || status === 'FAILED' || status === 'CANCELLED') break;
      if (status === 'WAITING_HUMAN') {
        const waiting = await evaluate(`(app.nodes || [])
          .filter(n => n.status === 'WAITING_HUMAN' && n.type === 'approval')
          .map(n => n.id)`);
        for (const nid of waiting) {
          await evaluate(`fetch('/api/nodes/${nid}/approval', {method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({action:'approve', comment:'验收自动批准'})})`);
        }
      }
      await sleep(500);
    }
    check(status === 'SUCCESS', '运行以 SUCCESS 结束（实际 ' + status + '）');
    const done = await evaluate(`(() => {
      const n = (app.nodes || []).find(x => x.output && (x.output.files || x.output.summary));
      return n ? { id: n.id, name: n.name } : null;
    })()`);
    check(!!done, '有节点产出了内容（' + (done && done.name) + '）');
    await evaluate(`(() => {
      const g = app.nodeEls[${JSON.stringify(done.id)}];
      g.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    })()`);
    await waitFor("!!document.querySelector('#paneDetail .out-kv, #paneDetail .raw-json')", 10000,
      '详情出现产出内容');
    const outText = await text('#paneDetail');
    check(outText.indexOf('产出内容') >= 0, '有「产出内容」小节');
    check(await evaluate("document.querySelectorAll('#paneDetail .out-kv > div').length >= 1"),
      '产出按字段摊开（不是一坨 JSON）：'
      + (await evaluate("document.querySelectorAll('#paneDetail .out-kv > div').length")) + ' 项');
    check(await evaluate("!!document.querySelector('#paneDetail details.raw-json')"),
      '原始 JSON 折叠保留，需要时能展开');
    await shot('04-node-output');

    console.log('\n[6] 无脚本错误');
    check(runtimeErrors.length === 0,
      '控制台无未捕获错误' + (runtimeErrors.length ? '：' + runtimeErrors.slice(0, 2).join(' | ') : ''));
  } catch (err) {
    failed = true;
    check(false, '执行异常：' + (err && err.message ? err.message : err));
    try { await shot('99-failure'); } catch (e) {}
  } finally {
    cdp.close();
    try { browser.proc.kill(); } catch (e) {}
  }

  const ok = checks.filter(Boolean).length;
  const total = checks.length;
  console.log('\n结果: ' + (!failed && ok === total ? 'PASS' : 'FAIL') + ' (' + ok + '/' + total + ')');
  process.exit(!failed && ok === total ? 0 : 1);
}

main();
