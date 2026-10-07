'use strict';

/*
 * 端到端测试:以子进程启动真实服务器,使用真实的控制/数据双 TCP 连接,
 * 通过测试钩子端口的"可控发送屏障"确定性地复现:
 *   - 慢读(数据连接堵住)期间 ABOR 在控制连接上生效
 *   - 零字节断点(REST == 文件大小)
 *   - 末尾完成竞争(最后一字节已冲刷 vs ABOR,只允许一种结论)
 *   - 旧监听器上迟到的数据连接不得被新传输使用
 * 客户端仅凭控制连接上的应答序列判定每次传输的结局。
 */

const { test, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const net = require('node:net');
const path = require('node:path');

const SERVER = path.join(__dirname, '..', 'ftpd.js');
const { FILES } = require(SERVER);

const ACCEPT_TIMEOUT_MS = 400;

// ---------------------------------------------------------------------------
// 行队列:按谓词取行,未匹配的行保留给后续取者
// ---------------------------------------------------------------------------
class LineQueue {
  constructor() {
    this.lines = [];
    this.waiters = [];
  }
  push(line) {
    for (let i = 0; i < this.waiters.length; i++) {
      if (this.waiters[i].pred(line)) {
        const w = this.waiters.splice(i, 1)[0];
        w.resolve(line);
        return;
      }
    }
    this.lines.push(line);
  }
  take(pred, timeout = 5000, what = 'line') {
    for (let i = 0; i < this.lines.length; i++) {
      if (pred(this.lines[i])) return Promise.resolve(this.lines.splice(i, 1)[0]);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waiters.findIndex((w) => w.resolve === resolve);
        if (idx >= 0) this.waiters.splice(idx, 1);
        reject(new Error(`timeout waiting for ${what}`));
      }, timeout);
      this.waiters.push({ pred, resolve: (l) => { clearTimeout(timer); resolve(l); } });
    });
  }
  drain() {
    return this.lines.splice(0);
  }
}

// ---------------------------------------------------------------------------
// FTP 控制连接客户端:只认 CRLF 行,按序读取应答
// ---------------------------------------------------------------------------
class FtpClient {
  constructor() {
    this.q = new LineQueue();
    this.closed = false;
  }
  connect(port) {
    return new Promise((resolve, reject) => {
      this.sock = net.connect(port, '127.0.0.1', resolve);
      this.sock.setNoDelay(true);
      this.sock.on('error', () => {});
      this.sock.on('close', () => { this.closed = true; });
      let buf = '';
      this.sock.on('data', (d) => {
        buf += d.toString('utf8');
        let i;
        while ((i = buf.indexOf('\r\n')) >= 0) {
          this.q.push(buf.slice(0, i));
          buf = buf.slice(i + 2);
        }
      });
    });
  }
  send(line) {
    this.sock.write(line + '\r\n');
  }
  expect(code, timeout) {
    return this.q.take(
      (l) => l.slice(0, 3) === String(code) && l[3] === ' ',
      timeout,
      `reply ${code}`,
    );
  }
  // 静默期断言:窗口内控制连接上不得再出现任何应答
  // (用于抓住"旧传输的完成回调替新传输发送成功"之类的串线)。
  async quiesce(ms = 250) {
    await new Promise((r) => setTimeout(r, ms));
    const extra = this.q.drain();
    assert.deepEqual(extra, [], `unexpected extra control replies: ${JSON.stringify(extra)}`);
  }
  close() {
    if (!this.sock.destroyed) this.sock.destroy();
  }
}

// ---------------------------------------------------------------------------
// 数据连接客户端
// ---------------------------------------------------------------------------
class DataConn {
  constructor() {
    this.chunks = [];
    this.bytes = 0;
    this.closed = false;
  }
  connect(port) {
    return new Promise((resolve, reject) => {
      this.sock = net.connect(port, '127.0.0.1', resolve);
      this.sock.on('data', (d) => {
        this.chunks.push(d);
        this.bytes += d.length;
      });
      this.sock.on('error', () => {});
      this.sock.on('close', () => { this.closed = true; });
    });
  }
  waitClosed(timeout = 5000) {
    if (this.closed) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('timeout waiting for data close')), timeout);
      this.sock.on('close', () => { clearTimeout(t); resolve(); });
    });
  }
  async waitBytes(n, timeout = 5000) {
    const t0 = Date.now();
    while (this.bytes < n) {
      if (Date.now() - t0 > timeout) throw new Error(`timeout waiting for ${n} data bytes (got ${this.bytes})`);
      await new Promise((r) => setTimeout(r, 10));
    }
  }
  async readAll() {
    await this.waitClosed();
    return Buffer.concat(this.chunks);
  }
  close() {
    if (!this.sock.destroyed) this.sock.destroy();
  }
}

// ---------------------------------------------------------------------------
// 测试钩子客户端
// ---------------------------------------------------------------------------
class Hook {
  constructor() {
    this.q = new LineQueue();
  }
  connect(port) {
    return new Promise((resolve, reject) => {
      this.sock = net.connect(port, '127.0.0.1', resolve);
      this.sock.on('error', () => {});
      let buf = '';
      this.sock.on('data', (d) => {
        buf += d.toString('utf8');
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
          this.q.push(buf.slice(0, i));
          buf = buf.slice(i + 1);
        }
      });
    });
  }
  async cmd(line) {
    this.sock.write(line + '\n');
    const reply = await this.q.take(
      (l) => l.startsWith('OK') || l.startsWith('ERR') || l === 'PONG',
      5000,
      `hook reply for ${line}`,
    );
    assert.ok(!reply.startsWith('ERR'), `hook command ${line} failed: ${reply}`);
    return reply;
  }
  waitFor(prefix, timeout = 5000) {
    return this.q.take((l) => l.startsWith(prefix), timeout, `hook event ${prefix}`);
  }
  drain() {
    this.q.drain();
  }
}

// ---------------------------------------------------------------------------
// 服务器子进程
// ---------------------------------------------------------------------------
let server;
let controlPort;
let hook;

function startServer() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [SERVER], {
      env: {
        ...process.env,
        FTPD_PORT: '0',
        FTPD_HOOK_PORT: '0',
        FTPD_ACCEPT_TIMEOUT_MS: String(ACCEPT_TIMEOUT_MS),
      },
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let buf = '';
    child.stdout.on('data', (d) => {
      buf += d.toString('utf8');
      const m = buf.match(/READY control=(\d+) hook=(\d+)/);
      if (m) resolve({ child, controlPort: +m[1], hookPort: +m[2] });
    });
    child.on('exit', (code) => reject(new Error(`server exited early: ${code}`)));
  });
}

before(async () => {
  server = await startServer();
  controlPort = server.controlPort;
  hook = new Hook();
  await hook.connect(server.hookPort);
});

after(() => {
  if (server) server.child.kill('SIGTERM');
});

beforeEach(async () => {
  await hook.cmd('BARRIER OFF');
  hook.drain();
});

// ---------------------------------------------------------------------------
// 公共动作
// ---------------------------------------------------------------------------
async function newClient() {
  const c = new FtpClient();
  await c.connect(controlPort);
  await c.expect(220);
  return c;
}

async function login(c) {
  c.send('USER demo');
  await c.expect(331);
  c.send('PASS demo123');
  await c.expect(230);
}

async function epsv(c) {
  c.send('EPSV');
  const line = await c.expect(229);
  const m = line.match(/\(\|\|\|(\d+)\|\)/);
  assert.ok(m, `bad 229 line: ${line}`);
  return +m[1];
}

// 完成一次下载,返回收到的字节;仅凭控制应答确认传输结局。
async function download(c, name) {
  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send(`RETR ${name}`);
  await c.expect(150);
  const data = await d.readAll();
  await c.expect(226);
  return data;
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------
test('问候、认证与账户校验', async () => {
  const c = await newClient();
  c.send('PASS whatever');
  await c.expect(503); // PASS 先于 USER
  c.send('USER demo');
  await c.expect(331);
  c.send('PASS wrong');
  await c.expect(530);
  c.send('USER demo');
  await c.expect(331);
  c.send('PASS demo123');
  await c.expect(230);
  c.send('NOOP');
  await c.expect(200);
  c.send('QUIT');
  await c.expect(221);
  c.close();
});

test('未登录时命令被拒绝', async () => {
  const c = await newClient();
  c.send('RETR alpha.bin');
  await c.expect(530);
  c.send('EPSV');
  await c.expect(530);
  c.send('TYPE I');
  await c.expect(530);
  c.send('REST 0');
  await c.expect(530);
  c.send('NOOP');
  await c.expect(200);
  c.send('ABOR');
  await c.expect(225); // 无活动传输
  await login(c); // 之后仍可正常登录
  c.close();
});

test('不支持的命令:上传/主动模式/目录操作', async () => {
  const c = await newClient();
  await login(c);
  for (const cmd of ['STOR x', 'APPE x', 'PORT 1,2,3,4,5,6', 'EPRT |1|2.3.4.5|6|',
    'PASV', 'LIST', 'CWD /', 'MKD x', 'DELE x', 'PWD']) {
    c.send(cmd);
    await c.expect(502);
  }
  c.send('TYPE A');
  await c.expect(504);
  c.send('TYPE I');
  await c.expect(200);
  c.send('FOOBAR');
  await c.expect(500);
  c.send('REST abc');
  await c.expect(501);
  c.send('REST -5');
  await c.expect(501);
  c.send('REST 7');
  await c.expect(350);
  c.close();
});

test('完整下载:字节与内置文件一致', async () => {
  const c = await newClient();
  await login(c);
  c.send('TYPE I');
  await c.expect(200);
  const alpha = await download(c, 'alpha.bin');
  assert.deepEqual(alpha, FILES.get('alpha.bin'));
  assert.equal(alpha.length, 32768);
  const beta = await download(c, 'beta.bin');
  assert.deepEqual(beta, FILES.get('beta.bin'));
  const gamma = await download(c, 'gamma.bin');
  assert.deepEqual(gamma, FILES.get('gamma.bin'));
  await c.quiesce();
  c.close();
});

test('REST 断点续传,且只被下一次被接受的 RETR 消费', async () => {
  const c = await newClient();
  await login(c);

  c.send('REST 100');
  await c.expect(350);
  let data = await download(c, 'beta.bin');
  assert.deepEqual(data, FILES.get('beta.bin').subarray(100));

  // REST 已被上一个 RETR 消费:不带 REST 的 RETR 从头开始
  data = await download(c, 'beta.bin');
  assert.deepEqual(data, FILES.get('beta.bin'));

  // REST 0 => 完整文件
  c.send('REST 0');
  await c.expect(350);
  data = await download(c, 'beta.bin');
  assert.deepEqual(data, FILES.get('beta.bin'));

  // 被拒绝的 RETR 不消费 REST:550 之后偏移仍然有效
  c.send('REST 100');
  await c.expect(350);
  c.send('RETR nope.bin');
  await c.expect(550);
  data = await download(c, 'beta.bin');
  assert.deepEqual(data, FILES.get('beta.bin').subarray(100));

  await c.quiesce();
  c.close();
});

test('零字节断点:REST 到文件末尾,传输立即成功', async () => {
  const c = await newClient();
  await login(c);
  c.send('REST 8192'); // == beta.bin 大小
  await c.expect(350);
  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR beta.bin');
  await c.expect(150);
  const data = await d.readAll(); // 服务器立刻结束数据侧
  await c.expect(226);
  assert.equal(data.length, 0);

  // REST 越过文件末尾 => 550,且该 RETR 被拒绝不消费 REST……
  c.send('REST 8193');
  await c.expect(350);
  await epsv(c);
  c.send('RETR beta.bin');
  await c.expect(550);
  // ……重新 REST 0 后恢复正常
  c.send('REST 0');
  await c.expect(350);
  const full = await download(c, 'beta.bin');
  assert.deepEqual(full, FILES.get('beta.bin'));
  await c.quiesce();
  c.close();
});

test('数据连接堵住时 ABOR 在控制连接上生效,会话可继续复用', async () => {
  const c = await newClient();
  await login(c);
  await hook.cmd('BARRIER 4096'); // 发送 4096 字节后堵住数据路径

  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR alpha.bin');
  await c.expect(150);
  const paused = await hook.waitFor('PAUSED');
  const written = +paused.split(' ')[2];
  assert.ok(written >= 4096, `expected >=4096 written, got ${written}`);
  await d.waitBytes(written); // 客户端确实收到了已冲刷的字节

  // 数据路径被屏障堵住期间,控制连接依然响应
  c.send('NOOP');
  await c.expect(200);

  // ABOR:先关数据侧,再按序 426、226
  c.send('ABOR');
  await c.expect(426);
  await c.expect(226);
  await d.waitClosed(); // 数据侧已被服务器关闭
  assert.equal(d.bytes, written); // 不多不少,正是屏障前冲刷的字节

  // 重复 ABOR => 225(已无活动传输)
  c.send('ABOR');
  await c.expect(225);

  await hook.cmd('RELEASE'); // 放行旧发送循环,它必须安静退出
  await c.quiesce(300); // 旧循环不得再产生任何控制应答

  // 会话复用:新的传输不受旧传输影响
  await hook.cmd('BARRIER OFF');
  const gamma = await download(c, 'gamma.bin');
  assert.deepEqual(gamma, FILES.get('gamma.bin'));
  await c.quiesce();
  c.close();
});

test('末尾完成竞争:末字节已冲刷时 ABOR 获胜,且只允许一种结论', async () => {
  const c = await newClient();
  await login(c);
  await hook.cmd('BARRIER 8192'); // 屏障设在 beta.bin 末尾

  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR beta.bin');
  await c.expect(150);
  await hook.waitFor('PAUSED'); // 全部 8192 字节已冲刷,结论尚未给出
  await d.waitBytes(8192);

  c.send('ABOR'); // 与"即将完成"竞争:ABOR 先到达 => 426、226
  await c.expect(426);
  await c.expect(226);
  await hook.cmd('RELEASE'); // 旧完成回调不得再补发 226
  await c.quiesce(300);

  // 反向情形:传输正常完成后 ABOR => 225
  await hook.cmd('BARRIER OFF');
  const g = await download(c, 'gamma.bin');
  assert.deepEqual(g, FILES.get('gamma.bin'));
  c.send('ABOR');
  await c.expect(225);

  // 旧传输的完成回调不能替下一次下载发送成功:
  // 新传输在控制上必须恰好看到 150 一次、226 一次。
  await hook.cmd('BARRIER 8192');
  const port2 = await epsv(c);
  const d2 = new DataConn();
  await d2.connect(port2);
  c.send('RETR beta.bin');
  await c.expect(150);
  await hook.waitFor('PAUSED');
  await hook.cmd('RELEASE'); // 这次让它成功
  const data = await d2.readAll();
  await c.expect(226);
  assert.deepEqual(data, FILES.get('beta.bin'));
  await c.quiesce(300); // 绝不允许出现第二个 226
  await hook.cmd('BARRIER OFF');
  c.close();
});

test('活动传输期间除 ABOR/NOOP/QUIT 外的命令被明确拒绝', async () => {
  const c = await newClient();
  await login(c);
  await hook.cmd('BARRIER 1000');

  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR alpha.bin');
  await c.expect(150);
  await hook.waitFor('PAUSED');

  for (const cmd of ['LIST', 'USER demo', 'PASS demo123', 'EPSV', 'REST 5',
    'RETR beta.bin', 'TYPE I', 'STOR x']) {
    c.send(cmd);
    await c.expect(450);
  }
  c.send('NOOP');
  await c.expect(200);

  c.send('ABOR');
  await c.expect(426);
  await c.expect(226);
  await hook.cmd('RELEASE');
  await hook.cmd('BARRIER OFF');
  await c.quiesce(300);
  d.close();
  c.close();
});

test('数据连接超时返回 425,会话保持可用', async () => {
  const c = await newClient();
  await login(c);

  // 没有 EPSV 直接 RETR => 425
  c.send('RETR beta.bin');
  await c.expect(425);

  // EPSV 后不接数据连接 => 超时 425
  await epsv(c);
  c.send('RETR beta.bin');
  await c.expect(425, 5000);

  // 会话仍然可用
  const beta = await download(c, 'beta.bin');
  assert.deepEqual(beta, FILES.get('beta.bin'));
  await c.quiesce(ACCEPT_TIMEOUT_MS + 400); // 旧的超时不应再触发任何东西
  c.close();
});

test('等待数据连接期间 ABOR 取消传输(426、226),定时器不再误报 425', async () => {
  const c = await newClient();
  await login(c);
  await epsv(c);
  c.send('RETR beta.bin'); // 没有数据连接到来
  c.send('ABOR');
  await c.expect(426);
  await c.expect(226);
  await c.quiesce(ACCEPT_TIMEOUT_MS + 400); // 超时定时器已被取消
  const beta = await download(c, 'beta.bin');
  assert.deepEqual(beta, FILES.get('beta.bin'));
  c.close();
});

test('旧监听器上迟到的数据连接不得被新传输使用', async () => {
  const c = await newClient();
  await login(c);

  const p1 = await epsv(c);
  const d1 = new DataConn();
  await d1.connect(p1);
  await hook.waitFor('PENDING'); // 服务器已接受 d1 为 pending

  const p2 = await epsv(c); // 替换监听器:旧监听器关闭,d1 被销毁
  await d1.waitClosed();
  assert.equal(d1.bytes, 0); // 旧连接上一字节都不能有

  // 新传输只能走新监听器上的连接
  const d2 = new DataConn();
  await d2.connect(p2);
  c.send('RETR beta.bin');
  await c.expect(150);
  const data = await d2.readAll();
  await c.expect(226);
  assert.deepEqual(data, FILES.get('beta.bin'));
  assert.equal(d1.bytes, 0);
  await c.quiesce();
  c.close();
});

test('数据连接中途失败返回 426,之后 ABOR 给 225', async () => {
  const c = await newClient();
  await login(c);
  await hook.cmd('BARRIER 4096');

  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR alpha.bin');
  await c.expect(150);
  await hook.waitFor('PAUSED');

  d.close(); // 客户端断开数据连接 => 传输失败
  await c.expect(426); // 仅 426,没有 226
  c.send('ABOR');
  await c.expect(225); // 传输已有结论,ABOR 不再产生 426/226
  await hook.cmd('RELEASE');
  await hook.cmd('BARRIER OFF');
  await c.quiesce(300);
  c.close();
});

test('QUIT 中止活动传输并按序给出 426、226、221', async () => {
  const c = await newClient();
  await login(c);
  await hook.cmd('BARRIER 1000');

  const port = await epsv(c);
  const d = new DataConn();
  await d.connect(port);
  c.send('RETR alpha.bin');
  await c.expect(150);
  await hook.waitFor('PAUSED');

  c.send('QUIT');
  await c.expect(426);
  await c.expect(226);
  await c.expect(221);
  await hook.cmd('RELEASE');
  await hook.cmd('BARRIER OFF');
  d.close();
  c.close();
});

test('控制命令按 CRLF 增量解析(分片到达)', async () => {
  const c = await newClient();
  // 把 USER 命令拆成多个 TCP 分片发送
  c.sock.write('US');
  await new Promise((r) => setTimeout(r, 50));
  c.sock.write('ER de');
  await new Promise((r) => setTimeout(r, 50));
  c.sock.write('mo\r');
  await new Promise((r) => setTimeout(r, 50));
  c.sock.write('\nPASS demo123\r\n');
  await c.expect(331);
  await c.expect(230);
  const gamma = await download(c, 'gamma.bin');
  assert.deepEqual(gamma, FILES.get('gamma.bin'));
  c.close();
});
