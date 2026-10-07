#!/usr/bin/env node
'use strict';

/*
 * readonly-ftpd — 单进程只读 FTP 演示服务（零依赖）。
 *
 * 仅支持: USER, PASS, TYPE I, EPSV, REST, RETR, ABOR, NOOP, QUIT
 * 不支持: 上传(STOR/APPE/...)、主动模式(PORT/EPRT/PASV)、目录操作(LIST/CWD/MKD/...) → 502
 *
 * 关键语义:
 *  - 控制连接按 CRLF 增量读取;数据发送与控制读取在事件循环上独立推进,
 *    数据连接被堵住(慢读/不回 ACK)时 ABOR 在控制连接上依然生效。
 *  - 每次 EPSV 替换旧监听器(旧监听器关闭、其上滞留的 pending 数据连接被销毁,
 *    迟到的旧监听器连接一律拒绝)。
 *  - 每次 RETR 独占:当前监听器、一个数据 socket、自己的文件游标(REST 偏移)与终态。
 *  - REST 只被下一次"被接受的"RETR 消费(被拒绝的 RETR 不消费)。
 *  - 活动传输期间除 ABOR/NOOP/QUIT 外的命令一律 450 拒绝。
 *  - 数据连接超时 → 425;传输失败 → 426;活动中止 → 先关数据侧再按序 426、226;
 *    已完成或无活动传输时 ABOR → 225。
 *  - 成功与中止竞争只允许一种结论:settle() 一次性生效,旧的 accept/drain/结束
 *    回调(旧 transfer 对象上的)不会为新传输发出任何应答。
 *
 * 测试钩子(仅当设置 FTPD_HOOK_PORT 时开启,行协议):
 *   PING            -> PONG
 *   BARRIER <n|OFF> -> OK ...     下一次传输在已发送 n 字节后暂停(可控发送屏障)
 *   RELEASE         -> OK ...     放行被屏障暂停的发送循环
 * 服务器主动通告: PENDING / ATTACHED <id> / PAUSED <id> <written> / SETTLED <id> <outcome>
 */

const net = require('node:net');

const CONTROL_PORT = parseInt(process.env.FTPD_PORT ?? '2121', 10);
const HOOK_PORT = process.env.FTPD_HOOK_PORT; // 未设置 => 不启用测试钩子
const ACCEPT_TIMEOUT_MS = parseInt(process.env.FTPD_ACCEPT_TIMEOUT_MS ?? '10000', 10);
const PASV_MIN = parseInt(process.env.PASV_PORT_MIN ?? '0', 10);
const PASV_MAX = parseInt(process.env.PASV_PORT_MAX ?? '0', 10);
const CHUNK_SIZE = 8192;
const MAX_LINE = 8192;

const DEMO_USER = 'demo';
const DEMO_PASS = 'demo123';

// 三份内置二进制演示文件,每份最多 32 KiB,内容确定性生成。
function makeFile(seed, size) {
  const buf = Buffer.alloc(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed * 17 + i * 31 + (i >> 3)) & 0xff;
  }
  return buf;
}

const FILES = new Map([
  ['alpha.bin', makeFile(1, 32768)], // 32 KiB,用于慢读/中止场景
  ['beta.bin', makeFile(2, 8192)],   // 8 KiB,用于断点/末尾竞争场景
  ['gamma.bin', makeFile(3, 64)],    // 64 B,小文件
]);

// ---------------------------------------------------------------------------
// 测试钩子:可控发送屏障(全局,测试串行使用)
// ---------------------------------------------------------------------------
const hookClients = new Set();
function announce(line) {
  for (const h of hookClients) {
    try { h.write(line + '\n'); } catch { /* 钩子断开不影响服务 */ }
  }
}

const barrier = { armed: false, at: 0 };
const pausedTransfers = new Set();

function releasePaused() {
  for (const t of [...pausedTransfers]) {
    if (t.wake) t.wake();
  }
}

// ---------------------------------------------------------------------------
// 传输终态:一次性结论
// ---------------------------------------------------------------------------
let nextTransferId = 1;

// 每个 transfer 的唯一结论入口。无论末字节冲刷完成、ABOR、数据 socket
// 错误/关闭、accept 超时还是会话拆除,都经由此处;settled 之后再次进入
// 直接返回 —— 旧 transfer 上迟到的回调永远不会产生第二次结论,更不会
// 落到新传输上(它们持有的 t 与 session.transfer 已不相等)。
function settle(t, outcome) {
  if (t.settled) return;
  t.settled = true;
  if (t.timer) { clearTimeout(t.timer); t.timer = null; }
  const s = t.session;
  if (s.transfer === t) s.transfer = null;

  // 先关闭数据侧,再按序发送控制应答。
  const sock = t.socket;
  t.socket = null;
  if (sock && !sock.destroyed) {
    if (outcome === 'ok') sock.end();      // 成功:优雅 FIN,保证已发字节可送达
    else sock.destroy();                    // 中止/失败/超时:直接断开
  }

  // 唤醒可能停在屏障或 write 回调里的发送循环,让它看到 settled 后自行退出。
  if (t.cancelIO) { const c = t.cancelIO; t.cancelIO = null; c(); }
  if (t.wake) { const w = t.wake; t.wake = null; w(); }

  announce(`SETTLED ${t.id} ${outcome}`);
  switch (outcome) {
    case 'ok':
      s.reply(226, 'Transfer complete.');
      break;
    case 'abort':
      s.reply(426, 'Transfer aborted.');
      s.reply(226, 'Abort successful.');
      break;
    case 'fail':
      s.reply(426, 'Data connection failed; transfer aborted.');
      break;
    case 'timeout':
      s.reply(425, 'No data connection established.');
      break;
    case 'silent':
      break;
  }
}

// ---------------------------------------------------------------------------
// 发送循环(数据侧,独立于控制读取推进)
// ---------------------------------------------------------------------------

// 屏障:armed 且本次传输尚未暂停过、且已发送字节数达到 at 时,暂停发送循环,
// 直到测试端 RELEASE 或传输被 settle(ABOR/失败/超时)。返回 true 表示
// 暂停期间传输已被结论,调用方应立即退出。
function maybePause(t) {
  if (!barrier.armed || t.barrierUsed || t.written < barrier.at) {
    return Promise.resolve(t.settled);
  }
  t.barrierUsed = true;
  pausedTransfers.add(t);
  announce(`PAUSED ${t.id} ${t.written}`);
  return new Promise((resolve) => {
    t.wake = () => {
      t.wake = null;
      pausedTransfers.delete(t);
      resolve(t.settled);
    };
  });
}

// 写一块数据并等待其冲刷回调(即 Node 的 drain 语义)。socket 被对端关闭、
// 出错或被 settle 销毁时同样返回;返回 false 表示写出错且已 settle('fail')。
function writeChunk(t, sock, chunk) {
  return new Promise((resolve) => {
    let done = false;
    const finish = (ok) => {
      if (done) return;
      done = true;
      t.cancelIO = null;
      if (!ok && !t.settled) settle(t, 'fail');
      resolve(ok);
    };
    t.cancelIO = () => finish(true); // settle 唤醒,仅解除阻塞
    try {
      sock.write(chunk, (err) => finish(!err));
    } catch {
      finish(false);
    }
  });
}

async function pump(t) {
  try {
    const sock = t.socket;
    while (t.pos < t.data.length) {
      if (t.settled) return;
      if (await maybePause(t)) return;
      const end = Math.min(t.pos + CHUNK_SIZE, t.data.length);
      const ok = await writeChunk(t, sock, t.data.subarray(t.pos, end));
      if (!ok) return; // settle('fail') 已执行
      t.pos = end;
      t.written = t.pos - t.offset;
      if (t.settled) return;
    }
    if (t.settled) return;
    // 末尾完成竞争点:允许测试把屏障设在文件末尾,使"最后一块已冲刷"与
    // "ABOR 到达"之间的竞争可复现;先到者通过 settle 的一次性生效获胜。
    if (await maybePause(t)) return;
    settle(t, 'ok');
  } catch {
    settle(t, 'fail');
  }
}

// ---------------------------------------------------------------------------
// 数据连接的接受与认领
// ---------------------------------------------------------------------------
function attachSocket(t, sock) {
  t.socket = sock;
  t.state = 'sending';
  if (t.timer) { clearTimeout(t.timer); t.timer = null; }
  // 传输中途对端关闭(FIN/RST)或出错 => 传输失败 426。
  // settle 幂等,成功路径上随后的 close 事件会被忽略。
  sock.on('error', () => settle(t, 'fail'));
  sock.on('end', () => settle(t, 'fail'));
  sock.on('close', () => settle(t, 'fail'));
  announce(`ATTACHED ${t.id}`);
  t.session.reply(150, `Opening BINARY mode data connection for ${t.fileName} (${t.data.length - t.offset} bytes).`);
  pump(t);
}

function onDataConn(session, listener, sock) {
  sock.setNoDelay(true);
  // 旧监听器上迟到的连接:EPSV 已替换监听器,一律拒绝。
  if (session.listener !== listener || !session.alive) {
    sock.destroy();
    return;
  }
  const t = session.transfer;
  if (t && !t.settled && t.state === 'waiting' && t.listener === listener) {
    attachSocket(t, sock);
    return;
  }
  // 没有等待中的传输:暂存一个 pending 数据连接(客户端先连数据再发 RETR
  // 的正常时序),多余的直接拒绝。
  if (session.pendingData) {
    sock.destroy();
    return;
  }
  session.pendingData = sock;
  announce('PENDING');
  const drop = () => {
    if (session.pendingData === sock) session.pendingData = null;
    sock.destroy();
  };
  sock.on('error', drop);
  sock.on('end', drop);
  sock.on('close', () => {
    if (session.pendingData === sock) session.pendingData = null;
  });
}

// ---------------------------------------------------------------------------
// 会话(每条控制连接一个)
// ---------------------------------------------------------------------------
class Session {
  constructor(ctrl) {
    this.ctrl = ctrl;
    this.alive = true;
    this.quitting = false;
    this.authState = 'user'; // 'user' -> 'pass' -> 'ready'
    this.username = null;
    this.listener = null;      // 当前 EPSV 监听器
    this.pendingData = null;   // 已被接受、尚未被 RETR 认领的数据连接
    this.restOffset = null;    // 待消费的 REST 偏移
    this.transfer = null;      // 当前活动传输(至多一个)
    this.buf = '';             // CRLF 增量解析缓冲
    this.lines = [];           // 已切出的完整命令行
    this.processing = false;
  }
  reply(code, text) {
    if (this.alive && !this.ctrl.destroyed) {
      this.ctrl.write(`${code} ${text}\r\n`);
    }
  }
}

function closeListener(s) {
  const l = s.listener;
  s.listener = null;
  if (l) { try { l.close(); } catch { /* 尚未 listen 完成 */ } }
  const p = s.pendingData;
  s.pendingData = null;
  if (p) p.destroy();
}

function cleanupSession(s) {
  if (!s.alive) return;
  s.alive = false;
  if (s.transfer && !s.transfer.settled) settle(s.transfer, 'silent');
  closeListener(s);
}

// ---------------------------------------------------------------------------
// 命令处理
// ---------------------------------------------------------------------------
function needAuth(s) {
  if (s.authState !== 'ready') {
    s.reply(530, 'Not logged in.');
    return false;
  }
  return true;
}

async function cmdEpsv(s) {
  closeListener(s); // 每次 EPSV 替换旧监听器(连同其 pending 数据连接)
  const server = net.createServer();
  server.on('connection', (sock) => onDataConn(s, server, sock));
  server.on('error', () => {});
  s.listener = server;
  let port;
  try {
    port = await listenPassive(server);
  } catch {
    if (s.listener === server) s.listener = null;
    try { server.close(); } catch { /* ignore */ }
    return s.reply(425, 'Cannot open a passive data listener.');
  }
  // await 期间会话可能已关闭或监听器又被替换:不得应答。
  if (s.listener !== server || !s.alive) {
    try { server.close(); } catch { /* ignore */ }
    return;
  }
  s.reply(229, `Entering Extended Passive Mode (|||${port}|).`);
}

function listenPassive(server) {
  return new Promise((resolve, reject) => {
    let port = PASV_MIN > 0 ? PASV_MIN : 0;
    const onError = (e) => {
      if (e.code === 'EADDRINUSE' && PASV_MIN > 0 && port < PASV_MAX) {
        port += 1;
        attempt();
      } else {
        reject(e);
      }
    };
    const attempt = () => {
      server.once('error', onError);
      server.listen(port, '0.0.0.0', () => {
        server.off('error', onError);
        resolve(server.address().port);
      });
    };
    attempt();
  });
}

function cmdRetr(s, arg) {
  if (!arg) return s.reply(501, 'RETR needs a file name.');
  const data = FILES.get(arg);
  if (!data) return s.reply(550, 'Unknown file. Available: alpha.bin, beta.bin, gamma.bin.');
  if (!s.listener) return s.reply(425, 'Use EPSV to open a data listener first.');
  const offset = s.restOffset === null ? 0 : s.restOffset;
  if (offset > data.length) return s.reply(550, 'REST offset is beyond end of file.');

  // 至此 RETR 被接受:消费 REST 偏移,并独占当前监听器、一个数据 socket、
  // 自己的文件游标与终态。被拒绝的 RETR 走不到这里,不会消费 REST。
  s.restOffset = null;
  const t = {
    id: nextTransferId++,
    session: s,
    fileName: arg,
    data,
    offset,
    pos: offset,
    written: 0,
    state: 'waiting', // 'waiting' -> 'sending';结论由 settle 一次性给出
    settled: false,
    socket: null,
    listener: s.listener,
    timer: null,
    barrierUsed: false,
    wake: null,
    cancelIO: null,
  };
  s.transfer = t;
  t.timer = setTimeout(() => settle(t, 'timeout'), ACCEPT_TIMEOUT_MS);

  const pending = s.pendingData;
  if (pending) {
    s.pendingData = null;
    pending.removeAllListeners('error');
    pending.removeAllListeners('end');
    pending.removeAllListeners('close');
    attachSocket(t, pending);
  }
  // 否则等待数据连接,超时由 t.timer 给 425。
}

const UNSUPPORTED = new Set([
  // 上传
  'STOR', 'STOU', 'APPE', 'ALLO',
  // 主动模式 / 旧式被动模式
  'PORT', 'EPRT', 'PASV',
  // 目录操作
  'LIST', 'NLST', 'MLSD', 'MLST', 'CWD', 'CDUP', 'PWD', 'XPWD',
  'MKD', 'XMKD', 'RMD', 'XRMD', 'DELE', 'RNFR', 'RNTO',
  // 其它常见但本服务不实现的命令
  'SIZE', 'MDTM', 'STAT', 'SYST', 'FEAT', 'OPTS', 'HELP', 'SITE',
  'MODE', 'STRU', 'ACCT', 'REIN', 'SMNT', 'CLNT', 'AUTH', 'PBSZ', 'PROT',
]);

async function handleCommand(s, line) {
  const sp = line.indexOf(' ');
  const verb = (sp < 0 ? line : line.slice(0, sp)).toUpperCase();
  const arg = sp < 0 ? '' : line.slice(sp + 1).trim();
  if (!verb) return s.reply(500, 'Empty command.');

  // 活动传输期间,除 ABOR、NOOP、QUIT 外的命令明确拒绝。
  if (s.transfer && !s.transfer.settled &&
      verb !== 'ABOR' && verb !== 'NOOP' && verb !== 'QUIT') {
    return s.reply(450, 'Transfer in progress; only ABOR, NOOP and QUIT are allowed.');
  }

  switch (verb) {
    case 'USER':
      s.username = arg;
      s.authState = 'pass';
      return s.reply(331, `User ${arg || '?'} okay, need password.`);
    case 'PASS': {
      if (s.authState === 'user') return s.reply(503, 'Send USER first.');
      if (s.username === DEMO_USER && arg === DEMO_PASS) {
        s.authState = 'ready';
        return s.reply(230, 'Logged in.');
      }
      s.authState = 'user';
      s.username = null;
      return s.reply(530, 'Authentication failed.');
    }
    case 'NOOP':
      return s.reply(200, 'OK.');
    case 'QUIT': {
      const t = s.transfer;
      if (t && !t.settled) settle(t, 'abort'); // 426、226 先行
      s.reply(221, 'Goodbye.');
      s.quitting = true;
      s.ctrl.end();
      return;
    }
    case 'ABOR': {
      const t = s.transfer;
      if (t && !t.settled) return settle(t, 'abort'); // 关数据侧 -> 426 -> 226
      return s.reply(225, 'No active transfer.');
    }
    case 'TYPE': {
      if (!needAuth(s)) return;
      if (!arg) return s.reply(501, 'TYPE needs an argument.');
      if (arg.toUpperCase() === 'I') return s.reply(200, 'Type set to I (binary).');
      return s.reply(504, 'Only TYPE I (binary) is supported.');
    }
    case 'EPSV':
      if (!needAuth(s)) return;
      return cmdEpsv(s);
    case 'REST': {
      if (!needAuth(s)) return;
      if (!/^\d+$/.test(arg)) return s.reply(501, 'REST needs a non-negative byte offset.');
      const n = Number(arg);
      if (!Number.isSafeInteger(n)) return s.reply(501, 'Offset too large.');
      s.restOffset = n;
      return s.reply(350, `Restarting at ${n}. Send RETR to begin transfer.`);
    }
    case 'RETR':
      if (!needAuth(s)) return;
      return cmdRetr(s, arg);
    default:
      if (UNSUPPORTED.has(verb)) {
        return s.reply(502, 'Command not supported by this read-only server.');
      }
      return s.reply(500, 'Unrecognized command.');
  }
}

// ---------------------------------------------------------------------------
// 控制连接:CRLF 增量读取,命令严格按序处理(应答顺序 == 命令顺序)
// ---------------------------------------------------------------------------
async function pumpCommands(s) {
  if (s.processing) return;
  s.processing = true;
  try {
    while (s.lines.length && s.alive && !s.quitting) {
      const line = s.lines.shift();
      await handleCommand(s, line);
    }
  } finally {
    s.processing = false;
  }
}

function onControlConn(ctrl) {
  ctrl.setNoDelay(true);
  const s = new Session(ctrl);
  s.reply(220, `readonly-ftpd ready (user ${DEMO_USER}; files: ${[...FILES.keys()].join(', ')}).`);

  ctrl.on('data', (d) => {
    s.buf += d.toString('utf8');
    let i;
    while ((i = s.buf.indexOf('\r\n')) >= 0) {
      s.lines.push(s.buf.slice(0, i));
      s.buf = s.buf.slice(i + 2);
    }
    if (s.buf.length > MAX_LINE) {
      s.buf = '';
      s.reply(500, 'Command line too long.');
      return;
    }
    pumpCommands(s);
  });
  const bye = () => cleanupSession(s);
  ctrl.on('close', bye);
  ctrl.on('error', bye);
}

// ---------------------------------------------------------------------------
// 测试钩子服务
// ---------------------------------------------------------------------------
function onHookConn(sock) {
  hookClients.add(sock);
  sock.on('close', () => hookClients.delete(sock));
  sock.on('error', () => {});
  let buf = '';
  sock.on('data', (d) => {
    buf += d.toString('utf8');
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i).trim();
      buf = buf.slice(i + 1);
      handleHook(sock, line);
    }
  });
}

function handleHook(sock, line) {
  const sp = line.indexOf(' ');
  const cmd = (sp < 0 ? line : line.slice(0, sp)).toUpperCase();
  const arg = sp < 0 ? '' : line.slice(sp + 1).trim();
  switch (cmd) {
    case 'PING':
      return void sock.write('PONG\n');
    case 'BARRIER': {
      if (arg.toUpperCase() === 'OFF' || arg === '-1') {
        barrier.armed = false;
        releasePaused();
        return void sock.write('OK barrier off\n');
      }
      const n = Number(arg);
      if (!Number.isInteger(n) || n < 0) return void sock.write('ERR bad barrier offset\n');
      barrier.armed = true;
      barrier.at = n;
      return void sock.write(`OK barrier at ${n}\n`);
    }
    case 'RELEASE':
      releasePaused();
      return void sock.write('OK released\n');
    default:
      return void sock.write('ERR unknown hook command\n');
  }
}

// ---------------------------------------------------------------------------
// 启动
// ---------------------------------------------------------------------------
function main() {
  const control = net.createServer(onControlConn);
  control.listen(CONTROL_PORT, '0.0.0.0', () => {
    const cp = control.address().port;
    if (HOOK_PORT !== undefined) {
      const hook = net.createServer(onHookConn);
      hook.listen(parseInt(HOOK_PORT, 10), '0.0.0.0', () => {
        console.log(`READY control=${cp} hook=${hook.address().port}`);
      });
    } else {
      console.log(`READY control=${cp} hook=0`);
    }
  });
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
}

if (require.main === module) main();

module.exports = { FILES, makeFile, DEMO_USER, DEMO_PASS };
