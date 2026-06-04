// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Control server for the headless-Chrome workload. Speaks the SAME newline
// protocol as the C/Node workloads so the harness drives it identically, but the
// working set is REAL browser memory: SETWS/DIRTY/WALK/HASH are evaluated inside
// the page via the Chrome DevTools Protocol, so the bytes live in the renderer's
// V8 heap (captured by snapshot / reclaimed by cgroup, exactly like guest RAM).
//
//   - PING gates on Chrome serving the page again (document.readyState complete),
//     so resume-to-first-ping measures "time until the browser is usable after
//     wake" — the real UX metric for an agent's browser sandbox.
//   - SETWS allocates a Uint8Array of N bytes in the page; DIRTY stamps a counter
//     into each 4 KiB page; WALK faults the whole set back in; HASH fingerprints
//     it. seed/counter live in the renderer heap and must survive suspend/resume.
//
// Node 22 provides global fetch + WebSocket, so the CDP client needs no deps.
'use strict'
const net = require('net')
const fs = require('fs')

// argv[2]: a unix socket path (CH path, bridged from vsock by socat), or
// "tcp:<port>" to listen on TCP (gVisor path — a netstack socket is
// checkpointable, unlike a bound host unix socket).
const arg = process.argv[2] || '/run/chrome.sock'
const isTCP = arg.startsWith('tcp:')
const tcpPort = isTCP ? parseInt(arg.slice(4), 10) : 0
const CDP_HTTP = 'http://127.0.0.1:9222'
const SEED = '0123456789abcdef' // 16 hex; lives in the renderer heap across suspend

// --- Page-side programs (evaluated in the renderer via Runtime.evaluate). ---
// Gate liveness on the REAL page (not the initial about:blank, which reports
// readyState 'complete' immediately) being DOM-ready. Only then stamp the seed,
// so the correctness token lives in the loaded page's context across suspend.
const PING_JS =
  `(()=>{ if(location.protocol==='about:'||location.href==='about:blank') return 'NOTREADY';` +
  ` if(document.readyState==='loading') return 'NOTREADY';` +
  ` if(typeof window.__seed==='undefined'){window.__seed=${JSON.stringify(SEED)};window.__counter=0;}` +
  ` return 'OK '+window.__seed+' '+window.__counter; })()`
const setwsJS = (n) =>
  `(()=>{ const n=${n}; const a=new Uint8Array(n); const dv=new DataView(a.buffer);` +
  ` let s=0x9e3779b9>>>0; for(let o=0;o+4<=n;o+=4096){ s^=s<<13;s>>>=0;s^=s>>>17;s^=s<<5;s>>>=0; dv.setUint32(o,s,true);} ` +
  ` window.__ws=a; if(typeof window.__seed==='undefined'){window.__seed=${JSON.stringify(SEED)};window.__counter=0;} return n; })()`
const dirtyJS = (b) =>
  `(()=>{ const a=window.__ws; if(!a) return -1; window.__counter=(window.__counter|0)+1; const c=window.__counter>>>0;` +
  ` const dv=new DataView(a.buffer); const lim=Math.min(${b},a.length); for(let o=0;o+4<=lim;o+=4096) dv.setUint32(o,c,true); return c; })()`
const WALK_JS =
  `(()=>{ const a=window.__ws; if(!a) return 0; const dv=new DataView(a.buffer); let s=0;` +
  ` for(let o=0;o+4<=a.length;o+=4096){ s=(s+dv.getUint32(o,true))>>>0; } return s>>>0; })()`
const HASH_JS =
  `(()=>{ const a=window.__ws; if(!a) return '00000000'; const dv=new DataView(a.buffer); let h=0x811c9dc5>>>0;` +
  ` for(let o=0;o+4<=a.length;o+=4096){ h^=dv.getUint32(o,true); h=Math.imul(h,0x01000193)>>>0; } return ('0000000'+h.toString(16)).slice(-8); })()`

// --- Minimal CDP client (global WebSocket + fetch, with lazy reconnect). ---
let ws = null, nextId = 1, connecting = null
const pending = new Map()
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function pickTargetURL() {
  const r = await fetch(CDP_HTTP + '/json')
  const list = await r.json()
  const pages = list.filter((t) => t.type === 'page' && t.webSocketDebuggerUrl)
  if (!pages.length) throw new Error('no page target yet')
  // Prefer the real served page over the initial about:blank tab.
  const real = pages.find((t) => /^https?:/.test(t.url || ''))
  return (real || pages[0]).webSocketDebuggerUrl
}

function connect() {
  if (connecting) return connecting
  connecting = (async () => {
    let url
    for (;;) { try { url = await pickTargetURL(); break } catch { await sleep(200) } }
    await new Promise((resolve, reject) => {
      const sock = new WebSocket(url)
      sock.onopen = () => { ws = sock; resolve() }
      sock.onerror = (e) => reject((e && e.error) || new Error('ws error'))
      sock.onclose = () => { if (ws === sock) ws = null }
      sock.onmessage = (ev) => {
        let m; try { m = JSON.parse(ev.data) } catch { return }
        if (m.id && pending.has(m.id)) {
          const p = pending.get(m.id); pending.delete(m.id)
          m.error ? p.reject(new Error(m.error.message || 'cdp error')) : p.resolve(m.result)
        }
      }
    })
  })().finally(() => { connecting = null })
  return connecting
}

function rpc(method, params) {
  return new Promise((resolve, reject) => {
    const id = nextId++
    pending.set(id, { resolve, reject })
    try { ws.send(JSON.stringify({ id, method, params })) }
    catch (e) { pending.delete(id); reject(e) }
  })
}

async function evalInPage(expression) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      if (!ws || ws.readyState !== 1) await connect()
      const res = await rpc('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true })
      if (res.exceptionDetails) throw new Error('page exception: ' + (res.exceptionDetails.text || ''))
      return res.result.value
    } catch (e) {
      ws = null
      if (attempt === 1) throw e
      await sleep(200)
    }
  }
}

async function handle(line) {
  try {
    if (line.startsWith('PING')) {
      const v = await evalInPage(PING_JS)
      if (typeof v === 'string' && v.startsWith('OK ')) {
        const [, seed, counter] = v.split(' ')
        return `PONG seed=${seed} counter=${counter}\n`
      }
      return 'ERR chrome-not-ready\n'
    }
    if (line.startsWith('SETWS ')) { await evalInPage(setwsJS(parseInt(line.slice(6), 10))); return 'OK\n' }
    if (line.startsWith('DIRTY ')) { await evalInPage(dirtyJS(parseInt(line.slice(6), 10))); return 'OK\n' }
    if (line.startsWith('WALK')) { const s = await evalInPage(WALK_JS); return `OK ${(s >>> 0).toString(16).padStart(16, '0')}\n` }
    if (line.startsWith('HASH')) { return `HASH ${await evalInPage(HASH_JS)}\n` }
    if (line.startsWith('READY')) return 'READY\n'
    if (line.startsWith('QUIT')) { setTimeout(() => process.exit(0), 10); return 'BYE\n' }
    return 'ERR unknown\n'
  } catch (e) {
    return 'ERR ' + String(e && e.message || e).replace(/\n/g, ' ') + '\n'
  }
}

connect().catch(() => {}) // warm the CDP connection while Chrome boots

if (!isTCP) { try { fs.unlinkSync(arg) } catch {} }
const server = net.createServer((conn) => {
  let buf = ''
  conn.on('data', async (chunk) => {
    buf += chunk.toString('latin1')
    let nl
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl)
      buf = buf.slice(nl + 1)
      conn.write(await handle(line))
    }
  })
  conn.on('error', () => {})
})
if (isTCP) server.listen(tcpPort, '0.0.0.0', () => console.error('[control] listening on tcp', tcpPort))
else server.listen(arg, () => console.error('[control] listening on', arg))
