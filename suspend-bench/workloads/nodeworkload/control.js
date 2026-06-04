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
// Control server for the realistic Node workload: a real Vite dev server (run by
// workload.sh) that a simulated Claude agent is continuously editing. This
// process speaks the SAME newline protocol as the C workload so the harness
// drives it identically, but with two realism twists:
//
//   - PING gates on the Vite dev server actually serving HTTP again, so
//     resume-to-first-ping measures "time until the dev server is usable after
//     wake" — the real UX metric for an agent sandbox.
//   - a background "agent" loop rewrites a component every 400ms and asks Vite to
//     re-transform it, keeping Vite's module graph / transform cache warm (the
//     state that must survive suspend/resume).
//
// SETWS/DIRTY/HASH still drive a synthetic buffer so the working-set axis stays a
// controlled variable on top of the real dev-server baseline.

'use strict'
const net = require('net')
const fs = require('fs')
const crypto = require('crypto')

const PAGE = 4096
const sock = process.argv[2] || '/run/node.sock'
const VITE = 'http://127.0.0.1:5173'
const COUNTER_FILE = '/app/src/components/Counter.jsx'

const seed = 0x0123456789abcdefn // survives suspend/resume in the V8 heap
let counter = 0n                  // DIRTY-controlled (stable across suspend) — used for correctness
let region = Buffer.alloc(0)      // controllable dirty working set
let editCount = 0                 // how many agent edits applied (realism, not correctness)

function fill(buf) {
  let s = Number(seed & 0xffffffffn) || 1
  for (let o = 0; o < buf.length; o += 4) { s ^= s << 13; s ^= s >>> 17; s ^= s << 5; s >>>= 0; buf.writeUInt32LE(s, o) }
}
function setWS(b) { region = Buffer.allocUnsafe(b); fill(region) }
function dirty(b) {
  if (b > region.length) b = region.length
  counter += 1n
  const c = Number(counter & 0xffffffffn)
  for (let o = 0; o < b; o += PAGE) region.writeUInt32LE(c, o)
}
function walk() { let s = 0; for (let o = 0; o < region.length; o += PAGE) s = (s + region.readUInt32LE(o)) >>> 0; return s >>> 0 }
function hash() { return crypto.createHash('sha1').update(region).digest('hex').slice(0, 16) }
function hex16(v) { return v.toString(16).padStart(16, '0') }

async function viteGet(pathName) {
  const c = new AbortController()
  const t = setTimeout(() => c.abort(), 4000)
  try { const r = await fetch(VITE + pathName, { signal: c.signal }); await r.text(); return r.ok }
  catch { return false }
  finally { clearTimeout(t) }
}

// Background "Claude agent": edit a component, then ask Vite to transform it.
function agentEdit() {
  editCount++
  const src = `import React from 'react'\nexport default function Counter() { return <div data-count="${editCount}">edits: ${editCount}</div> }\n`
  try { fs.writeFileSync(COUNTER_FILE, src) } catch {}
  viteGet('/src/components/Counter.jsx?t=' + editCount).catch(() => {})
}
setInterval(agentEdit, 400)

async function handle(line) {
  if (line.startsWith('PING')) {
    // Gate liveness on the dev server serving the app again.
    return (await viteGet('/')) ? `PONG seed=${hex16(seed)} counter=${counter.toString()}\n` : 'ERR vite-not-ready\n'
  }
  if (line.startsWith('SETWS ')) { setWS(parseInt(line.slice(6), 10)); return 'OK\n' }
  if (line.startsWith('DIRTY ')) { dirty(parseInt(line.slice(6), 10)); return 'OK\n' }
  if (line.startsWith('WALK')) { const s = walk(); await viteGet('/src/main.jsx'); return `OK ${hex16(BigInt(s))}\n` }
  if (line.startsWith('HASH')) return `HASH ${hash()}\n`
  if (line.startsWith('READY')) return 'READY\n'
  if (line.startsWith('QUIT')) { setTimeout(() => process.exit(0), 10); return 'BYE\n' }
  return 'ERR unknown\n'
}

try { fs.unlinkSync(sock) } catch {}
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
server.listen(sock, () => console.error('[control] listening on', sock))
