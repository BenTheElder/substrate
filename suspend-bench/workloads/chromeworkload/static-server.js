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
// Minimal static file server for the mirrored site (the guest has no external
// network, so Chrome loads the page from here on 127.0.0.1). No deps.
'use strict'
const http = require('http')
const fs = require('fs')
const path = require('path')

const ROOT = process.argv[2] || '/site'
const PORT = parseInt(process.argv[3] || '8080', 10)
const TYPES = {
  '.html': 'text/html', '.htm': 'text/html', '.css': 'text/css',
  '.js': 'text/javascript', '.mjs': 'text/javascript', '.json': 'application/json',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.webp': 'image/webp',
  '.ico': 'image/x-icon', '.woff': 'font/woff', '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
}

http.createServer((req, res) => {
  let rel = decodeURIComponent(req.url.split('?')[0])
  // Resolve under ROOT, never escaping it.
  const fp = path.normalize(path.join(ROOT, rel))
  if (!fp.startsWith(path.normalize(ROOT))) { res.writeHead(403); return res.end() }
  fs.stat(fp, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404); return res.end('not found') }
    res.writeHead(200, { 'content-type': TYPES[path.extname(fp).toLowerCase()] || 'application/octet-stream' })
    fs.createReadStream(fp).pipe(res)
  })
}).listen(PORT, '127.0.0.1', () => console.error('[static] serving', ROOT, 'on', PORT))
