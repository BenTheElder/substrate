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
// Generates N small React components + a barrel file, so the Vite dev server has
// a non-trivial module graph to keep warm (like a real app mid-iteration). Run
// at image-build time: `node gen-app.js /app 30`.

const fs = require('fs')
const path = require('path')

const root = process.argv[2] || '/app'
const N = parseInt(process.argv[3] || '30', 10)
const dir = path.join(root, 'src/components')
fs.mkdirSync(dir, { recursive: true })

let imports = ''
const names = []
for (let i = 0; i < N; i++) {
  const name = `Comp${i}`
  fs.writeFileSync(path.join(dir, `${name}.jsx`),
`import React from 'react'
export default function ${name}() {
  const items = Array.from({ length: 12 }, (_, k) => ({ k, v: (k * ${i} + 7) % 97 }))
  return (
    <section className="c${i}">
      <h3>Component ${i}</h3>
      <ul>{items.map(it => <li key={it.k}>{it.k}:{it.v}</li>)}</ul>
    </section>
  )
}
`)
  imports += `import ${name} from './${name}.jsx'\n`
  names.push(name)
}
fs.writeFileSync(path.join(dir, 'generated.jsx'), imports + `export default [${names.join(', ')}]\n`)
console.error(`[gen-app] wrote ${N} components to ${dir}`)
