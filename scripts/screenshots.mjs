// Captures the demo's output as terminal-style screenshots for the README (docs/images/).
//   NODE_PATH=<a node_modules with playwright> node scripts/screenshots.mjs [nats-url]
// Without a NATS URL only the in-memory run is captured.
import { execFileSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { writeFileSync } from 'node:fs'

const require = createRequire(import.meta.url)
const { chromium } = require('playwright')

const nats = process.argv[2]
const runs = [{ file: 'demo-memory.png', cmd: 'go run ./examples/demo', args: ['run', './examples/demo'] }]
if (nats) runs.push({ file: 'demo-nats.png', cmd: `go run ./examples/demo -nats ${nats.replace(/:\/\/.*@/, '://')}`, args: ['run', './examples/demo', '-nats', nats] })

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;')
const color = (line) => {
  let l = esc(line)
  l = l.replace(/^(\s*[\d.]+s)/, '<span class="t">$1</span>')
  if (/failed for good|dead letter/.test(line)) return `<span class="red">${l}</span>`
  if (/failed:|released|SIGTERM/.test(line)) return `<span class="yellow">${l}</span>`
  if (/sent to|built|run /.test(line)) return `<span class="green">${l}</span>`
  return l
}

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 980, height: 200 }, deviceScaleFactor: 2 })
for (const r of runs) {
  const out = execFileSync('go', r.args, { encoding: 'utf8', env: { ...process.env, GOTOOLCHAIN: 'local' } })
  const html = `<!doctype html><meta charset="utf-8"><style>
    body{margin:0;background:#e9ecef;font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
    .win{margin:16px;border-radius:10px;background:#1e1f24;color:#d7dae0;box-shadow:0 6px 24px #0003;overflow:hidden}
    .bar{background:#2c2e35;padding:9px 14px;display:flex;gap:7px;align-items:center}
    .bar i{width:12px;height:12px;border-radius:50%;display:inline-block}
    .bar span{margin-left:10px;color:#9aa0aa;font-size:12px}
    pre{margin:0;padding:14px 18px 18px;white-space:pre-wrap}
    .p{color:#7fb4ff}.t{color:#7d8590}.green{color:#7ee2a8}.yellow{color:#f2cc60}.red{color:#ff8a80}
  </style><div class="win"><div class="bar"><i style="background:#ff5f57"></i><i style="background:#febc2e"></i><i style="background:#28c840"></i><span>jobqueue demo</span></div>
  <pre><span class="p">$ ${esc(r.cmd)}</span>\n${out.trimEnd().split('\n').map(color).join('\n')}</pre></div>`
  await page.setContent(html)
  const win = await page.$('.win')
  await win.screenshot({ path: `docs/images/${r.file}` })
  writeFileSync(`docs/images/${r.file.replace('.png', '.txt')}`, out)
  console.log('wrote docs/images/' + r.file)
}
await browser.close()
