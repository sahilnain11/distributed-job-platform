#!/usr/bin/env node
'use strict';

// Optional media-generation tool. It never submits jobs or calls the backend.
// Requires Playwright/Chromium and ffmpeg; these are not application dependencies.
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { spawnSync } = require('node:child_process');
const { chromium } = require('playwright');

const root = path.resolve(__dirname, '..');
const media = path.join(root, 'docs', 'demo');
const output = path.join(root, 'target', 'demo-render');
const story = JSON.parse(fs.readFileSync(path.join(media, 'storyboard.json'), 'utf8'));
const ffmpeg = process.env.FFMPEG || 'ffmpeg';

function encode(args) {
  const result = spawnSync(ffmpeg, ['-hide_banner', '-loglevel', 'error', '-y', ...args], {
    encoding: 'utf8', timeout: 120000,
  });
  if (result.error || result.status !== 0) {
    throw new Error(result.error?.message || result.stderr || 'Media encoding failed');
  }
}

function checkStory() {
  if (story.width !== 1280 || story.height !== 720 || story.seconds_per_scene !== 5 || story.scenes.length !== 12) {
    throw new Error('Review timing/layout changes before rendering the fixed one-minute walkthrough');
  }
  const ids = new Set();
  for (const scene of story.scenes) {
    if (ids.has(scene.id) || !Number.isInteger(scene.step) || scene.step < 0 || !scene.caption || scene.caption.length > 330) {
      throw new Error('Invalid storyboard scene: ' + scene.id);
    }
    ids.add(scene.id);
  }
}

const template = `<!doctype html><html lang="en"><meta charset="utf-8"><title>Offline walkthrough frame</title>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; connect-src 'none'">
<style>
*{box-sizing:border-box}body{margin:0;width:1280px;height:720px;background:#0b1220;color:#e6edf7;font-family:Arial,Helvetica,sans-serif;padding:37px 48px}
header{display:flex;justify-content:space-between;align-items:center;font-size:15px;color:#a6bad2}.brand{font-weight:bold;font-size:20px;color:#5eead4}.label{border:1px solid #536447;border-radius:20px;padding:8px 13px;color:#fcd34d;font-size:12px;letter-spacing:1px}
.chapter{color:#5eead4;font-size:14px;letter-spacing:2px;margin-top:27px;font-weight:bold}h1{font-size:38px;letter-spacing:-1px;margin:12px 0 15px;line-height:1.15;height:47px}#caption{font-size:21px;color:#c0cfe2;line-height:1.48;height:94px;margin:0}
.flow{display:flex;gap:12px;align-items:center;margin:24px 0 22px}.node{flex:1;background:#17263a;border:1px solid #3c516f;border-radius:12px;text-align:center;padding:16px 8px;font-size:17px;font-weight:bold}.active{border-color:#5eead4;background:#19463f;color:#89f4e4}.arrow{color:#5eead4;font-size:21px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.box{background:#121e30;border:1px solid #2b3c53;border-radius:12px;padding:17px 20px;height:193px}.small{color:#9cb0c9;font-size:13px;text-transform:uppercase;letter-spacing:1px}.facts{display:flex;justify-content:space-between;margin-top:10px;font-size:18px}.facts strong{color:#5eead4}.detail{font-family:monospace;font-size:14px;line-height:1.5;color:#d4e5f4;white-space:pre-wrap;margin:12px 0 0}.delivery{font-size:15px;line-height:1.45;margin:12px 0 0;color:#c0cfe2}.footer{position:absolute;bottom:32px;left:48px;right:48px;display:flex;justify-content:space-between;color:#91a6bf;font-size:13px}.progress{position:absolute;left:48px;bottom:15px;right:48px;height:4px;background:#233247}.progress>div{height:100%;background:#5eead4}
</style><header><span class="brand">↗ Distributed Job Platform</span><span class="label">ILLUSTRATED · OFFLINE · NOT LIVE</span></header>
<div class="chapter" id="chapter"></div><h1 id="title"></h1><p id="caption"></p>
<div class="flow"><div class="node">API</div><span class="arrow">→</span><div class="node">PostgreSQL</div><span class="arrow">↔</span><div class="node">Publisher</div><span class="arrow">→</span><div class="node">RabbitMQ</div><span class="arrow">→</span><div class="node">Worker</div></div>
<div class="grid"><div class="box"><span class="small">Illustrative committed database state</span><div class="facts"><span>J1: <strong id="state"></strong></span><span>Attempts: <strong id="counts"></strong></span></div><div class="detail" id="history"></div><div class="detail" id="outbox"></div></div><div class="box"><span class="small">Ownership and delivery</span><p class="delivery" id="lease"></p><p class="delivery" id="delivery"></p><p class="delivery" id="note"></p></div></div>
<div class="footer"><span>sahilnain11 / distributed-job-platform</span><span>Timing compressed · Teaching IDs, not real jobs</span><span id="counter"></span></div><div class="progress"><div id="bar"></div></div></html>`;

async function main() {
  checkStory();
  fs.mkdirSync(output, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({ viewport: { width: story.width, height: story.height }, deviceScaleFactor: 1 });
    // Nothing outside this checkout may be loaded: no dashboard, API, secrets or telemetry.
    await context.route('**/*', route => route.request().url().startsWith('file:') ? route.continue() : route.abort());
    const explorer = await context.newPage();
    await explorer.goto(pathToFileURL(path.join(root, 'docs', 'implementation-explorer.html')).href);
    const frame = await context.newPage();
    await frame.setContent(template);
    const snapshots = [];
    for (const [index, scene] of story.scenes.entries()) {
      const snapshot = await explorer.evaluate(({ scenario, step }) => {
        if (!scenarios[scenario] || !scenarios[scenario][step]) throw new Error('Storyboard no longer matches explorer');
        const facts = { ...initial };
        for (let i = 0; i <= step; i++) Object.assign(facts, scenarios[scenario][i].change);
        return { ...facts, decision: scenarios[scenario][step].title };
      }, scene);
      snapshots.push({ scene: scene.id, ...snapshot });
      await frame.evaluate(({ scene, snapshot, index, length }) => {
        const put = (id, value) => document.getElementById(id).textContent = value;
        put('chapter', scene.chapter); put('title', scene.title); put('caption', scene.caption);
        put('state', snapshot.state); put('counts', snapshot.lifetime + ' / ' + snapshot.cycle);
        put('history', snapshot.history); put('outbox', 'Outbox: ' + snapshot.outbox);
        put('lease', 'Lease: ' + snapshot.lease); put('delivery', snapshot.delivery);
        put('note', index === 11 ? 'Tests and measurements are evidence under stated conditions—not universal guarantees.' : 'Decision: ' + snapshot.decision);
        put('counter', String(index + 1).padStart(2, '0') + ' / ' + length);
        document.getElementById('bar').style.width = ((index + 1) / length * 100) + '%';
        document.querySelectorAll('.node').forEach(node => node.classList.toggle('active', node.textContent === scene.focus));
      }, { scene, snapshot, index, length: story.scenes.length });
      await frame.evaluate(() => document.fonts.ready);
      const overflow = await frame.evaluate(() => [...document.querySelectorAll('h1,#caption,.box')].some(node => node.scrollHeight > node.clientHeight + 1));
      if (overflow) throw new Error('Frame text overflows: ' + scene.id);
      await frame.screenshot({ path: path.join(output, `frame-${String(index).padStart(2, '0')}.png`) });
    }
    fs.writeFileSync(path.join(output, 'snapshots.json'), JSON.stringify(snapshots, null, 2) + '\n');
  } finally {
    await browser.close();
  }
  const seconds = story.scenes.length * story.seconds_per_scene;
  encode(['-framerate', `1/${story.seconds_per_scene}`, '-i', path.join(output, 'frame-%02d.png'),
    '-t', String(seconds), '-r', '10', '-c:v', 'libx264', '-preset', 'slow', '-crf', '29',
    '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-map_metadata', '-1', '-an', path.join(media, 'walkthrough.mp4')]);
  encode(['-framerate', `1/${story.seconds_per_scene}`, '-i', path.join(output, 'frame-%02d.png'),
    '-filter_complex', '[0:v]scale=800:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=64:stats_mode=full[p];[b][p]paletteuse=dither=none',
    '-loop', '0', '-final_delay', String(story.seconds_per_scene * 100), path.join(media, 'preview.gif')]);
  for (const name of ['walkthrough.mp4', 'preview.gif']) {
    const size = fs.statSync(path.join(media, name)).size;
    if (size > 1_000_000) throw new Error(`${name} exceeds public artifact budget: ${size}`);
    console.log(`${name}: ${size} bytes`);
  }
  console.log(`Rendered ${seconds}s, ${story.scenes.length} frames from existing explorer snapshots. No backend requests.`);
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });