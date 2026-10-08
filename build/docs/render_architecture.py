#!/usr/bin/env python3
"""Render the repository architecture Markdown subset to a standalone reading page."""
from pathlib import Path
import argparse
import html
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'docs/architecture/repository-layout.md'
TARGET = SOURCE.with_suffix('.html')


def inline(value):
    value = html.escape(value)
    value = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)', r'<img alt="\1" src="\2">', value)
    value = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', value)
    return re.sub(r'`([^`]+)`', r'<code>\1</code>', value)


def render():
    lines = SOURCE.read_text().splitlines()
    blocks, headings = [], []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith('```'):
            code = []
            i += 1
            while i < len(lines) and not lines[i].startswith('```'):
                code.append(lines[i]); i += 1
            blocks.append('<pre><code>' + html.escape('\n'.join(code)) + '</code></pre>')
        elif line.startswith('|'):
            rows = []
            while i < len(lines) and lines[i].startswith('|'):
                cells = lines[i].strip('|').split('|')
                if not all(re.fullmatch(r'[\s:-]+', cell) for cell in cells):
                    tag = 'th' if not rows else 'td'
                    rows.append('<tr>' + ''.join(f'<{tag}>{inline(c.strip())}</{tag}>' for c in cells) + '</tr>')
                i += 1
            blocks.append('<div class="table"><table>' + ''.join(rows) + '</table></div>')
            continue
        elif line.startswith('# '):
            blocks.append('<h1>' + inline(line[2:]) + '</h1>')
        elif line.startswith('## '):
            anchor = f'section-{len(headings)+1}'
            headings.append((anchor, line[3:]))
            blocks.append(f'<h2 id="{anchor}">' + inline(line[3:]) + '</h2>')
        elif line:
            blocks.append('<p>' + inline(line) + '</p>')
        i += 1
    nav = ''.join(f'<a href="#{anchor}">{html.escape(title)}</a>' for anchor, title in headings)
    return '''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADX 当前目录与架构</title><style>
*{box-sizing:border-box}body{margin:0;background:#f5f8fb;color:#17324d;font:16px/1.8 system-ui,-apple-system,"PingFang SC",sans-serif}main{max-width:1320px;margin:auto;padding:32px}header{padding:16px 0;border-bottom:1px solid #ccdae5}a{color:#146a91;text-underline-offset:4px}.layout{display:grid;grid-template-columns:200px minmax(0,1fr);gap:36px}nav{position:sticky;top:24px;align-self:start;padding-top:32px}nav a{display:block;margin-bottom:14px;font-size:14px}article{min-width:0}h1{font-size:34px;line-height:1.35}h2{margin-top:42px;scroll-margin-top:24px}p{overflow-wrap:anywhere}img{width:100%;height:auto}pre{overflow:auto;background:#17324d;color:#e6eef5;padding:24px;border-radius:12px;font-size:13px;line-height:1.8}code{font-family:ui-monospace,monospace;font-size:.88em}p code,td code{background:#e4edf4;padding:2px 4px;border-radius:4px}.table{overflow:auto}table{border-collapse:collapse;width:100%;background:white;font-size:14px}th,td{text-align:left;padding:12px;border:1px solid #d5e0e8;min-width:140px}th{background:#e6f0f6}button{font:inherit;color:#17324d;background:white;border:1px solid #afc4d2;border-radius:8px;padding:6px 14px;cursor:pointer}header{display:flex;gap:24px;align-items:center;flex-wrap:wrap}footer{margin-top:36px;padding:20px 0;border-top:1px solid #ccdae5;font-size:13px}@media(max-width:760px){main{padding:18px}.layout{display:block}nav{position:static;display:flex;gap:14px;flex-wrap:wrap}h1{font-size:28px}}@media print{nav,header button{display:none}.layout{display:block}main{padding:0}pre{white-space:pre-wrap}body{background:white}h2{break-after:avoid}}
</style></head><body><main><header><strong>AGENT DX / ARCHITECTURE</strong><a href="repository-layout.md">Markdown 源文档</a><a href="../testing/control-plane-implementation.md">当前实现</a><button onclick="window.print()">打印 / 保存 PDF</button></header>
<div class="layout"><nav aria-label="文档目录">''' + nav + '</nav><article>' + '\n'.join(blocks) + '''</article></div>
<footer>由 build/docs/render_architecture.py 从 repository-layout.md 生成；更新 Markdown 后重新生成。验收结果与源码能力分别记录。</footer></main></body></html>
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    expected = render()
    if args.check:
        if not TARGET.exists() or TARGET.read_text() != expected:
            parser.exit(1, 'architecture HTML is stale; run python3 build/docs/render_architecture.py\n')
        print('architecture HTML matches Markdown')
    else:
        TARGET.write_text(expected)


if __name__ == '__main__':
    main()
