#!/usr/bin/env python3
"""Check repository document links, JSON examples, SVG and generated architecture."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit
import argparse
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import render_architecture

ROOT = Path(__file__).resolve().parents[2]


class HTMLLinks(HTMLParser):
    def __init__(self):
        super().__init__(); self.links = []; self.ids = set()
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs: self.ids.add(attrs['id'])
        for name in ('href', 'src'):
            if attrs.get(name): self.links.append(attrs[name])


def markdown_anchors(text):
    result, seen = set(), {}
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', text, re.M):
        slug = re.sub(r'[^\w\-\s]', '', heading.lower()).replace(' ', '-')
        count = seen.get(slug, 0); seen[slug] = count + 1
        result.add(slug + (f'-{count}' if count else ''))
    result.update(re.findall(r'\bid=["\']([^"\']+)', text))
    return result


def check():
    tracked = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT).decode().split('\0')
    docs = sorted({p for p in tracked if Path(p).suffix in {'.md', '.rst', '.html', '.svg'}})
    errors, checked_links, examples, svg_count = [], 0, 0, 0
    for name in docs:
        path = ROOT / name
        if not path.is_file(): continue
        text = path.read_text()
        links = []
        if path.suffix == '.md':
            fences = re.findall(r'^```([^\n]*)\n(.*?)^```\s*$', text, re.M | re.S)
            if len(re.findall(r'^```', text, re.M)) % 2:
                errors.append(f'{name}: unclosed code fence')
            for language, body in fences:
                if language.strip() == 'json':
                    examples += 1
                    try: json.loads(body)
                    except ValueError as exc: errors.append(f'{name}: JSON example: {exc}')
            prose = re.sub(r'^```[^\n]*\n.*?^```\s*$', '', text, flags=re.M | re.S)
            links = re.findall(r'!?\[[^\]]*\]\(([^)]+)\)', prose)
            links += re.findall(r'^\[[^\]]+\]:\s*(\S+)', prose, re.M)
        elif path.suffix == '.html':
            parser = HTMLLinks(); parser.feed(text); links = parser.links
        elif path.suffix == '.svg':
            svg_count += 1
            try: ET.fromstring(text)
            except ET.ParseError as exc: errors.append(f'{name}: SVG: {exc}')
        for value in links:
            value = value.strip().strip('<>')
            parsed = urlsplit(value)
            if parsed.scheme or value.startswith('//'): continue
            target = (path.parent / unquote(parsed.path)).resolve() if parsed.path else path
            checked_links += 1
            if not target.exists():
                errors.append(f'{name}: missing link {value}'); continue
            if parsed.fragment and target.suffix in {'.md', '.html'}:
                if target.suffix == '.md': anchors = markdown_anchors(target.read_text())
                else:
                    parser = HTMLLinks(); parser.feed(target.read_text()); anchors = parser.ids
                if unquote(parsed.fragment) not in anchors:
                    errors.append(f'{name}: missing anchor {value}')
    if render_architecture.TARGET.read_text() != render_architecture.render():
        errors.append('architecture HTML differs from Markdown; regenerate it')
    return {'documents': len(docs), 'local_links': checked_links, 'json_examples': examples,
            'svg_files': svg_count, 'errors': errors, 'files': docs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = check()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k:v for k,v in result.items() if k != 'files'}, ensure_ascii=False, indent=2))
    return bool(result['errors'])


if __name__ == '__main__':
    sys.exit(main())
