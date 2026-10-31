"""Render the one canonical five-step specification; never runs product tests."""

from pathlib import Path
import re, html, json, hashlib

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "docs/testing/test-strategy.md"
TARGET = SOURCE.with_suffix(".html")
md = SOURCE.read_text()


def inline(text):
    parts = re.split(r"(`[^`]+`)", text)
    result = []
    for p in parts:
        if p.startswith("`") and p.endswith("`"):
            result.append("<code>" + html.escape(p[1:-1]) + "</code>")
        else:
            p = html.escape(p)
            p = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', p)
            p = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", p)
            result.append(p)
    return "".join(result)


def blocks(text):
    lines = text.splitlines()
    out = []
    paragraph = []
    list_type = None
    i = 0

    def flush():
        if paragraph:
            out.append("<p>" + inline(" ".join(paragraph)) + "</p>")
            paragraph.clear()

    def end_list():
        nonlocal list_type
        if list_type:
            out.append("</li></" + list_type + ">")
            list_type = None

    while i < len(lines):
        line = lines[i]
        if line.startswith("```"):
            flush()
            end_list()
            code = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code.append(lines[i])
                i += 1
            out.append("<pre><code>" + html.escape("\n".join(code)) + "</code></pre>")
            i += 1
            continue
        if line.startswith("|"):
            flush()
            end_list()
            table_rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip("|").split("|")]
                if not all(re.fullmatch(r"[\s:-]+", c) for c in cells):
                    table_rows.append(cells)
                i += 1
            header = (
                "<thead><tr>"
                + "".join('<th scope="col">' + inline(c) + "</th>" for c in table_rows[0])
                + "</tr></thead>"
            )
            body = (
                "<tbody>"
                + "".join(
                    "<tr>" + "".join("<td>" + inline(c) + "</td>" for c in row) + "</tr>" for row in table_rows[1:]
                )
                + "</tbody>"
            )
            out.append('<div class="detail-table"><table>' + header + body + "</table></div>")
            continue
        if not line.strip():
            flush()
            end_list()
            i += 1
            continue
        heading = re.match(r"^(#{1,6}) (.*)", line)
        if heading:
            flush()
            end_list()
            level = len(heading[1])
            out.append(f"<h{level}>" + inline(heading[2]) + f"</h{level}>")
            i += 1
            continue
        item = re.match(r"^(?:([-])|([0-9]+)\.) (.*)", line)
        if item:
            flush()
            kind = "ul" if item[1] else "ol"
            if kind != list_type:
                end_list()
                out.append("<" + kind + ">")
                list_type = kind
            else:
                out.append("</li>")
            out.append("<li>" + inline(item[3]))
            i += 1
            continue
        if list_type:
            out.append(" " + inline(line.strip()))
        else:
            paragraph.append(line.strip())
        i += 1
    flush()
    end_list()
    return "\n".join(out)


section_parts = re.split(r'<a id="(step-[1-5]|layer-inventory)"></a>\n', md)
assert len(section_parts) == 13
front = section_parts[0].replace('<a id="top"></a>\n', "")
front = front.split("## 先看五步的逻辑\n", 1)[1]
principles, remaining = front.split("## 五步总表\n", 1)
overview, shared = remaining.split("## 统一口径与优先级\n", 1)
sections = []
labels = ["基础UT／LLT", "单VM集成", "多namespace集成", "Buildkite门禁", "固定包验收"]
notes = [
    ("维护基线", "现有命令先取得基础保证；明确选包、feature和ignored边界。"),
    ("P0", "真实Meta/Node与挂载；11基础case及统一入口已实现。"),
    ("P1", "同VM隔离节点基础＋第二读者；共用run.py，已实测通过。"),
    ("P0 → P1", "CI用既有目标K8s；先通AFS冒烟，再扩Full。"),
    ("P1 → P2", "先让固定包可自主跑全量／专项，再补长稳和受控性能。"),
]
for section_index in range(1, len(section_parts), 2):
    identifier = section_parts[section_index]
    section_body = section_parts[section_index + 1].strip()
    section_heading, section_body = section_body.split("\n", 1)
    title = section_heading.removeprefix("## ")
    if identifier == "layer-inventory":
        reference = (
            (
                '<details id="layer-inventory" class="reference"><summary><span><span'
                ' class="kicker">按需查阅</span><h2>分层现状与源码参考</h2><small>原13集合×9列 · 原有规划 '
                '· 精确调用证据</small></span><span class="chevron" aria-hidden="true">＋</s'
                'pan></summary><div class="detail-body">'
            )
            + blocks(section_body)
            + "</div></details>"
        )
    else:
        n = int(identifier[-1])
        priority, note = notes[n - 1]
        sections.append(
            '<details id="'
            + identifier
            + '" class="step-detail"><summary><span class="number">0'
            + str(n)
            + '</span><span class="summary-copy"><h2>'
            + html.escape(title)
            + "</h2><small>"
            + html.escape(note)
            + '</small></span><span class="badge">'
            + priority
            + '</span><span class="chevron" aria-hidden="true">＋</span></summary><div class="detail-body">'
            + blocks(section_body)
            + "</div></details>"
        )
assert len(sections) == 5
styles = (
    "\n:root{color-scheme:light dark;--bg:#f5f7f8;--surface:#fff;--text:#1"
    "f3340;--muted:#61727c;--line:#dce4e7;--accent:#176b60;--soft:#edf5f2"
    ";--code:#f0f3f5;--badge:#e8f2ed;--warn:#8a5e19}*{box-sizing:border-b"
    "ox}html{scroll-behavior:smooth;scroll-padding-top:90px}body{margin:0"
    ";background:var(--bg);color:var(--text);font:15px/1.85 -apple-system"
    ',BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-'
    "serif}a{color:var(--accent);text-decoration:none}a:hover{text-decora"
    "tion:underline}button,input{font:inherit}button{cursor:pointer;borde"
    "r:1px solid var(--line);border-radius:6px;background:var(--surface);"
    "color:var(--text);padding:7px 12px}button:hover{border-color:var(--a"
    "ccent)}:focus-visible{outline:3px solid var(--accent);outline-offset"
    ":3px}[hidden]{display:none!important}.shell{max-width:1500px;margin:"
    "auto;padding:0 36px}.topline{display:flex;justify-content:space-betw"
    "een;gap:20px;align-items:center;padding:22px 0;border-bottom:1px sol"
    "id var(--line);font-size:12px;letter-spacing:.04em;color:var(--muted"
    ")}.toplinks{display:flex;gap:18px}.hero{padding:35px 0 20px}.eyebrow"
    ",.kicker{color:var(--accent);font-size:12px;letter-spacing:.08em}h1{"
    "font-size:35px;line-height:1.35;letter-spacing:-.02em;margin:8px 0 1"
    "2px}h2{font-size:23px;line-height:1.45;margin:0 0 14px}h3{font-size:"
    "19px;line-height:1.5;margin:30px 0 12px}h4{font-size:16px;margin:24p"
    "x 0 10px}.lead{font-size:18px;margin:0 0 16px}.meta{display:flex;fle"
    "x-wrap:wrap;gap:8px}.pill,.badge{display:inline-block;background:var"
    "(--badge);border-radius:5px;padding:3px 9px;font-size:12px;white-spa"
    "ce:nowrap;color:var(--accent)}.pill.neutral{background:var(--code);c"
    "olor:var(--muted)}.principles{padding:15px 0 22px}.principles>p:firs"
    "t-child{font-size:17px}.principles ol{padding-left:24px;margin:15px "
    "0}.principles li{padding:10px 0 10px 5px;border-bottom:1px solid var"
    "(--line)}.principles>p:last-child{font-size:13px;color:var(--muted);"
    "max-width:1200px}.journey{display:flex;gap:7px;flex-wrap:wrap;paddin"
    "g:12px 0;background:var(--bg);position:sticky;top:0;z-index:5;border"
    "-top:1px solid var(--line);border-bottom:1px solid var(--line)}.jour"
    "ney a{padding:6px 13px;border:1px solid var(--line);background:var(-"
    "-surface);border-radius:6px;font-size:14px}.section-heading{display:"
    "flex;justify-content:space-between;gap:16px;align-items:baseline;mar"
    "gin:32px 0 10px}.section-heading h2{margin:0}.muted{color:var(--mute"
    "d);font-size:13px}.overview{margin-bottom:25px}.detail-table{overflo"
    "w:auto;max-width:100%;border:1px solid var(--line);border-radius:8px"
    ";background:var(--surface);margin:15px 0}table{width:100%;min-width:"
    "1080px;table-layout:fixed;border-collapse:separate;border-spacing:0;"
    "font-size:13px;line-height:1.8}th,td{padding:14px;vertical-align:top"
    ";text-align:left;border-right:1px solid var(--line);border-bottom:1p"
    "x solid var(--line);overflow-wrap:anywhere}th{background:var(--code)"
    ";font-weight:650}th:last-child,td:last-child{border-right:0}tr:last-"
    "child td{border-bottom:0}.overview table{min-width:1220px}.overview "
    "th:first-child,.overview td:first-child{width:135px}.overview th:las"
    "t-child,.overview td:last-child{width:145px}.overview td:last-child{"
    "background:var(--soft)}code{font: .88em/1.5 ui-monospace,SFMono-Regu"
    "lar,Menlo,Consolas,monospace;background:var(--code);border-radius:4p"
    "x;padding:2px 5px;overflow-wrap:anywhere}pre{background:var(--code);"
    "border:1px solid var(--line);border-radius:7px;padding:18px;overflow"
    ":auto;max-width:100%;line-height:1.65}pre code{background:transparen"
    "t;padding:0;white-space:pre;overflow-wrap:normal}.shared{padding:3px"
    " 0 25px;font-size:14px}.shared li{margin:10px 0}.tools{display:flex;"
    "justify-content:space-between;gap:16px;flex-wrap:wrap;margin:8px 0 1"
    "5px}.tools-left{display:flex;gap:8px;flex-wrap:wrap}.search{backgrou"
    "nd:var(--surface);color:var(--text);border:1px solid var(--line);bor"
    "der-radius:6px;padding:8px 12px;width:280px}.step-detail,.reference{"
    "background:var(--surface);border:1px solid var(--line);border-radius"
    ":9px;margin:13px 0;scroll-margin-top:85px}.step-detail>summary,.refe"
    "rence>summary{list-style:none;display:flex;align-items:center;gap:18"
    "px;cursor:pointer;padding:20px 23px}.step-detail>summary::-webkit-de"
    "tails-marker,.reference>summary::-webkit-details-marker{display:none"
    "}.number{font:22px/1 ui-monospace,monospace;color:var(--accent)}.sum"
    "mary-copy{flex:1;min-width:0}summary h2{font-size:20px;margin:0 0 4p"
    "x}summary small{display:block;color:var(--muted);font-size:13px;line"
    "-height:1.7}.chevron{font-size:22px;font-weight:400;color:var(--mute"
    "d)}details[open]>summary .chevron{font-size:0}details[open]>summary "
    ".chevron:after{content:'−';font-size:22px}.detail-body{border-top:1p"
    "x solid var(--line);padding:15px 25px 30px}.detail-body p{margin:12p"
    "x 0}.detail-body li{margin:8px 0}.detail-body>h3{padding-top:18px;bo"
    "rder-top:1px solid var(--line);margin-top:32px}.detail-body>h3:first"
    "-child{border:0;margin-top:0}.reference{margin-top:28px;background:t"
    "ransparent}.reference>summary{justify-content:space-between}.referen"
    "ce table{min-width:2450px}.reference .detail-body{background:var(--s"
    "urface)}.empty{padding:20px;border:1px dashed var(--line);border-rad"
    "ius:8px;color:var(--muted)}footer{font-size:12px;color:var(--muted);"
    "padding:27px 0 35px;display:flex;justify-content:space-between;gap:2"
    "0px;flex-wrap:wrap}.sr-only{position:absolute;width:1px;height:1px;o"
    "verflow:hidden;clip:rect(0,0,0,0)}\n@media(prefers-color-scheme:dark)"
    "{:root{--bg:#142128;--surface:#1b2b34;--text:#e0e9ed;--muted:#9cafb9"
    ";--line:#354750;--accent:#86d1bd;--soft:#1b3933;--code:#23363f;--bad"
    "ge:#254039;--warn:#e3c17c}}@media(max-width:800px){.shell{padding:0 "
    "22px}.journey{position:static}.step-detail>summary{padding:18px;gap:"
    "12px}.detail-body{padding:10px 18px 24px}}@media(max-width:560px){.s"
    "hell{padding:0 16px}h1{font-size:29px}.hero{padding-top:24px}.lead{f"
    "ont-size:16px}.topline{align-items:flex-start}.toplinks{gap:10px}.jo"
    "urney a{padding:6px 9px}.section-heading{display:block}summary h2{fo"
    "nt-size:18px}.number{font-size:18px}.step-detail>summary{flex-wrap:w"
    "rap}.summary-copy{flex-basis:calc(100% - 50px)}.step-detail .badge{m"
    "argin-left:30px}.step-detail .chevron{margin-left:auto}.search{width"
    ":100%;font-size:16px}.tools-right{width:100%}.detail-body{padding:8p"
    "x 14px 22px}.section-heading h2{font-size:21px}}@media print{@page{s"
    "ize:A3 landscape;margin:12mm}:root{color-scheme:light;--bg:#fff;--su"
    "rface:#fff;--text:#172a35;--muted:#516471;--line:#bbc9d0;--accent:#2"
    "15e51;--code:#eef2f3;--soft:#edf5f2;--badge:#edf5f2}.shell{max-width"
    ":none;padding:0}.journey,.tools,.toplinks,.chevron,.empty{display:no"
    "ne!important}.hero{padding:15px 0}.principles{break-after:page}.deta"
    "il-table{overflow:visible;border:0}table,.overview table,.reference "
    "table{min-width:0;font-size:9pt;table-layout:fixed}th,td{padding:6px"
    "}thead{display:table-header-group}tr{break-inside:avoid}details{bord"
    "er:0}.detail-body{padding:0;border:0}.step-detail{break-before:page}"
    ".step-detail>summary{padding:10px 0}.reference{break-before:page}pre"
    ",pre code{white-space:pre-wrap;overflow-wrap:anywhere}footer{padding"
    ":15px 0}}\n"
)
script = (
    "\n'use strict';\nconst items=[...document.querySelectorAll('.step-deta"
    "il')];\nconst allDetails=[...document.querySelectorAll('main details'"
    ")];\nconst search=document.getElementById('search');\nconst status=doc"
    "ument.getElementById('search-status');\nfunction openTarget(){let tar"
    "get;try{target=document.getElementById(decodeURIComponent(location.h"
    "ash.slice(1)));}catch(e){return;}if(!target)return;let node=target;w"
    "hile(node){if(node.tagName==='DETAILS')node.open=true;node=node.pare"
    "ntElement;}target.scrollIntoView({block:'start'});}\nwindow.addEventL"
    "istener('hashchange',openTarget);\ndocument.querySelectorAll('a[href^"
    "=\"#\"]').forEach(link=>link.addEventListener('click',()=>{if(search.v"
    "alue){search.value='';applySearch();}if(link.hash===location.hash)op"
    "enTarget();}));\nfunction applySearch(){const q=search.value.trim().t"
    "oLowerCase();let shown=0;items.forEach(item=>{const match=!q||item.t"
    "extContent.toLowerCase().includes(q);item.hidden=!match;if(match){sh"
    "own++;if(q)item.open=true;}});status.textContent=q?'匹配 '+shown+' / 5"
    " 步':'5 步详细规范 · 按需展开';document.getElementById('empty').hidden=shown!="
    "=0;}\nsearch.addEventListener('input',applySearch);\ndocument.getEleme"
    "ntById('expand').addEventListener('click',()=>items.filter(x=>!x.hid"
    "den).forEach(x=>x.open=true));\ndocument.getElementById('collapse').a"
    "ddEventListener('click',()=>allDetails.forEach(x=>x.open=false));\nle"
    "t beforePrint=[];\nwindow.addEventListener('beforeprint',()=>{beforeP"
    "rint=allDetails.map(x=>({open:x.open,hidden:x.hidden}));allDetails.f"
    "orEach(x=>{x.hidden=false;x.open=true;});});\nwindow.addEventListener"
    "('afterprint',()=>allDetails.forEach((x,i)=>{if(beforePrint[i]){x.op"
    "en=beforePrint[i].open;x.hidden=beforePrint[i].hidden;}}));\ndocument"
    ".getElementById('print').addEventListener('click',()=>window.print()"
    ");\napplySearch();if(location.hash)openTarget();\n"
)
nav = "".join(
    '<a href="#step-' + str(section_index + 1) + '">' + str(section_index + 1) + " " + label + "</a>"
    for section_index, label in enumerate(labels)
)
page = (
    (
        '<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8"><meta'
        ' name="viewport" content="width=device-width,initial-scale=1"><meta '
        'name="color-scheme" content="light dark"><title>ADX／AFS 测试规范 · 五步验证路'
        "径</title><style>"
    )
    + styles
    + (
        '</style></head><body><main class="shell" id="top">\n<header class="to'
        'pline"><span>AGENT DX / TEST STRATEGY</span><div class="toplinks"><a'
        ' href="test-strategy.md">Markdown原文</a><button id="print" type="butt'
        'on">打印／PDF</button></div></header>\n<section class="hero"><div class='
        '"eyebrow">已确认规范 · 单页收敛</div><h1>ADX／AFS 五步测试规范</h1><p class="lead">开'
        '发自检 → 共享CI门禁 → 固定包验收</p><div class="meta"><span class="pill">第二/三步基础'
        '已实现 · CI1组件已通过</span><span class="pill neutral">2026-11-01</span><s'
        'pan class="pill neutral">候选 00f1695b</span></div></section>\n<secti'
        'on class="principles" aria-labelledby="principles-title"><h2 id="pri'
        'nciples-title">先把五步逻辑讲清楚</h2>'
    )
    + blocks(principles)
    + """</section>
<nav class="journey" aria-label="五步同页导航">"""
    + nav
    + (
        '<a href="#layer-inventory">分层参考</a></nav>\n<section id="overview" cla'
        'ss="overview"><div class="section-heading"><h2>五步总表</h2><span class='
        '"muted">原则 · 现状 · 环境与入口 · 待补 · 优先级</span></div>'
    )
    + blocks(overview)
    + """</section>
<section class="shared"><h2>统一口径与实施优先级</h2>"""
    + blocks(shared)
    + (
        '</section>\n<section aria-labelledby="detail-title"><div class="secti'
        'on-heading"><h2 id="detail-title">再按步骤查看细节</h2><span class="muted" i'
        'd="search-status" role="status" aria-live="polite">5步详细规范</span></di'
        'v><div class="tools"><div class="tools-left"><button id="expand" typ'
        'e="button">展开五步</button><button id="collapse" type="button">收起细节</bu'
        'tton></div><div class="tools-right"><label><span class="sr-only">搜索五'
        '步详细规范</span><input id="search" class="search" type="search" placehol'
        'der="搜索用例、目录、脚本、环境…"></label></div></div>'
    )
    + "".join(sections)
    + """<p id="empty" class="empty" hidden>没有匹配的步骤，请调整关键词。</p></section>"""
    + reference
    + (
        "<footer><span>CI1组件闭环已通过 · SDK产品与必需门禁待补</span>"
        "<span>源码已有断言、历史通过与当前实测分别登记</span></footer></main><script>"
    )
    + script
    + """</script></body></html>"""
)
TARGET.write_text(page)
metadata = {
    "kind": "single-page-five-step-render",
    "source": str(SOURCE),
    "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
    "output": str(TARGET),
    "output_sha256": hashlib.sha256(TARGET.read_bytes()).hexdigest(),
    "steps": 5,
    "overview_rows": 5,
    "overview_columns": 6,
    "retained_reference_rows": 13,
    "retained_reference_columns": 9,
    "normative_html_pages": 1,
    "offline": True,
    "component_integration_implemented": True,
    "ci_asset_handoff_implemented": True,
    "ci_component_adapter_implemented": True,
    "ci_runtime_validated": True,
    "ci_runtime_scope": "Full #76 targeted AFS components only; product and required gate pending",
    "ci_product_cases_integrated": False,
}
print(json.dumps(metadata, ensure_ascii=False, indent=2))
