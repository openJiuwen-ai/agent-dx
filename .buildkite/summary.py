#!/usr/bin/env python3
"""Publish explicit build artifacts and validation evidence on the build page."""
import argparse
import hashlib
import html
import json
import os
from pathlib import Path


def read(path):
    return json.loads(path.read_text()) if path.is_file() else None


def link(label, path):
    return f'[{label}](artifact://out/buildkite/{path})'


def code(value):
    return '<code>' + html.escape(str(value)) + '</code>'


def collect(root, stage, exit_code, commit, artifact_build=None):
    # Base packaging and Full acceptance are independent Buildkite pipelines.
    # The Full image stage therefore starts its own summary, while the E2E job
    # still consumes and extends the image provenance from its preceding job.
    previous = {'e2e': 'images'}.get(stage)
    if stage == 'images' and (root / 'summaries/release.json').is_file():
        previous = 'release'
    result = read(root / 'summaries' / f'{previous}.json') if previous else None
    if result and result['commit'] != commit:
        if stage != 'e2e' or not artifact_build:
            raise ValueError('summary belongs to a different commit')
        result['image_build_commit'] = result['commit']
        result['commit'] = commit
    if previous and not result and exit_code == 0:
        raise ValueError('previous stage summary missing')
    result = result or {'commit': commit, 'stages': {}}
    result['stages'][stage] = {'status': 'passed' if exit_code == 0 else 'failed', 'exit_code': exit_code}
    if stage == 'release':
        manifest = read(root / 'release-manifest.json')
        build_manifest = read(root / 'build-manifest.json')
        archive = root / 'adx-release.tar.gz'
        if manifest and build_manifest and archive.is_file():
            if build_manifest.get('commit') != commit:
                raise ValueError('build manifest belongs to a different commit')
            digest = hashlib.sha256()
            with archive.open('rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(chunk)
            result['release'] = {'manifest': manifest, 'build_manifest': build_manifest,
                                 'bytes': archive.stat().st_size,
                                 'sha256': digest.hexdigest(),
                                 'sdk': sorted(p.name for p in (root / 'sdk').glob('*.whl'))}
        if result.get('release'):
            result['release']['admin'] = sorted(p.name for p in (root / 'admin').glob('adxadmin-*') if p.suffix == '.whl' or p.name.endswith('.tar.gz'))
            result['release']['execd'] = (root / 'adx-execd.tar.gz').is_file()
            result['release']['sdk_source'] = sorted(p.name for p in (root / 'sdk').glob('*.tar.gz'))
        if exit_code == 0 and not result.get('release'):
            raise ValueError('release artifacts missing')
    elif stage == 'images':
        bundle = read(root / 'bundle/bundle.json')
        registry = read(root / 'bundle/registry-images.json')
        if bundle and registry:
            product_commit = bundle.get('package', {}).get('commit')
            if product_commit:
                result['product_commit'] = product_commit
            result['images'] = {'references': registry['references'], 'base_images': bundle['base_images'],
                                'backend': bundle['backend']['sandboxd_revision'], 'collector': bundle.get('collector')}
        if exit_code == 0 and not result.get('images'):
            raise ValueError('published image references missing')
    elif stage == 'e2e':
        report = read(root / 'acceptance/result.json')
        bundle = read(root / 'bundle/bundle.json')
        bundle_commit = (bundle or {}).get('package', {}).get('commit')
        if bundle_commit:
            if result.get('product_commit') and result['product_commit'] != bundle_commit:
                raise ValueError('image summary product commit differs from the image bundle')
            result['product_commit'] = bundle_commit
        if report and result.get('product_commit'):
            harness = report.get('harness') or {}
            if not harness:
                if exit_code == 0:
                    raise ValueError('acceptance harness identity missing')
            else:
                if harness.get('commit') != commit:
                    raise ValueError('acceptance harness commit differs from this build')
                if harness.get('product_commit') != result['product_commit']:
                    raise ValueError('acceptance product commit differs from the image bundle')
        result['e2e'] = {'report': report, 'placement': read(root / 'acceptance/placement.json') or []}
        result['e2e']['collection'] = {
            node: {kind: read(root / 'acceptance' / node / f'{kind}-{node}.json')
                   for kind in ('collection', 'gateway-metrics', 'traces')}
            for node in ('node1', 'node2')}
        if (exit_code == 0 and result.get('images', {}).get('collector')
                and 'stop' in (report or {}).get('required_checks', [])):
            if not all(e and e.get('status') == 'passed'
                       for node in result['e2e']['collection'].values() for e in node.values()):
                raise ValueError('Collector and Gateway metrics evidence missing or failed')
        if exit_code == 0 and (not report or report['status'] != 'passed' or
                               report['cleanup_errors'] or report['missing_checks']):
            raise ValueError('Kubernetes acceptance evidence missing or failed')
    return result


def render(result):
    lines = ['## ADX 构建与产物汇总', '', '提交：' + code(result['commit']), '',
             '| 阶段 | 状态 | 完整日志 |', '|---|---|---|']
    product_commit = result.get('product_commit')
    if product_commit and product_commit != result['commit']:
        lines[3:3] = ['产品产物提交：' + code(product_commit), '']
    if result.get('image_build_commit'):
        lines[3:3] = ['镜像构建提交：' + code(result['image_build_commit']), '']
    for stage, name in [('release', '编译与发布包'), ('images', '镜像构建与推送'), ('e2e', 'Kubernetes E2E')]:
        state = result['stages'].get(stage)
        if state:
            lines.append(f"| {name} | {state['status']}（exit {state['exit_code']}） | " +
                         link('日志', f'logs/step-{stage}.log') + ' |')
    release = result.get('release')
    if release:
        manifest = release['manifest']
        lines += ['', '### 发布包', '',
                  link('下载统一发布包', 'adx-release.tar.gz') + ' · ' +
                  link('SHA256 文件', 'adx-release.tar.gz.sha256') + ' · ' +
                  link('包内文件清单', 'release-manifest.json') + ' · ' +
                  link('构建汇总清单', 'build-manifest.json'), '',
                  f"平台：{code(manifest['target'])}；配置：{code(manifest['profile'])}；大小：{release['bytes'] / 1048576:.1f} MiB", '',
                  'SHA256：' + code(release['sha256']), '',
                  '组件：' + ', '.join(code(Path(p).name) for p in manifest['files'] if p.startswith(('bin/', 'runtime/')))]
        if release['sdk']:
            lines += ['', 'SDK：' + ' · '.join(link(name, 'sdk/' + name) for name in release['sdk'])]
        if release.get('sdk_source'):
            lines += ['', 'SDK sdist：' + ' · '.join(link(name, 'sdk/' + name) for name in release['sdk_source'])]
        if release.get('admin'):
            lines += ['', 'adxadmin：' + ' · '.join(link(name, 'admin/' + name) for name in release['admin'])]
        if release.get('execd'):
            lines += ['', link('独立 Execd 包', 'adx-execd.tar.gz') + ' · ' + link('Execd SHA256', 'adx-execd.tar.gz.sha256')]
    images = result.get('images')
    if images:
        lines += ['', '### 镜像', '', '| 镜像 | 拉取地址（固定 digest） |', '|---|---|']
        for role, ref in sorted(images['references'].items()):
            lines.append(f'| {role} | {code(ref)} |')
        lines += ['', link('镜像清单', 'bundle/registry-images.json') + ' · ' + link('构建来源', 'bundle/bundle.json'),
                  '', 'sandboxd revision：' + code(images['backend'])]
    e2e = result.get('e2e')
    if e2e:
        report = e2e['report']
        lines += ['', '### Kubernetes 验收', '']
        if report:
            lines += ['结果：' + code(report['status']) + '；场景：' + ', '.join(report['checks']),
                      '', '清理错误数：' + str(len(report['cleanup_errors'])) + '；缺失场景数：' + str(len(report['missing_checks'])),
                      '', link('result.json', 'acceptance/result.json') + ' · ' + link('JUnit', 'acceptance/junit.xml')]
            if report['error']:
                lines += ['', '错误：' + code(report['error'])]
        else:
            lines += ['验收报告尚未生成，请查看本阶段日志。']
        for node, checks in e2e.get('collection', {}).items():
            evidence = checks.get('collection')
            if evidence:
                lines += ['', f"{node} 日志采集：{code(evidence['status'])}；唯一记录：{evidence.get('unique_probe_records', 0)}/40；" +
                          link('采集与故障恢复结果', f'acceptance/{node}/collection-{node}.json') + ' · ' +
                          link('Gateway 指标', f'acceptance/{node}/gateway-metrics-{node}.json'),
                          link('Trace 关联', f'acceptance/{node}/traces-{node}.json')]
        placement = e2e['placement']
        if placement:
            lines += ['', '| Pod | 宿主节点 | Pod IP |', '|---|---|---|']
            lines += [f"| {code(p['pod'])} | {code(p['host'])} | {code(p['ip'])} |" for p in placement]
            if len({p['host'] for p in placement}) == 1:
                lines += ['', '**本轮两个 Pod 位于同一宿主节点；未覆盖跨宿主机网络。**']
            lines += ['', link('部署落点', 'acceptance/placement.json')]
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', choices=('release', 'images', 'e2e'), required=True)
    parser.add_argument('--exit-code', type=int, required=True)
    parser.add_argument('--root', type=Path, default=Path('out/buildkite'))
    args = parser.parse_args()
    commit = os.environ['BUILDKITE_COMMIT']
    result = collect(args.root, args.stage, args.exit_code, commit,
                     artifact_build=os.environ.get('ADX_E2E_ARTIFACT_BUILD'))
    output = args.root / 'summaries'
    output.mkdir(parents=True, exist_ok=True)
    (output / f'{args.stage}.json').write_text(json.dumps(result, indent=2) + '\n')
    (output / f'{args.stage}.md').write_text(render(result))
    print('Build summary: ' + str(output / f'{args.stage}.md'), flush=True)


if __name__ == '__main__':
    main()
