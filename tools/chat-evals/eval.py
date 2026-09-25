#!/usr/bin/env python3
"""Fixed-snapshot chat evaluations. Python stdlib + DuckDB CLI only."""
import argparse
import hashlib
import json
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from answer_quality import score_quality

ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT / 'data/southfork-2023-2025.duckdb'
TABLES = ('posts', 'qsource_posts', 'pax', 'aos', 'historical_totals')


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def query(db, sql):
    result = subprocess.run(['duckdb', '-readonly', '-json', str(db), '-c', sql],
                            check=True, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout or '[]')


def read(name):
    return json.loads((ROOT / name).read_text())


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


def fingerprint(db):
    # Logical rows, not database-file bytes (which vary by DuckDB version).
    result = {}
    for table in TABLES:
        rows = query(db, f'SELECT * FROM {table} ORDER BY ALL')
        encoded = json.dumps(rows, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        result[table] = {'rows': len(rows), 'sha256': hashlib.sha256(encoded).hexdigest()}
    return result


def freeze(source, fixture):
    if fixture.exists():
        raise ValueError(f'Fixture already exists: {fixture}; refusing to replace it')
    fixture.parent.mkdir(parents=True, exist_ok=True)
    temporary = fixture.with_suffix('.building.duckdb')
    if temporary.exists():
        raise ValueError(f'Remove stale temporary fixture first: {temporary}')
    statements = [f'ATTACH {sql_string(source.resolve())} AS original (READ_ONLY)']
    for table in TABLES + ('import_metadata',):
        scope = " WHERE date >= DATE '2023-01-01' AND date < DATE '2026-01-01'" if table in ('posts', 'qsource_posts') else ''
        statements.append(f'CREATE TABLE {table} AS SELECT * FROM original.{table}{scope}')
    for view in query(source, "SELECT sql FROM duckdb_views() WHERE NOT internal ORDER BY view_name"):
        statements.append(view['sql'].rstrip(';'))
    try:
        subprocess.run(['duckdb', str(temporary), '-c', ';\n'.join(statements) + ';'],
                       check=True, capture_output=True, text=True, timeout=60)
        manifest_path = ROOT / 'manifest.json'
        actual = fingerprint(temporary)
        if manifest_path.exists() and actual != read('manifest.json')['tables']:
            raise ValueError('Source does not reproduce the committed fixture fingerprint. Obtain the original fixture; do not silently update goldens.')
        temporary.replace(fixture)
    finally:
        temporary.unlink(missing_ok=True)


def verify(fixture):
    manifest = read('manifest.json')
    if fingerprint(fixture) != manifest['tables']:
        raise ValueError('Fixture fingerprint mismatch; evaluation aborted')
    cases = read('cases.json')
    goldens = read('expected.json')
    if set(goldens) != {case['id'] for case in cases}:
        raise ValueError('Case IDs and expected result IDs differ')
    for case in cases:
        actual = query(fixture, case['sql'])
        if actual != goldens[case['id']]:
            raise ValueError(f"Baseline changed: {case['id']}; review SQL/data and version goldens explicitly")
    print(f'PASS: fingerprint and {len(cases)} deterministic SQL baselines', flush=True)
    return cases, goldens


def canonical_rows(rows):
    # Ignore column aliases, retaining requested column order and row ranking.
    return [[round(v, 6) if isinstance(v, float) else v for v in row] for row in rows]


def score(case, expected, response):
    queries = response.get('queries', [])
    if not queries:
        return False, 'No executed query returned'
    last = queries[-1]
    if last.get('truncated'):
        return False, 'Final result truncated'
    wanted = canonical_rows([list(row.values()) for row in expected])
    actual = canonical_rows(last.get('rows', []))
    views = response.get('visualizations', [])
    if not views or views[-1].get('kind') != 'table':
        return False, 'Missing user-facing result table (final query may have failed)'
    if views[-1].get('truncated') or canonical_rows(views[-1].get('rows', [])) != wanted:
        return False, 'User-facing table differs from expected result'
    if not response.get('answer', '').strip():
        return False, 'Missing narrative answer'
    return actual == wanted, 'Exact result values/order; aliases ignored' if actual == wanted else 'Result mismatch (review column order and report)'


def source_hashes():
    provenance = {}
    for name in ('cases.json', 'expected.json', 'eval.py', 'answer_quality.py',
                 '../../F3Lambda/Analytics/AnalyticsChatService.cs',
                 '../../F3Lambda/Analytics/ChatAnswerFormatting.cs'):
        path = ROOT / name
        if path.exists():
            provenance[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return provenance


def evaluate(case, expected, response):
    data_passed, reason = score(case, expected, response)
    quality = score_quality(case, expected, response)
    checks_passed = data_passed and quality['presentation']['passed'] and quality['prose']['status'] != 'fail'
    return {'data': {'passed': data_passed, 'reason': reason}, **quality,
            'automated_checks_passed': checks_passed,
            'status': 'needs_review' if checks_passed else 'fail'}


def summarize(results):
    summary = []
    for model in dict.fromkeys(r['model'] for r in results):
        rows = [r for r in results if r['model'] == model]
        summary.append({
            'model': model, 'total': len(rows),
            'data_passed': sum(r.get('evaluation', {}).get('data', {}).get('passed', False) for r in rows),
            'presentation_passed': sum(r.get('evaluation', {}).get('presentation', {}).get('passed', False) for r in rows),
            'prose_failed': sum(r.get('evaluation', {}).get('prose', {}).get('status') == 'fail' for r in rows),
            'prose_needs_review': sum(r.get('evaluation', {}).get('prose', {}).get('status') == 'needs_review' for r in rows),
            'automated_checks_passed': sum(r.get('passed', False) for r in rows),
            'errors': sum('error' in r for r in rows),
            'median_seconds': statistics.median(r['seconds'] for r in rows)})
    return summary


def rescore(args, cases, goldens):
    report = json.loads(args.input.read_text())
    by_id = {c['id']: c for c in cases}
    results = [r for r in report['results'] if not args.model or r['model'] in args.model]
    if not results:
        raise ValueError('No matching model results')
    fixture_sha256 = hashlib.sha256(args.fixture.read_bytes()).hexdigest()
    for item in results:
        if 'response' in item:
            if item['response'].get('snapshot', {}).get('sha256') != fixture_sha256:
                raise ValueError('Stored response snapshot differs from current frozen fixture')
            case = by_id[item['case']]
            expected = goldens[case['id']]
            if item.get('expected') != expected:
                raise ValueError('Stored expected rows differ from current goldens')
            item['evaluation'] = evaluate(case, expected, item['response'])
            item['passed'] = item['evaluation']['automated_checks_passed']
    report.update(results=results, summary=summarize(results), scoring_version=2,
                  rescored_from=str(args.input), scoring_source_sha256=source_hashes(),
                  pass_semantics='Automated checks only; prose still requires semantic review.')
    write(args.report, report)
    print(json.dumps(report['summary'], indent=2))
    return 0 if all(r['passed'] for r in results) else 1


def run(args, cases, goldens):
    selected = [c for c in cases if not args.case or c['id'] in args.case]
    if not selected:
        raise ValueError('No matching cases')
    results = []
    provenance = source_hashes()
    fixture_sha256 = hashlib.sha256(args.fixture.read_bytes()).hexdigest()
    for model in args.model:
        for repeat in range(args.repeat):
            for case in selected:
                start = time.monotonic()
                item = {'model': model, 'repeat': repeat + 1, 'case': case['id'], 'passed': False}
                try:
                    body = {'messages': case['messages'], 'model': model, 'region': 'southfork'}
                    request = urllib.request.Request(args.url, data=json.dumps(body).encode(),
                                                     headers={'Content-Type': 'application/json'})
                    with urllib.request.urlopen(request, timeout=args.timeout) as response:
                        payload = json.load(response)
                    if payload.get('snapshot', {}).get('sha256') != fixture_sha256:
                        raise ValueError('Backend snapshot SHA256 does not match local frozen fixture; set F3_ANALYTICS_DB_PATH and restart backend')
                    if payload.get('model') != model:
                        raise ValueError('Backend response model does not match requested model')
                    evaluation = evaluate(case, goldens[case['id']], payload)
                    passed = evaluation['automated_checks_passed']
                    reason = evaluation['data']['reason']
                    item.update(passed=passed, reason=reason, response=payload, evaluation=evaluation,
                                expected=goldens[case['id']])
                except urllib.error.HTTPError as exc:
                    item['error'] = str(exc)
                    item['http_status'] = exc.code
                    body = exc.read(32768).decode('utf-8', errors='replace')
                    try:
                        item['error_response'] = json.loads(body)
                    except ValueError:
                        item['error_response'] = body
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    item['error'] = str(exc)
                item['seconds'] = round(time.monotonic() - start, 3)
                results.append(item)
                print(f"{model} {case['id']}: {'CHECKS PASS / REVIEW' if item['passed'] else 'FAIL'} ({item['seconds']}s)", flush=True)
    summary = summarize(results)
    report = {'scoring_version': 2, 'source_sha256': provenance,
              'pass_semantics': 'Automated checks only; prose still requires semantic review.',
              'fixture_manifest': read('manifest.json'), 'endpoint': args.url,
              'summary': summary, 'results': results,
              'human_review': ['Narrative agrees with query values and ranking.',
                               'Dates, counting rules, exclusions, and snapshot freshness are explicit.',
                               'No invented causes, personal inferences, or unreturned statistics.',
                               'Follow-up retains scope and applies the requested change.',
                               'SQL supports the answer; errors are not presented as facts.']}
    write(args.report, report)
    print(json.dumps(summary, indent=2))
    print(f'Report: {args.report}')
    return 0 if all(r['passed'] for r in results) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, default=FIXTURE)
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('freeze', help='Create ignored historical fixture; never overwrites or changes goldens')
    create.add_argument('--source', type=Path, default=ROOT.parent / 'southfork-duckdb/data/southfork.duckdb')
    commands.add_parser('verify', help='Check fingerprint and reference SQL without an API key')
    execute = commands.add_parser('run', help='Call local chat API with each requested model')
    execute.add_argument('--model', action='append', required=True, help='Repeat for multiple OpenRouter model IDs')
    execute.add_argument('--url', default='http://localhost:5055/chat')
    execute.add_argument('--case', action='append')
    execute.add_argument('--repeat', type=int, default=1)
    execute.add_argument('--timeout', type=float, default=180)
    execute.add_argument('--report', type=Path, default=ROOT / 'reports/latest.json')
    rescore_parser = commands.add_parser('rescore', help='Score saved responses without calling a model')
    rescore_parser.add_argument('--input', type=Path, required=True)
    rescore_parser.add_argument('--report', type=Path, required=True)
    rescore_parser.add_argument('--model', action='append')
    args = parser.parse_args()
    try:
        if args.command == 'freeze':
            freeze(args.source, args.fixture)
            print(f'Fixture: {args.fixture}')
            return 0
        cases, goldens = verify(args.fixture)
        if args.command == 'verify':
            return 0
        if args.command == 'rescore':
            return rescore(args, cases, goldens)
        if args.repeat < 1:
            raise ValueError('--repeat must be at least 1')
        return run(args, cases, goldens)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
