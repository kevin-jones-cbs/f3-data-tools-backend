"""Harness regression checks; no network or model credentials required."""
import importlib.util
import json
import io
import urllib.error
from types import SimpleNamespace
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('chat_eval', Path(__file__).with_name('eval.py'))
eval = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eval)


class HarnessTests(unittest.TestCase):
    def test_http_failure_keeps_diagnostic_body(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / 'fixture.duckdb'
            fixture.write_bytes(b'fixture')
            report = Path(directory) / 'report.json'
            args = SimpleNamespace(case=None, fixture=fixture, model=['test/model'], repeat=1,
                                   url='http://localhost/chat', timeout=1, report=report)
            body = b'{"error":"Query budget exhausted","code":"query_attempt_limit","attempts":[{"sql":"SELECT bad_column","error":"Unknown column"}]}'
            error = urllib.error.HTTPError(args.url, 502, 'Bad Gateway', {}, io.BytesIO(body))
            with patch.object(eval.urllib.request, 'urlopen', side_effect=error), patch.object(eval, 'read', return_value={}):
                self.assertEqual(eval.run(args, [{'id':'case', 'messages':[]}], {}), 1)
            result = json.loads(report.read_text())['results'][0]
            self.assertEqual(result['http_status'], 502)
            self.assertEqual(result['error_response']['code'], 'query_attempt_limit')
            self.assertEqual(result['error_response']['attempts'][0]['error'], 'Unknown column')

    def test_correct_table_does_not_hide_bad_presentation(self):
        response = {'answer': '| posts |\n| --- |\n| 3 |',
                    'queries': [{'rows': [[3]], 'truncated': False}],
                    'visualizations': [{'kind': 'table', 'rows': [[3]], 'truncated': False}]}
        result = eval.evaluate({'id': 'example'}, [{'posts': 3}], response)
        self.assertTrue(result['data']['passed'])
        self.assertFalse(result['presentation']['passed'])
        self.assertFalse(result['automated_checks_passed'])

    def test_checks_pass_is_still_pending_semantic_review(self):
        response = {'answer': 'There are 3 attendance posts in this result.',
                    'queries': [{'rows': [[3]], 'truncated': False}],
                    'visualizations': [{'kind': 'table', 'rows': [[3]], 'truncated': False}]}
        result = eval.evaluate({'id': 'example'}, [{'posts': 3}], response)
        self.assertTrue(result['automated_checks_passed'])
        self.assertEqual(result['status'], 'needs_review')
        self.assertEqual(result['prose']['status'], 'needs_review')

    def test_summary_separates_dimensions_and_transport_errors(self):
        evaluation = {'data': {'passed': True}, 'presentation': {'passed': False},
                      'prose': {'status': 'needs_review'}}
        rows = [{'model': 'luna', 'seconds': 2, 'passed': False, 'evaluation': evaluation},
                {'model': 'luna', 'seconds': 4, 'passed': False, 'error': 'HTTP 502'}]
        summary = eval.summarize(rows)[0]
        self.assertEqual(summary['data_passed'], 1)
        self.assertEqual(summary['presentation_passed'], 0)
        self.assertEqual(summary['prose_needs_review'], 1)
        self.assertEqual(summary['automated_checks_passed'], 0)
        self.assertEqual(summary['errors'], 1)
        self.assertEqual(summary['median_seconds'], 3)

    def test_rescore_uses_saved_answer_and_rejects_changed_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / 'fixture.duckdb'
            fixture.write_bytes(b'fixture')
            source = Path(directory) / 'old.json'
            output = Path(directory) / 'new.json'
            expected = [{'posts': 3}]
            response = {'answer': '**Three** posts.',
                        'snapshot': {'sha256': eval.hashlib.sha256(b'fixture').hexdigest()},
                        'queries': [{'rows': [[3]]}],
                        'visualizations': [{'kind': 'table', 'rows': [[3]]}]}
            source.write_text(json.dumps({'results': [{'model': 'luna', 'case': 'example',
                              'passed': True, 'seconds': 2, 'expected': expected, 'response': response}]}))
            args = SimpleNamespace(input=source, report=output, model=None, fixture=fixture)
            with patch.object(eval.urllib.request, 'urlopen') as network:
                self.assertEqual(eval.rescore(args, [{'id': 'example'}], {'example': expected}), 1)
                network.assert_not_called()
            result = json.loads(output.read_text())['results'][0]
            self.assertTrue(result['evaluation']['data']['passed'])
            self.assertFalse(result['passed'])
            fixture.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'snapshot differs'):
                eval.rescore(args, [{'id': 'example'}], {'example': expected})

    def test_aliases_are_ignored_but_ranking_is_not(self):
        expected = [{'ao': 'The Way', 'fngs': 43}, {'ao': 'The Grid', 'fngs': 20}]
        response = {'answer': 'Example', 'queries': [{'columns': ['location', 'count'],
                    'rows': [['The Way', 43], ['The Grid', 20]], 'truncated': False}],
                    'visualizations': [{'kind': 'table', 'rows': [['The Way', 43], ['The Grid', 20]], 'truncated': False}]}
        self.assertTrue(eval.score({}, expected, response)[0])
        response['queries'][0]['rows'].reverse()
        self.assertFalse(eval.score({}, expected, response)[0])

    def test_last_success_without_final_visualization_is_not_a_pass(self):
        response = {'answer': 'Could not complete query',
                    'queries': [{'rows': [[3]], 'truncated': False}], 'visualizations': []}
        self.assertFalse(eval.score({}, [{'posts': 3}], response)[0])

    def test_missing_truncated_and_incorrect_results_fail(self):
        self.assertFalse(eval.score({}, [], {'answer': 'Made up', 'queries': []})[0])
        response = {'answer': 'Example', 'queries': [{'rows': [[3]], 'truncated': True}],
                    'visualizations': [{'kind': 'table', 'rows': [[3]], 'truncated': False}]}
        self.assertFalse(eval.score({}, [{'posts': 3}], response)[0])
        response['queries'][0]['truncated'] = False
        self.assertFalse(eval.score({}, [{'posts': 4}], response)[0])
        response['answer'] = ''
        self.assertFalse(eval.score({}, [{'posts': 3}], response)[0])

    def test_changed_fixture_fails_before_queries_run(self):
        with patch.object(eval, 'read', return_value={'tables': {'posts': {'sha256': 'old'}}}), \
             patch.object(eval, 'fingerprint', return_value={'posts': {'sha256': 'new'}}), \
             patch.object(eval, 'query') as query:
            with self.assertRaisesRegex(ValueError, 'fingerprint mismatch'):
                eval.verify(Path('unused'))
            query.assert_not_called()

    def test_freeze_never_overwrites_existing_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / 'fixture.duckdb'
            fixture.write_bytes(b'original')
            with self.assertRaisesRegex(ValueError, 'already exists'):
                eval.freeze(Path('unused'), fixture)
            self.assertEqual(fixture.read_bytes(), b'original')


if __name__ == '__main__':
    unittest.main()
