import json
import unittest
from pathlib import Path
from answer_quality import score_quality

ROOT = Path(__file__).parent
EXPECTED = json.loads((ROOT / 'expected.json').read_text())


def check(text, case='q_leaders_2023'):
    return score_quality({'id': case}, EXPECTED[case], {'answer': text})


class AnswerQualityTests(unittest.TestCase):
    def test_numeric_rows_duplicate_the_real_table(self):
        result = check('Regular Q totals:\n2023: 10\n2024: 10\n2025: 11', 'peacock_followup_qs')
        self.assertIn('duplicate_spaced_table', result['presentation']['issues'])
        self.assertTrue(check('10 in 2023, 10 in 2024, and 11 in 2025.', 'peacock_followup_qs')['presentation']['passed'])

    def test_comparative_and_period_claims(self):
        self.assertEqual('fail', check('The Way led with 43, more than double any other location.', 'fng_aos_2025')['prose']['status'])
        self.assertEqual('needs_review', check('The Combine had 34, more than double the next AO.', 'peacock_aos_2025')['prose']['status'])
        self.assertEqual('fail', check('2023 was the busiest half-year window.', 'peacock_jan_aug')['prose']['status'])

    def test_clean_still_requires_human_review(self):
        result = check('Happy Tree led with 59 Qs. Three PAX tied at 25.')
        self.assertTrue(result['presentation']['passed'])
        self.assertEqual('needs_review', result['prose']['status'])
        self.assertEqual([], result['prose']['issues'])

    def test_tie_undercount(self):
        self.assertEqual('fail', check('Two PAX tied at 25.')['prose']['status'])
        self.assertEqual('needs_review', check('Four PAX tied at 25.')['prose']['status'])

    def test_ranked_monthly_comparison_needs_review(self):
        self.assertEqual('needs_review', check('August had the second highest posts.', 'monthly_2024')['prose']['status'])

    def test_explicit_named_tie_list_must_match_group_count(self):
        historical = 'a tight group of four PAX tied at 25 Qs each (Baby Ruth, Citation, Deuce)'
        self.assertEqual('fail', check(historical)['prose']['status'])
        self.assertEqual('needs_review', check('Four PAX tied at 25 (including Baby Ruth, Citation, Deuce).')['prose']['status'])
        self.assertEqual('needs_review', check('Three PAX tied at 25 (Baby Ruth, Citation and Deuce).')['prose']['status'])

    def test_zero_data_requires_snapshot_scope(self):
        self.assertEqual('fail', check('There were zero posts in 2020.', 'no_data_2020')['prose']['status'])
        self.assertEqual('needs_review', check('This snapshot contains zero records for 2020; it covers 2023–2025.', 'no_data_2020')['prose']['status'])
        self.assertEqual('fail', check('The database proves no workouts happened in 2020.', 'no_data_2020')['prose']['status'])

    def test_fastest_calendar_years_claim_needs_review(self):
        result = check("That is two calendar years worth of posts.", 'fastest_blocks_2023_2025')
        self.assertTrue(any('Calendar-years' in reason for reason in result['prose']['review_reasons']))

    def test_monthly_extremes(self):
        self.assertEqual('fail', check('August had the highest attendance.', 'monthly_2024')['prose']['status'])
        self.assertEqual('needs_review', check('December had the highest attendance.', 'monthly_2024')['prose']['status'])
        self.assertEqual('needs_review', check('October had the most workouts.', 'monthly_2024')['prose']['status'])
        self.assertEqual('needs_review', check('August had the most unique PAX.', 'monthly_2024')['prose']['status'])
        self.assertEqual('fail', check('March had the fewest posts.', 'monthly_2024')['prose']['status'])

    def test_scoped_extreme_not_annual_contradiction(self):
        result = check('August had the highest summer attendance.', 'monthly_2024')
        self.assertEqual('needs_review', result['prose']['status'])
        self.assertTrue(any('Scoped' in reason for reason in result['prose']['review_reasons']))

    def test_sql_markdown_and_tables(self):
        self.assertIn('leaked_sql', check('SELECT count(*) FROM posts')['presentation']['issues'])
        self.assertIn('markdown_formatting', check('**Happy Tree** led.')['presentation']['issues'])
        self.assertIn('duplicate_pipe_table', check('| PAX | Qs |\n| Happy Tree | 59 |')['presentation']['issues'])
        self.assertIn('duplicate_spaced_table', check('Happy Tree  59\nDouble Dip  54')['presentation']['issues'])
        self.assertIn('duplicate_spaced_table', check('Happy Tree 59\nDouble Dip 54')['presentation']['issues'])
        self.assertTrue(check('Happy Tree led with 59 Qs, followed by Double Dip with 54.')['presentation']['passed'])

    def test_excessive_and_empty_narrative(self):
        self.assertIn('excessive_narrative_over_250_words', check('word ' * 251)['presentation']['issues'])
        self.assertIn('empty_answer', check('')['presentation']['issues'])

    def test_personal_inference_is_review_not_automatic_conviction(self):
        result = check('Attendance dropped because he changed jobs.')
        self.assertEqual('needs_review', result['prose']['status'])
        self.assertTrue(any('Causal' in reason for reason in result['prose']['review_reasons']))


if __name__ == '__main__':
    unittest.main()
