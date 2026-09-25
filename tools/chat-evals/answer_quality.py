"""Conservative, deterministic presentation checks and selected prose checks.

No LLM judge and no claim of comprehensive factual verification. A clean answer
still needs human review. Checks intentionally favor explicit, narrow claims.
"""
import calendar
import re
from collections import Counter

WORD_COUNTS = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
               'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}
MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}


def score_quality(case, expected, response):
    answer = response.get('answer', '')
    answer = answer if isinstance(answer, str) else ''
    presentation = []
    prose = []
    review = ['Human review required: these rules do not verify every claim, omission, or implication.']
    if not answer.strip():
        presentation.append('empty_answer')
    if re.search(r'\bSELECT\s+(?:DISTINCT\s+)?[\s\S]{1,180}?\bFROM\s+\w+|\bWITH\s+\w+\s+AS\s*\(|\b(?:INSERT\s+INTO|DELETE\s+FROM|DROP\s+TABLE)\b', answer, re.I):
        presentation.append('leaked_sql')
    if re.search(r'```|`[^`\n]+`|\*\*[^\n]+?\*\*|__[^\n]+?__|(?m:^\s{0,3}(?:#{1,6}\s|[-*+]\s|\d+[.)]\s|>\s))|\[[^\]]+\]\([^)]+\)', answer):
        presentation.append('markdown_formatting')
    lines = [line.strip() for line in answer.splitlines() if line.strip()]
    if sum(line.count('|') >= 2 for line in lines) >= 2:
        presentation.append('duplicate_pipe_table')
    # Two tabular rows, each with a label and numeric cells separated by tabs or
    # repeated spaces. Single ordinary sentences and a single ranked fact are OK.
    if sum(bool(re.search(r'\S(?:\t+| {2,})\d[\d,.]*(?:\s|$)', line)) for line in lines) >= 2:
        presentation.append('duplicate_spaced_table')
    # Also catch compact rows rendered with just one space (e.g. "The Way 43").
    row_lines = 0
    for line in lines:
        for row in expected:
            values = list(row.values())
            if len(values) >= 2:
                label = re.escape(str(values[0]))
                tail = r'\s+'.join(re.escape(str(value)) for value in values[1:])
                if re.fullmatch(r'(?:\d+[.)]?\s+)?' + label + r':?\s+' + tail, line, re.I):
                    row_lines += 1
                    break
    if row_lines >= 2 and 'duplicate_spaced_table' not in presentation:
        presentation.append('duplicate_spaced_table')
    if len(answer.split()) > 250:
        presentation.append('excessive_narrative_over_250_words')

    case_id = case.get('id', '')
    # Narrow ranking comparison: 'more than double any other/next' requires
    # the leader to exceed twice the runner-up, not merely lead the ranking.
    plain = re.sub(r'[*_`]', '', answer)
    if case_id in ('fng_aos_2025', 'peacock_aos_2025') and len(expected) >= 2:
        metric = 'fngs' if case_id == 'fng_aos_2025' else 'posts'
        if re.search(r'\bmore than double (?:any other|(?:his |their |the )?next)', plain, re.I):
            if expected[0][metric] <= 2 * expected[1][metric]:
                prose.append('ranking_ratio_contradiction: leader does not exceed twice the runner-up')
    if case_id == 'peacock_jan_aug' and re.search(r'\bhalf[- ]year\b', plain, re.I):
        prose.append('period_contradiction: January through August is eight months, not half a year')
    if case_id == 'q_leaders_2023':
        counts = Counter(row['qs'] for row in expected)
        pattern = r'\b(one|two|three|four|five|six|seven|eight|nine|ten|\d+)\s+(?:(?:PAX|people|participants|members|leaders)\s+)?(?:are\s+|were\s+)?tied\s+(?:at|with|on)\s+(\d+)\b'
        for match in re.finditer(pattern, answer, re.I):
            count_text, value = match.groups()
            claimed = WORD_COUNTS.get(count_text.lower(), int(count_text) if count_text.isdigit() else None)
            actual = counts[int(value)]
            enumeration = re.match(r'\s*(?:Qs?\s*)?(?:each\s*)?\(([^)]+)\)', answer[match.end():], re.I)
            if enumeration:
                group = enumeration.group(1)
                # Only treat a plain, fully enumerated list as exhaustive. An
                # 'including'/'e.g.' list may legitimately name just a subset.
                names = [row['pax'] for row in expected if row['qs'] == int(value)]
                remainder = group
                mentioned = []
                for name in sorted(names, key=len, reverse=True):
                    name_pattern = r'(?<!\w)' + re.escape(name) + r'(?!\w)'
                    if re.search(name_pattern, remainder, re.I):
                        mentioned.append(name)
                        remainder = re.sub(name_pattern, '', remainder, flags=re.I)
                remainder = re.sub(r'\band\b|[\s,&;]', '', remainder, flags=re.I)
                if mentioned and not remainder and claimed != len(mentioned):
                    prose.append(f'enumerated_tie_count_contradiction: group says {claimed} but lists {len(mentioned)} distinct PAX')
            # Only disprove undercounts: there may be additional ties outside
            # the top-10 result, which cannot disprove a claimed larger group.
            if actual and claimed < actual:
                prose.append(f'tie_count_contradiction: at least {actual} returned PAX have {value} Qs; answer says {claimed}')
            elif claimed > actual:
                review.append(f'Tie claim ({claimed} at {value}) may include people outside the returned top 10; verify.')

    if case_id == 'monthly_2024':
        for sentence in re.split(r'(?<=[.!?])\s+|\n', answer):
            lower = sentence.lower()
            months = [number for name, number in MONTHS.items() if re.search(r'\b' + name + r'\b', lower)]
            highs = bool(re.search(r'\b(highest|most|peak(?:ed)?|busiest|maximum|strongest)\b', lower))
            lows = bool(re.search(r'\b(lowest|fewest|minimum|quietest|least)\b', lower))
            if re.search(r"\b(not|never|wasn't|isn't|didn't)\b", lower):
                continue
            if len(months) != 1 or highs == lows:
                continue
            # A scoped comparison is not an annual-extreme claim.
            if re.search(r'\b(summer|winter|spring|fall|autumn|quarter|half|among|between|excluding|except|second|third|fourth|improved|improvement|growth|increase|decrease|than)\b', lower):
                review.append('Scoped monthly comparison requires human verification.')
                continue
            metrics = []
            if re.search(r'\b(posts|attendance)\b', lower): metrics.append('posts')
            if re.search(r'\bworkouts\b', lower): metrics.append('workouts')
            if re.search(r'\bunique\s+pax\b', lower): metrics.append('unique_pax')
            if len(metrics) != 1:
                review.append('Monthly extreme has ambiguous metric; verify intended measure.')
                continue
            metric = metrics[0]
            extreme = (max if highs else min)(row[metric] for row in expected)
            winners = [row['month'] for row in expected if row[metric] == extreme]
            if months[0] not in winners:
                prose.append(f'monthly_extreme_contradiction: {metric} {"maximum" if highs else "minimum"} is in ' + ', '.join(calendar.month_name[m] for m in winners))
    if case_id == 'no_data_2020':
        # This case explicitly requests a snapshot limitation, not a claim that
        # no workouts happened. Detection is conservative and lexical.
        scoped = re.search(r'\b(snapshot|database|dataset|available data|recorded data|coverage)\b', answer, re.I)
        if not scoped:
            prose.append('missing_snapshot_scope: zero stored records must be scoped to the available snapshot')
        if re.search(r'\bno workouts (?:happened|occurred|took place)\b', answer, re.I) and not re.search(r"\b(not|cannot|can't|doesn't|does not)\b", answer, re.I):
            prose.append('unsupported_no_workouts_claim: absence of snapshot rows does not prove absence of workouts')
    if case_id == 'fastest_blocks_2023_2025' and re.search(r'\b(?:two|2) calendar years(?:[’\']? worth)?\b', answer, re.I):
        review.append('Calendar-years interpretation is unsupported by the milestone table; verify the exact elapsed days and 2023–2025 scope.')
    if re.search(r'\b(because|caused by|due to|likely reflects|probably reflects|suggests (?:he|she|they))\b', answer, re.I):
        review.append('Causal or personal interpretation needs human review; attendance alone may not support it.')
    return {'presentation': {'passed': not presentation, 'issues': presentation},
            'prose': {'status': 'fail' if prose else 'needs_review',
                      'issues': prose, 'review_reasons': list(dict.fromkeys(review))}}
