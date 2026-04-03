"""no_match/비대칭/부호반전 건 민감도 재추출 → merged CSV 반영"""
import pandas as pd
import numpy as np
import json, os, sys, pickle, time, re
import anthropic

sys.stdout.reconfigure(encoding='utf-8')
# flush stdout for background execution
import functools
print = functools.partial(print, flush=True)
sys.path.insert(0, '.')
from validate_2014_2017 import find_sensitivity_tables, extract_numbers_from_text, validate_sensitivity_match, detect_shock

API_KEY = os.environ.get('CLAUDE_API_KEY', '')
MODEL = 'claude-haiku-4-5-20251001'
client = anthropic.Anthropic(api_key=API_KEY)

PROMPT = """아래는 한국 기업 퇴직연금 주석의 민감도 분석 테이블입니다.

이 테이블에서 다음 6개 값을 추출하세요:
1. DR_shock: 할인율 변동폭 (%p 단위, 예: 1 또는 0.5)
2. SensitivityDR_up: 할인율 증가시 확정급여채무 변동액. 감소하면 음수. 원 단위 정수.
3. SensitivityDR_down: 할인율 감소시 확정급여채무 변동액. 증가하면 양수. 원 단위 정수.
4. SG_shock: 임금상승률 변동폭 (%p 단위, 예: 1 또는 0.5)
5. SensitivitySG_up: 임금상승률 증가시 확정급여채무 변동액. 증가하면 양수. 원 단위 정수.
6. SensitivitySG_down: 임금상승률 감소시 확정급여채무 변동액. 감소하면 음수. 원 단위 정수.

규칙:
- 금액은 원 단위 정수. 백만원/천원 단위면 곱하여 원으로 변환. (단위) 표기 참고.
- 비율(%)로 공시된 경우(예: -5.71%): DBO × 비율/100으로 변환. DBO가 없으면 비율 그대로 반환하고 키 앞에 "pct_"를 붙이세요.
- 변동폭이 1%p(또는 1%, 0.01)이면 DR_shock=1. 0.5%p(또는 0.5%, 0.005)이면 DR_shock=0.5.
- 값이 없으면 null.
- 당기 값만 추출. 전기 값은 무시.
- JSON만 반환. 설명 텍스트 없이 JSON만.

{"DR_shock": ..., "SensitivityDR_up": ..., "SensitivityDR_down": ..., "SG_shock": ..., "SensitivitySG_up": ..., "SensitivitySG_down": ...}
"""


def call_llm(tables_text, dbo=None):
    user_msg = tables_text
    if dbo and dbo > 0:
        user_msg = f"[DBO(확정급여채무) = {int(dbo):,}원]\n\n" + user_msg
    resp = client.messages.create(
        model=MODEL, max_tokens=300, system=PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text.strip()
    text = re.sub(r'```\w*', '', text).strip()
    brace_start = text.find('{')
    brace_end = text.find('}', brace_start) if brace_start >= 0 else -1
    if brace_start >= 0 and brace_end >= 0:
        json_str = re.sub(r'(?<=\d),(?=\d)', '', text[brace_start:brace_end+1])
        data = json.loads(json_str)
        if dbo and dbo > 0:
            for key in list(data.keys()):
                val = data[key]
                if isinstance(val, str) and val.startswith('pct_'):
                    try:
                        pct = float(val.replace('pct_', '')) / 100
                        data[key] = int(dbo * pct)
                    except ValueError:
                        data[key] = None
        return data, resp.usage.input_tokens, resp.usage.output_tokens
    return None, resp.usage.input_tokens, resp.usage.output_tokens


def process_all(start_year=2014):
    with open('reextract_targets.pkl', 'rb') as f:
        all_targets = pickle.load(f)

    grand_total_in, grand_total_out = 0, 0
    grand_ok, grand_fail, grand_skip = 0, 0, 0
    grand_improved = 0

    for year in sorted(all_targets.keys()):
        if year < start_year:
            continue
        target_corps = set(all_targets[year])
        if not target_corps:
            continue

        csv_path = f'llm_extract_{year}_merged.csv'
        ext_dir = f'pension_extracts/{year}'
        df = pd.read_csv(csv_path, dtype={'corp_code': str})
        df['corp_code'] = df['corp_code'].str.zfill(8)

        ok_mask = df['status'] == 'OK'
        target_mask = ok_mask & df['corp_code'].isin(target_corps)
        targets_df = df[target_mask]

        print(f'\n=== {year}: {len(targets_df)}건 재추출 ===')
        total_in, total_out = 0, 0
        n_ok, n_fail, n_skip = 0, 0, 0
        n_improved = 0
        n_updated = 0

        for i, (idx, row) in enumerate(targets_df.iterrows()):
            corp_code = row['corp_code']
            json_path = os.path.join(ext_dir, f'{corp_code}.json')
            if not os.path.exists(json_path):
                n_skip += 1
                continue

            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            sens_tables = find_sensitivity_tables(data.get('tables', []))
            if not sens_tables:
                n_skip += 1
                continue

            tables_text = '\n\n'.join(sens_tables)
            dbo = row.get('DBO', None)

            try:
                result, in_tok, out_tok = call_llm(tables_text, dbo)
                total_in += in_tok
                total_out += out_tok
            except Exception as e:
                n_fail += 1
                print(f'  {row["corp_name"]}: 오류 {e}')
                if 'rate' in str(e).lower() or '429' in str(e):
                    time.sleep(30)
                continue

            if not result:
                n_fail += 1
                continue

            n_ok += 1

            # shock 환산 (0.5%p → ×2)
            for prefix, llm_prefix in [('DR', 'DR'), ('SG', 'SG')]:
                shock = result.get(f'{llm_prefix}_shock')
                if shock and float(shock) < 1:
                    mult = 1.0 / float(shock)
                    for suffix in ['up', 'down']:
                        key = f'Sensitivity{llm_prefix}_{suffix}'
                        val = result.get(key)
                        if val is not None and isinstance(val, (int, float)):
                            result[key] = int(val * mult)

            # 매칭 검증 (재추출 결과)
            numbers = extract_numbers_from_text(' '.join(sens_tables))
            new_vals = {
                'SensitivityDR_1pct': result.get('SensitivityDR_up'),
                'SensitivityDR_1pct_down': result.get('SensitivityDR_down'),
                'SensitivitySG_1pct': result.get('SensitivitySG_up'),
                'SensitivitySG_1pct_down': result.get('SensitivitySG_down'),
            }

            # 기존 값 vs 재추출 값 비교 → 매칭 개선 여부 확인
            improved = False
            for col, new_val in new_vals.items():
                if new_val is None:
                    continue
                old_val = row.get(col)
                old_match = validate_sensitivity_match(old_val, dbo, numbers) if pd.notna(old_val) else 'no_value'
                new_match = validate_sensitivity_match(new_val, dbo, numbers)

                # 새 값이 매칭되거나, 부호가 올바르면 업데이트
                should_update = False
                if old_match == 'no_match' and new_match.startswith('match_'):
                    should_update = True
                    improved = True
                elif old_match == 'no_match' and new_match == 'no_match':
                    # 둘 다 no_match면 부호 정상성 체크
                    if col == 'SensitivityDR_1pct' and isinstance(new_val, (int, float)) and new_val < 0 and (pd.isna(old_val) or old_val > 0):
                        should_update = True
                        improved = True
                    elif col == 'SensitivityDR_1pct_down' and isinstance(new_val, (int, float)) and new_val > 0 and (pd.isna(old_val) or old_val < 0):
                        should_update = True
                        improved = True
                    elif col == 'SensitivitySG_1pct' and isinstance(new_val, (int, float)) and new_val > 0 and (pd.isna(old_val) or old_val < 0):
                        should_update = True
                        improved = True
                    elif col == 'SensitivitySG_1pct_down' and isinstance(new_val, (int, float)) and new_val < 0 and (pd.isna(old_val) or old_val > 0):
                        should_update = True
                        improved = True

                if should_update:
                    df.loc[idx, col] = new_val
                    n_updated += 1

            if improved:
                n_improved += 1

            if (i + 1) % 50 == 0:
                cost = total_in / 1e6 * 1.0 + total_out / 1e6 * 5.0
                print(f'  [{i+1}/{len(targets_df)}] ok={n_ok} fail={n_fail} improved={n_improved} cost=${cost:.2f}')

        # shock_raw 업데이트
        for idx, row in df[target_mask].iterrows():
            corp_code = row['corp_code']
            json_path = os.path.join(ext_dir, f'{corp_code}.json')
            if os.path.exists(json_path):
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                dr_shock, sg_shock = detect_shock(data.get('tables', []))
                if dr_shock:
                    df.loc[idx, 'DR_shock_raw'] = dr_shock
                if sg_shock:
                    df.loc[idx, 'SG_shock_raw'] = sg_shock

        # merged CSV 업데이트 저장
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')

        cost = total_in / 1e6 * 1.0 + total_out / 1e6 * 5.0
        print(f'  완료: ok={n_ok} fail={n_fail} skip={n_skip} improved={n_improved} updated={n_updated} cost=${cost:.2f}')

        grand_total_in += total_in
        grand_total_out += total_out
        grand_ok += n_ok
        grand_fail += n_fail
        grand_skip += n_skip
        grand_improved += n_improved

    grand_cost = grand_total_in / 1e6 * 1.0 + grand_total_out / 1e6 * 5.0
    print(f'\n=== 전체 완료 ===')
    print(f'OK={grand_ok}, fail={grand_fail}, skip={grand_skip}, improved={grand_improved}')
    print(f'토큰: in={grand_total_in:,}, out={grand_total_out:,}')
    print(f'비용: ${grand_cost:.2f}')


if __name__ == '__main__':
    start_year = int(sys.argv[1]) if len(sys.argv) > 1 else 2014
    process_all(start_year=start_year)
