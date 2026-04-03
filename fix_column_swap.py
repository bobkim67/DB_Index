"""DBO/SC/IC에 할인율/임금상승률/듀레이션이 잘못 들어간 38건 재추출

원인: LLM이 가정 테이블 값을 금액 변수로 오분류
판별: DBO > 0 AND DBO < 100 (비율값 혼입)
처리: pension_extracts JSON에서 다시 LLM 추출 → merged CSV 업데이트
"""
import pandas as pd
import numpy as np
import json, os, sys, time, re
import anthropic
sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')
from dart_llm_batch import call_llm, AMOUNT_VARS, RANGE_VARS, ALL_VAR_COLUMNS

print = __builtins__.__dict__['print']  # restore original print

# 대상 식별
targets = []
for year in range(2014, 2026):
    df = pd.read_csv(f'llm_extract_{year}_merged.csv', dtype={'corp_code': str})
    df['corp_code'] = df['corp_code'].str.zfill(8)
    ok = df[df['status'] == 'OK']
    suspect = ok[ok['DBO'].notna() & (ok['DBO'] > 0) & (ok['DBO'] < 100)]
    for idx, r in suspect.iterrows():
        targets.append({
            'year': year,
            'corp_code': r['corp_code'],
            'corp_name': r['corp_name'],
            'idx': idx,  # original df index
            'old_DBO': r['DBO'],
        })

print(f'재추출 대상: {len(targets)}건\n')

total_in, total_out = 0, 0
n_ok, n_fail = 0, 0
results = []

for i, t in enumerate(targets):
    year = t['year']
    cc = t['corp_code']
    name = t['corp_name']

    # pension_extracts JSON 로드
    json_path = os.path.join('pension_extracts', str(year), f'{cc}.json')
    if not os.path.exists(json_path):
        print(f'  [{i+1}/{len(targets)}] {year} {name}: JSON 없음, skip')
        n_fail += 1
        continue

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if data.get('n_tables', 0) == 0:
        print(f'  [{i+1}/{len(targets)}] {year} {name}: 테이블 없음, skip')
        n_fail += 1
        continue

    tables_text = '\n\n'.join(data['tables'])

    try:
        llm_result, usage = call_llm(tables_text, year)
        total_in += usage['input_tokens']
        total_out += usage['output_tokens']

        new_dbo = llm_result.get('DBO')
        new_dr = llm_result.get('DiscountRate_Mid')

        # 검증: 새 DBO가 정상 범위인지 (> 1000 이상이어야 원 단위)
        if new_dbo and new_dbo > 1000:
            status = 'FIXED'
            n_ok += 1
        elif new_dbo and new_dbo < 100:
            status = 'STILL_BAD'
            n_fail += 1
        else:
            status = 'NULL_DBO'
            n_ok += 1  # DBO=null이면 해당 기업은 퇴직연금 없음

        results.append({
            'year': year,
            'corp_code': cc,
            'corp_name': name,
            'old_DBO': t['old_DBO'],
            'new_DBO': new_dbo,
            'new_DR': new_dr,
            'status': status,
            'llm_result': llm_result,
        })

        print(f'  [{i+1}/{len(targets)}] {year} {name}: '
              f'old_DBO={t["old_DBO"]:.2f} → new_DBO={new_dbo} new_DR={new_dr} [{status}]')

    except Exception as e:
        print(f'  [{i+1}/{len(targets)}] {year} {name}: 오류 {e}')
        n_fail += 1
        if '429' in str(e):
            time.sleep(30)

cost = total_in / 1e6 * 1.0 + total_out / 1e6 * 5.0
print(f'\n완료: OK={n_ok}, fail={n_fail}, cost=${cost:.2f}')

# merged CSV 업데이트
print('\nmerged CSV 업데이트 중...')
for year in range(2014, 2026):
    year_results = [r for r in results if r['year'] == year and r['status'] in ('FIXED', 'NULL_DBO')]
    if not year_results:
        continue

    csv_path = f'llm_extract_{year}_merged.csv'
    df = pd.read_csv(csv_path, dtype={'corp_code': str})
    df['corp_code'] = df['corp_code'].str.zfill(8)

    for r in year_results:
        mask = df['corp_code'] == r['corp_code']
        if not mask.any():
            continue

        idx = df[mask].index[0]
        llm = r['llm_result']

        # 금액 변수 업데이트
        for var in AMOUNT_VARS:
            df.loc[idx, var] = llm.get(var)

        # 범위 변수 업데이트
        for var in RANGE_VARS:
            df.loc[idx, f'{var}_Min'] = llm.get(f'{var}_Min')
            df.loc[idx, f'{var}_Max'] = llm.get(f'{var}_Max')
            df.loc[idx, f'{var}_Mid'] = llm.get(f'{var}_Mid')

    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f'  {year}: {len(year_results)}건 업데이트 → {csv_path}')

print('\n전체 완료')
