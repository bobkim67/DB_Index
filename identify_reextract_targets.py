"""전 연도 no_match/비대칭/부호반전 재추출 대상 식별"""
import pandas as pd
import numpy as np
import json, os, sys, pickle

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from validate_2014_2017 import find_sensitivity_tables, extract_numbers_from_text, validate_sensitivity_match

all_targets = {}

for year in range(2014, 2026):
    csv_path = f'llm_extract_{year}_merged.csv'
    ext_dir = f'pension_extracts/{year}'
    df = pd.read_csv(csv_path, dtype={'corp_code': str})
    df['corp_code'] = df['corp_code'].str.zfill(8)
    ok = df[df['status'] == 'OK'].copy()

    targets = set()
    reasons = {}

    # === no_match 식별 ===
    has_sens = ok['SensitivityDR_1pct'].notna()
    for idx in ok[has_sens].index:
        corp_code = ok.loc[idx, 'corp_code']
        json_path = os.path.join(ext_dir, f'{corp_code}.json')
        if not os.path.exists(json_path):
            continue
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        sens_tables = find_sensitivity_tables(data.get('tables', []))
        if not sens_tables:
            continue
        numbers = extract_numbers_from_text(' '.join(sens_tables))
        dbo = ok.loc[idx, 'DBO']

        any_nomatch = False
        for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down',
                     'SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
            val = ok.loc[idx, col]
            if pd.notna(val):
                result = validate_sensitivity_match(val, dbo, numbers)
                if result == 'no_match':
                    any_nomatch = True
                    break

        if any_nomatch:
            targets.add(corp_code)
            reasons.setdefault(corp_code, []).append('no_match')

    # === 비대칭 식별 ===
    has_both = ok['SensitivityDR_1pct'].notna() & ok['SensitivityDR_1pct_down'].notna()
    if has_both.any():
        dr_up = ok.loc[has_both, 'SensitivityDR_1pct'].abs()
        dr_down = ok.loc[has_both, 'SensitivityDR_1pct_down'].abs()
        ratio = dr_up / dr_down.replace(0, np.nan)
        mask_asym = (ratio > 2) | (ratio < 0.5)
        for idx in ratio[mask_asym].index:
            cc = ok.loc[idx, 'corp_code']
            targets.add(cc)
            reasons.setdefault(cc, []).append('asymmetric')

    # === 부호반전 식별 ===
    mask1 = ok['SensitivityDR_1pct'].notna() & (ok['SensitivityDR_1pct'] > 0)
    for idx in ok[mask1].index:
        cc = ok.loc[idx, 'corp_code']
        targets.add(cc)
        reasons.setdefault(cc, []).append('sign_flip_DR_up')
    mask2 = ok['SensitivityDR_1pct_down'].notna() & (ok['SensitivityDR_1pct_down'] < 0)
    for idx in ok[mask2].index:
        cc = ok.loc[idx, 'corp_code']
        targets.add(cc)
        reasons.setdefault(cc, []).append('sign_flip_DR_down')
    mask3 = ok['SensitivitySG_1pct'].notna() & (ok['SensitivitySG_1pct'] < 0)
    for idx in ok[mask3].index:
        cc = ok.loc[idx, 'corp_code']
        targets.add(cc)
        reasons.setdefault(cc, []).append('sign_flip_SG_up')
    mask4 = ok['SensitivitySG_1pct_down'].notna() & (ok['SensitivitySG_1pct_down'] > 0)
    for idx in ok[mask4].index:
        cc = ok.loc[idx, 'corp_code']
        targets.add(cc)
        reasons.setdefault(cc, []).append('sign_flip_SG_down')

    n_nomatch = sum(1 for v in reasons.values() if 'no_match' in v)
    n_asym = sum(1 for v in reasons.values() if 'asymmetric' in v)
    n_sign = sum(1 for v in reasons.values() if any('sign_flip' in r for r in v))

    all_targets[year] = list(targets)
    print(f'{year}: 재추출 대상 {len(targets)}사 (no_match={n_nomatch}, asym={n_asym}, sign={n_sign})')

total = sum(len(t) for t in all_targets.values())
print(f'\n총 재추출 대상: {total}사')
print(f'예상 비용: ~${total * 0.003:.1f} (민감도 경량 프롬프트)')

# pickle로 저장
with open('reextract_targets.pkl', 'wb') as f:
    pickle.dump(all_targets, f)
print('reextract_targets.pkl 저장 완료')
