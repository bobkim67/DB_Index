"""DR/SG 이상치 보정 — 전 연도 merged CSV에서 체계적으로 처리

1. 노무라/인바디: 전 연도 행 삭제
2. DR_Mid > 15%: null 처리 (해외법인 Min/Max 이상은 유지, Mid만 보정)
3. DR_Min > 15% & DR_Mid <= 15%: Min만 null 처리
"""
import pandas as pd
import numpy as np
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 전 연도 삭제 대상
DELETE_CORPS = ['01082834', '00269922']  # 노무라, 인바디

for year in range(2014, 2026):
    csv_path = f'llm_extract_{year}_merged.csv'
    df = pd.read_csv(csv_path, dtype={'corp_code': str})
    df['corp_code'] = df['corp_code'].str.zfill(8)
    n_before = len(df)

    # 1. 삭제 대상 제거
    mask_del = df['corp_code'].isin(DELETE_CORPS)
    n_del = mask_del.sum()
    if n_del > 0:
        deleted_names = df.loc[mask_del, 'corp_name'].tolist()
        df = df[~mask_del].copy()
        print(f'{year}: 삭제 {n_del}건 ({", ".join(deleted_names)})')

    # 2. DR_Mid > 15% → null (Min/Max도 같이 null)
    for prefix in ['DiscountRate']:
        mid_col = f'{prefix}_Mid'
        min_col = f'{prefix}_Min'
        max_col = f'{prefix}_Max'
        mask_mid = df[mid_col].notna() & (df[mid_col] > 15)
        n_mid = mask_mid.sum()
        if n_mid > 0:
            names = df.loc[mask_mid, 'corp_name'].tolist()
            vals = df.loc[mask_mid, mid_col].tolist()
            for name, val in zip(names, vals):
                print(f'  {year} {name}: {mid_col}={val:.1f}% → null')
            df.loc[mask_mid, [min_col, max_col, mid_col]] = np.nan

    # 3. DR_Min > 15% & DR_Mid <= 15%: Min만 null (해외법인 범위)
    for prefix in ['DiscountRate']:
        mid_col = f'{prefix}_Mid'
        min_col = f'{prefix}_Min'
        mask_min = (df[min_col].notna() & (df[min_col] > 15) &
                    (df[mid_col].isna() | (df[mid_col] <= 15)))
        n_min = mask_min.sum()
        if n_min > 0:
            print(f'  {year}: {min_col} > 15% (Mid 정상): {n_min}건 → Min null')
            df.loc[mask_min, min_col] = np.nan

    # 4. DR_Max > 15% & DR_Mid <= 15%: Max만 null
    for prefix in ['DiscountRate']:
        mid_col = f'{prefix}_Mid'
        max_col = f'{prefix}_Max'
        mask_max = (df[max_col].notna() & (df[max_col] > 15) &
                    (df[mid_col].isna() | (df[mid_col] <= 15)))
        n_max = mask_max.sum()
        if n_max > 0:
            print(f'  {year}: {max_col} > 15% (Mid 정상): {n_max}건 → Max null')
            df.loc[mask_max, max_col] = np.nan

    # Mid 재계산 (Min/Max 변경 후)
    for prefix in ['DiscountRate', 'SalaryGrowth']:
        mn = df[f'{prefix}_Min']
        mx = df[f'{prefix}_Max']
        mask_single = mn.notna() & mx.notna() & (mn == mx)
        df.loc[mask_single, f'{prefix}_Mid'] = df.loc[mask_single, f'{prefix}_Min']
        mask_no_mid = mn.notna() & mx.notna() & df[f'{prefix}_Mid'].isna()
        df.loc[mask_no_mid, f'{prefix}_Mid'] = (mn[mask_no_mid] + mx[mask_no_mid]) / 2

    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    n_after = len(df)
    if n_before != n_after or n_del > 0:
        print(f'  {year}: {n_before} → {n_after}건')

print('\n완료')
