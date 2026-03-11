"""
pension_index.py — DB형 퇴직연금 부채 인덱스 산출
2022=100 기준, 연말 DBO 시계열로 "연 평균 부채 증가율" 제시
총액분리 가능 CIK 대상, Tier 균등가중 방식
"""
import pandas as pd
import numpy as np
import csv
from pathlib import Path

# ── 경로 ──
BASE = Path(__file__).parent
VAL_2024 = BASE / '2024_4Q' / 'val.tsv'
VAL_2023 = BASE / '2023_4Q' / 'val.tsv'
DBO_TYPE_CSV = BASE / 'pension_dbo_type.csv'
MAPPING_CSV = BASE / 'pension_cik_mapping.csv'
OUT_DATA = BASE / 'pension_index_data.csv'
OUT_RESULT = BASE / 'pension_index_result.csv'

# ── IFRS 표준 ELEMENT_ID ──
IFRS_DBO = 'ifrs-full_DefinedBenefitObligationAtPresentValue'
IFRS_PA = 'ifrs-full_PlanAssetsAtFairValue'
TARGET_EIDS = {IFRS_DBO, IFRS_PA}

# ── Tier 경계 (2022末 DBO 기준, 원) ──
TIER_BOUNDS = [1_000_000_000_000, 200_000_000_000, 50_000_000_000]
TIER_LABELS = ['1_대형', '2_중형', '3_중소형', '4_소형']

# ── 이상치 기준 ──
OUTLIER_MEDIAN_MULT = 100   # DBO > 100× median → EXTREME_SIZE
OUTLIER_YOY_PCT = 5.0       # YoY 변동 > 500% → EXTREME_CHANGE

CHUNK_SIZE = 500_000


# ═══════════════════════════════════════════════════════════
# Step 1: 총액분리 CIK 식별
# ═══════════════════════════════════════════════════════════
def load_target_ciks():
    """pension_dbo_type.csv → 총액분리 가능 CIK set (has_ifrs_dbo & has_pa & ifrs_dbo_val > 0)"""
    df = pd.read_csv(DBO_TYPE_CSV, dtype=str)
    mask = (
        (df['has_ifrs_dbo'] == 'True')
        & (df['has_pa'] == 'True')
        & df['ifrs_dbo_val'].notna()
        & (df['ifrs_dbo_val'] != '')
    )
    df_filt = df[mask].copy()
    df_filt['_val'] = pd.to_numeric(df_filt['ifrs_dbo_val'], errors='coerce')
    target = set(df_filt.loc[df_filt['_val'] > 0, 'CIK'])
    print(f"[Step 1] 총액분리 대상 CIK: {len(target)}개")
    return target


# ═══════════════════════════════════════════════════════════
# Step 2: 3개년 패널 추출
# ═══════════════════════════════════════════════════════════
def _extract_year_data(val_path, target_ciks, ctx_prefix):
    """val.tsv에서 특정 기간 IFRS DBO/PA 추출 (chunked 읽기)

    ctx_prefix: 'CFY2024eFY', 'PFY2023eFY' 등
    Returns: DataFrame[CIK, variable(DBO/PA), value]
    """
    chunks = []
    for chunk in pd.read_csv(
        val_path, sep='\t', dtype=str,
        quoting=csv.QUOTE_NONE, on_bad_lines='warn',
        chunksize=CHUNK_SIZE,
        usecols=['CIK', 'ELEMENT_ID', 'CONTEXT_ID', 'UNIT_ID', 'VALUE'],
    ):
        mask = (
            chunk['CIK'].isin(target_ciks)
            & chunk['ELEMENT_ID'].isin(TARGET_EIDS)
            & (chunk['UNIT_ID'] == 'KRW')
            & chunk['CONTEXT_ID'].str.startswith(ctx_prefix, na=False)
        )
        filt = chunk[mask]
        if not filt.empty:
            chunks.append(filt.copy())

    if not chunks:
        return pd.DataFrame(columns=['CIK', 'variable', 'value'])

    df = pd.concat(chunks, ignore_index=True)
    df['value'] = pd.to_numeric(df['VALUE'], errors='coerce')
    df['variable'] = df['ELEMENT_ID'].map({IFRS_DBO: 'DBO', IFRS_PA: 'PA'})

    # 우선순위 스코어: Consolidated > non-Separate > any; Dimension축 적을수록 좋음
    df['is_consol'] = df['CONTEXT_ID'].str.contains('ConsolidatedMember', na=False).astype(int)
    df['is_separate'] = df['CONTEXT_ID'].str.contains('SeparateMember', na=False).astype(int)
    df['n_axes'] = df['CONTEXT_ID'].str.count('Axis')
    df['score'] = df['is_consol'] * 100 + (1 - df['is_separate']) * 50 - df['n_axes'] * 10

    # CIK × variable별 최고 score 선택
    idx = df.groupby(['CIK', 'variable'])['score'].idxmax()
    return df.loc[idx, ['CIK', 'variable', 'value']].reset_index(drop=True)


def build_panel(target_ciks):
    """3개년 패널(wide format) 구축

    시점 소스:
      2024末 ← 2024_4Q CFY2024eFY
      2023末 ← 2024_4Q PFY2023eFY (primary) / 2023_4Q CFY2023eFY (fallback)
      2022末 ← 2023_4Q PFY2022eFY
    """
    extractions = [
        ('2024',     VAL_2024, 'CFY2024eFY'),
        ('2023_pfy', VAL_2024, 'PFY2023eFY'),
        ('2023_cfy', VAL_2023, 'CFY2023eFY'),
        ('2022',     VAL_2023, 'PFY2022eFY'),
    ]

    dfs = {}
    for label, path, prefix in extractions:
        print(f"  추출: {label} ({path.parent.name}/{prefix})...")
        dfs[label] = _extract_year_data(path, target_ciks, prefix)
        print(f"    → {len(dfs[label])} rows")

    # 2023: PFY 우선, CFY fallback
    df_2023 = dfs['2023_pfy'].copy()
    covered = set(df_2023['CIK'].unique())
    missing = target_ciks - covered
    if missing:
        fb = dfs['2023_cfy']
        fb = fb[fb['CIK'].isin(missing)]
        df_2023 = pd.concat([df_2023, fb], ignore_index=True)
        print(f"  2023末 fallback: {len(fb)} rows from {len(missing)} CIKs")

    # wide format: CIK × (DBO_YYYY, PA_YYYY)
    panels = []
    for year, df in [('2022', dfs['2022']), ('2023', df_2023), ('2024', dfs['2024'])]:
        if df.empty:
            continue
        wide = df.pivot_table(index='CIK', columns='variable', values='value', aggfunc='first')
        wide.columns = [f'{c}_{year}' for c in wide.columns]
        panels.append(wide)

    panel = panels[0]
    for p in panels[1:]:
        panel = panel.join(p, how='outer')
    panel = panel.reset_index()

    # 컬럼 순서 정리
    desired = ['CIK']
    for y in ['2022', '2023', '2024']:
        desired += [f'DBO_{y}', f'PA_{y}']
    panel = panel.reindex(columns=[c for c in desired if c in panel.columns])

    print(f"[Step 2] 패널 CIK: {len(panel)}개")
    return panel


# ═══════════════════════════════════════════════════════════
# Step 3: 이상치 탐지
# ═══════════════════════════════════════════════════════════
def detect_outliers(panel):
    flags = pd.Series('', index=panel.index)

    # EXTREME_SIZE: DBO > 100× median (어느 연도든)
    dbo_cols = [c for c in panel.columns if c.startswith('DBO_')]
    for col in dbo_cols:
        vals = panel[col].dropna()
        if len(vals) == 0:
            continue
        med = vals.median()
        if med > 0:
            extreme = panel[col] > OUTLIER_MEDIAN_MULT * med
            flags = flags.where(~extreme, flags + 'EXTREME_SIZE;')

    # EXTREME_CHANGE: YoY > 500%
    for prev, curr in [('DBO_2022', 'DBO_2023'), ('DBO_2023', 'DBO_2024')]:
        if prev not in panel.columns or curr not in panel.columns:
            continue
        both = panel[prev].notna() & panel[curr].notna() & (panel[prev] > 0)
        ratio = panel[curr] / panel[prev].where(both)
        extreme = (ratio > 1 + OUTLIER_YOY_PCT) | (ratio < 1 / (1 + OUTLIER_YOY_PCT))
        flags = flags.where(~(both & extreme), flags + 'EXTREME_CHANGE;')

    # NEGATIVE_DBO
    for col in dbo_cols:
        neg = panel[col].fillna(0) < 0
        flags = flags.where(~neg, flags + 'NEGATIVE_DBO;')

    panel['outlier_flag'] = flags.str.strip(';')
    n = (panel['outlier_flag'] != '').sum()
    print(f"[Step 3] 이상치: {n}개 CIK 플래그")
    return panel


# ═══════════════════════════════════════════════════════════
# Step 4: Tier 배정
# ═══════════════════════════════════════════════════════════
def assign_tiers(panel):
    """2022末 DBO 기준 Tier 배정"""
    def _tier(dbo):
        if pd.isna(dbo) or dbo <= 0:
            return None
        for i, bound in enumerate(TIER_BOUNDS):
            if dbo >= bound:
                return TIER_LABELS[i]
        return TIER_LABELS[-1]

    panel['tier'] = panel.get('DBO_2022', pd.Series(dtype=float)).apply(_tier)

    print("[Step 4] Tier 분포:")
    for t in TIER_LABELS:
        n = (panel['tier'] == t).sum()
        print(f"  {t}: {n}개")
    print(f"  (미배정: {panel['tier'].isna().sum()}개)")
    return panel


# ═══════════════════════════════════════════════════════════
# Step 5: 인덱스 산출
# ═══════════════════════════════════════════════════════════
def compute_index(panel):
    """Tier 균등가중, DBO 가중, 동일가중 인덱스 산출

    Returns: (df_result, balanced_panel)
    """
    # Balanced panel: 3개년 DBO > 0, 이상치 아님, Tier 있음
    mask = (
        (panel.get('DBO_2022', 0) > 0)
        & (panel.get('DBO_2023', 0) > 0)
        & (panel.get('DBO_2024', 0) > 0)
        & (panel['outlier_flag'] == '')
        & panel['tier'].notna()
    )
    bal = panel[mask].copy()
    print(f"[Step 5] Balanced panel: {len(bal)}개 CIK")

    years = ['2022', '2023', '2024']
    rows = []

    for year in years:
        r = {'year_end': int(year)}

        # 총합
        r['total_dbo'] = bal[f'DBO_{year}'].sum()
        pa_col = f'PA_{year}'
        r['total_pa'] = bal[pa_col].sum() if pa_col in bal.columns else np.nan
        r['funding_ratio'] = r['total_pa'] / r['total_dbo'] if r['total_dbo'] > 0 else np.nan
        r['n_cik'] = len(bal)

        # 개별 성장률 (vs 2022)
        growth = bal[f'DBO_{year}'] / bal['DBO_2022']

        # ── Tier 균등가중 ──
        tier_growths = {}
        for t in TIER_LABELS:
            t_mask = bal['tier'] == t
            t_g = growth[t_mask]
            if len(t_g) > 0:
                tier_growths[t] = t_g.mean()
                r[f'growth_{t}'] = round(t_g.mean(), 6)
                r[f'n_{t}'] = int(len(t_g))
            else:
                tier_growths[t] = None
                r[f'growth_{t}'] = np.nan
                r[f'n_{t}'] = 0

        active = [t for t, g in tier_growths.items() if g is not None]
        w = 1.0 / len(active) if active else 0.25
        idx_tiered = sum(w * tier_growths[t] for t in active) * 100
        r['index_tiered'] = round(idx_tiered, 2)

        # ── DBO 가중 ──
        total_base = bal['DBO_2022'].sum()
        if total_base > 0:
            dbo_wgt = (bal['DBO_2022'] * growth).sum() / total_base
        else:
            dbo_wgt = 1.0
        r['index_dbo_weighted'] = round(dbo_wgt * 100, 2)

        # ── 동일가중 ──
        r['index_equal_weighted'] = round(growth.mean() * 100, 2)

        rows.append(r)

    df_result = pd.DataFrame(rows)

    # YoY 변동률 추가
    for col in ['index_tiered', 'index_dbo_weighted', 'index_equal_weighted']:
        df_result[f'{col}_yoy'] = df_result[col].pct_change()

    return df_result, bal


# ═══════════════════════════════════════════════════════════
# Step 6: 부가 지표
# ═══════════════════════════════════════════════════════════
def compute_supplementary(target_ciks, panel):
    """pension_cik_mapping.csv에서 부가 지표 추출

    Returns: dict[year] → {var_metric: value, ...}
    """
    mapping = pd.read_csv(MAPPING_CSV, dtype=str)
    mapping['sample_VALUE'] = pd.to_numeric(mapping['sample_VALUE'], errors='coerce')
    mapping = mapping[mapping['CIK'].isin(target_ciks)]

    quarter_year = {'2024_4Q': '2024', '2023_4Q': '2023'}
    rate_vars = ['DiscountRate', 'SalaryGrowth', 'Duration']
    amount_vars = ['ServiceCost', 'BenefitPayment', 'InterestCost', 'ActuarialGL']

    supp = {}
    for quarter, year in quarter_year.items():
        q = mapping[mapping['quarter'] == quarter]
        dbo_col = f'DBO_{year}'
        row = {}

        for var in rate_vars:
            vd = q[q['variable'] == var].drop_duplicates('CIK')
            vals = vd['sample_VALUE'].dropna()
            row[f'{var}_n'] = int(len(vals))

            if vals.empty:
                row[f'{var}_mean'] = np.nan
                row[f'{var}_dbo_wgt'] = np.nan
                continue

            row[f'{var}_mean'] = round(vals.mean(), 6)

            # DBO 가중평균
            if dbo_col in panel.columns:
                merged = vd[['CIK', 'sample_VALUE']].merge(
                    panel[['CIK', dbo_col]], on='CIK', how='inner'
                )
                merged = merged.dropna(subset=['sample_VALUE', dbo_col])
                denom = merged[dbo_col].sum()
                if len(merged) > 0 and denom > 0:
                    row[f'{var}_dbo_wgt'] = round(
                        (merged['sample_VALUE'] * merged[dbo_col]).sum() / denom, 6
                    )
                else:
                    row[f'{var}_dbo_wgt'] = np.nan
            else:
                row[f'{var}_dbo_wgt'] = np.nan

        for var in amount_vars:
            vd = q[q['variable'] == var].drop_duplicates('CIK')
            vals = vd['sample_VALUE'].dropna()
            row[f'{var}_total'] = vals.sum() if len(vals) > 0 else np.nan
            row[f'{var}_n'] = int(len(vals))

        supp[year] = row

    return supp


# ═══════════════════════════════════════════════════════════
# Step 7: CSV 출력 & 실행
# ═══════════════════════════════════════════════════════════
def add_growth_columns(panel):
    """패널에 성장률 컬럼 추가"""
    if 'DBO_2022' in panel.columns:
        base = panel['DBO_2022'].replace(0, np.nan)
        for y in ['2022', '2023', '2024']:
            col = f'DBO_{y}'
            if col in panel.columns:
                panel[f'growth_{y}'] = panel[col] / base
    return panel


def main():
    print("=" * 60)
    print("DB형 퇴직연금 부채 인덱스 산출")
    print("=" * 60)

    # Step 1
    target_ciks = load_target_ciks()

    # Step 2
    print("\n[Step 2] 3개년 패널 추출...")
    panel = build_panel(target_ciks)

    # Step 3
    print()
    panel = detect_outliers(panel)

    # Step 4
    print()
    panel = assign_tiers(panel)

    # Step 5
    print()
    df_result, bal = compute_index(panel)

    # Step 6
    print("\n[Step 6] 부가 지표...")
    supp = compute_supplementary(target_ciks, panel)
    for year, metrics in sorted(supp.items()):
        print(f"  {year}末:")
        for k, v in metrics.items():
            if '_n' not in k:
                print(f"    {k}: {v}")

    # 부가 지표를 result에 병합
    for _, row in df_result.iterrows():
        yr = str(int(row['year_end']))
        if yr in supp:
            for k, v in supp[yr].items():
                df_result.loc[df_result['year_end'] == row['year_end'], k] = v

    # Step 7: 출력
    panel = add_growth_columns(panel)
    out_cols = ['CIK']
    for y in ['2022', '2023', '2024']:
        out_cols += [f'DBO_{y}', f'PA_{y}']
    out_cols += ['tier', 'outlier_flag']
    for y in ['2022', '2023', '2024']:
        out_cols.append(f'growth_{y}')
    panel_out = panel[[c for c in out_cols if c in panel.columns]]
    panel_out.to_csv(OUT_DATA, index=False, encoding='utf-8-sig')
    print(f"\n[Step 7] {OUT_DATA.name} 저장 ({len(panel_out)} rows)")

    df_result.to_csv(OUT_RESULT, index=False, encoding='utf-8-sig')
    print(f"[Step 7] {OUT_RESULT.name} 저장 ({len(df_result)} rows)")

    # 요약
    print("\n" + "=" * 60)
    print("인덱스 결과:")
    print(df_result[['year_end', 'index_tiered', 'index_dbo_weighted',
                      'index_equal_weighted', 'n_cik']].to_string(index=False))

    if len(df_result) >= 3:
        v24 = df_result.loc[df_result['year_end'] == 2024, 'index_tiered'].iloc[0]
        cagr = (v24 / 100) ** (1 / 2) - 1
        print(f"\nCAGR 2022-2024 (Tier균등): {cagr:.2%}")
        print(f"적립비율 2024末: {df_result.loc[df_result['year_end'] == 2024, 'funding_ratio'].iloc[0]:.2%}")

    print("=" * 60)


if __name__ == '__main__':
    main()
