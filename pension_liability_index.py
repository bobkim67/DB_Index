"""
pension_liability_index.py — 퇴직연금 부채증가율 → 목표수익률 산출

공모 OCIO: 동일가중 drift + 2차원 시나리오 테이블 (할인율 × 임금상승률)
사모 OCIO: 개별 기업 drift + 2차원 시나리오 테이블

입력: llm_extract_2024.csv (DART LLM 배치 추출 결과)
출력:
  - pension_target_return_public.csv  (공모 OCIO 시나리오)
  - pension_target_return_private.csv (사모 OCIO 기업별 시나리오)
  - 콘솔 리포트

보험계리 근거:
  부채증가율(ΔR, Δg) = base_drift + (-Dur_r × ΔR) + (Dur_g × Δg)
  base_drift = (SC - BP + IC) / DBO
  임금효과 = SC/DBO × [1 - 1/(1+g)^Dur]
  순수근속효과 = SC/DBO × 1/(1+g)^Dur
"""

import pandas as pd
import numpy as np
import sys


# ──────────────────────────────────────────────
# Step 1: 데이터 로드 & 필터
# ──────────────────────────────────────────────

AMOUNT_COLS = [
    '확정급여채무', '사외적립자산', '순확정급여부채',
    '당기근무원가', '이자비용', '이자수익', '순이자비용',
    '급여지급액', '보험수리적손익', '퇴직급여비용합계',
    '예상기여금', '확정기여제도비용',
]
RATE_COLS = [
    '할인율_최소', '할인율_최대', '할인율_중간',
    '임금상승률_최소', '임금상승률_최대', '임금상승률_중간',
    '듀레이션_최소', '듀레이션_최대', '듀레이션_중간',
]


def load_llm_extract(csv_path='llm_extract_2024.csv'):
    """LLM 추출 CSV 로드 → 금액/비율 변수 float 변환, DBO > 0 필터"""
    df = pd.read_csv(csv_path, dtype=str)

    # 처리상태 == 'OK' 필터 (부분 추출도 제외)
    df = df[df['처리상태'] == 'OK'].copy()

    # float 변환
    for c in AMOUNT_COLS + RATE_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # DBO > 0 필터
    df = df[df['확정급여채무'] > 0].copy()
    df = df.reset_index(drop=True)

    print(f"[로드] OK & DBO>0: {len(df)}사")
    return df


# ──────────────────────────────────────────────
# Step 2: 공모 OCIO 유니버스 구성
# ──────────────────────────────────────────────

def build_public_universe(df, dbo_floor=10_000_000_000):
    """공모 OCIO 유니버스: DBO > 100억, 필수변수(SC, IC, BP) 보유, 이상치 제거"""
    uni = df[df['확정급여채무'] > dbo_floor].copy()
    print(f"[유니버스] DBO > {dbo_floor/1e8:.0f}억: {len(uni)}사")

    # SC, IC, BP 모두 보유
    mask = (
        uni['당기근무원가'].notna() &
        uni['이자비용'].notna() &
        uni['급여지급액'].notna()
    )
    uni = uni[mask].copy()
    print(f"[유니버스] SC+IC+BP 보유: {len(uni)}사")

    # 비율 변수 계산 (이상치 판별용)
    uni['SC_DBO'] = uni['당기근무원가'] / uni['확정급여채무']
    uni['BP_DBO'] = uni['급여지급액'] / uni['확정급여채무']
    uni['IC_DBO'] = uni['이자비용'] / uni['확정급여채무']

    # 이상치 제거: SC/DBO > 50% or < 0, BP/DBO > 50% or < 0
    n_before = len(uni)
    uni = uni[
        (uni['SC_DBO'] >= 0) & (uni['SC_DBO'] <= 0.5) &
        (uni['BP_DBO'] >= 0) & (uni['BP_DBO'] <= 0.5) &
        (uni['IC_DBO'] >= 0) & (uni['IC_DBO'] <= 0.2)
    ].copy()
    n_outlier = n_before - len(uni)
    if n_outlier > 0:
        print(f"[유니버스] 이상치 제거: {n_outlier}사")

    print(f"[유니버스] 최종: {len(uni)}사")
    return uni


# ──────────────────────────────────────────────
# Step 3: Drift 산출 + 임금효과 분해
# ──────────────────────────────────────────────

def compute_drift(df):
    """기업별 drift 산출 + 임금효과 분해, 공모 인덱스 집계"""
    df = df.copy()

    # 기업별 drift
    df['drift'] = (df['당기근무원가'] - df['급여지급액'] + df['이자비용']) / df['확정급여채무']
    df['sc_effect'] = df['당기근무원가'] / df['확정급여채무']
    df['bp_effect'] = df['급여지급액'] / df['확정급여채무']
    df['ic_effect'] = df['이자비용'] / df['확정급여채무']

    # 임금효과 분해 (g, Dur 보유 기업만)
    has_wage_dur = (
        df['임금상승률_중간'].notna() &
        df['듀레이션_중간'].notna() &
        (df['임금상승률_중간'] > 0) &
        (df['듀레이션_중간'] > 0)
    )
    df['wage_effect'] = np.nan
    df['accrual_effect'] = np.nan

    if has_wage_dur.any():
        g = df.loc[has_wage_dur, '임금상승률_중간'] / 100  # % → decimal
        dur = df.loc[has_wage_dur, '듀레이션_중간']
        sc_dbo = df.loc[has_wage_dur, 'sc_effect']

        wage_factor = 1 - 1 / (1 + g) ** dur
        df.loc[has_wage_dur, 'wage_effect'] = sc_dbo * wage_factor
        df.loc[has_wage_dur, 'accrual_effect'] = sc_dbo * (1 - wage_factor)

    # 집계 통계
    stats = {
        'n_total': len(df),
        'n_wage_dur': has_wage_dur.sum(),
        'dbo_total': df['확정급여채무'].sum(),
        'drift_median': df['drift'].median(),
        'drift_mean': df['drift'].mean(),
        'drift_std': df['drift'].std(),
        'drift_p25': df['drift'].quantile(0.25),
        'drift_p75': df['drift'].quantile(0.75),
        'sc_dbo_median': df['sc_effect'].median(),
        'bp_dbo_median': df['bp_effect'].median(),
        'ic_dbo_median': df['ic_effect'].median(),
    }

    if has_wage_dur.any():
        stats['wage_effect_median'] = df.loc[has_wage_dur, 'wage_effect'].median()
        stats['accrual_effect_median'] = df.loc[has_wage_dur, 'accrual_effect'].median()
    else:
        stats['wage_effect_median'] = np.nan
        stats['accrual_effect_median'] = np.nan

    return df, stats


# ──────────────────────────────────────────────
# Step 4: Duration & 가정 통계
# ──────────────────────────────────────────────

def compute_assumptions(df):
    """Duration, 할인율, 임금상승률 대표값 산출"""
    result = {}

    # Duration
    dur_valid = df[df['듀레이션_중간'].notna() & (df['듀레이션_중간'] > 0)]
    result['dur_n'] = len(dur_valid)
    result['dur_coverage'] = len(dur_valid) / len(df)
    if len(dur_valid) > 0:
        result['dur_median'] = dur_valid['듀레이션_중간'].median()
        dbo_weights = dur_valid['확정급여채무'] / dur_valid['확정급여채무'].sum()
        result['dur_dbo_weighted'] = (dur_valid['듀레이션_중간'] * dbo_weights).sum()
    else:
        result['dur_median'] = np.nan
        result['dur_dbo_weighted'] = np.nan

    # 할인율
    dr_valid = df[df['할인율_중간'].notna() & (df['할인율_중간'] > 0)]
    result['dr_n'] = len(dr_valid)
    result['dr_coverage'] = len(dr_valid) / len(df)
    result['dr_median'] = dr_valid['할인율_중간'].median() if len(dr_valid) > 0 else np.nan

    # 임금상승률
    sg_valid = df[df['임금상승률_중간'].notna() & (df['임금상승률_중간'] > 0)]
    result['sg_n'] = len(sg_valid)
    result['sg_coverage'] = len(sg_valid) / len(df)
    result['sg_median'] = sg_valid['임금상승률_중간'].median() if len(sg_valid) > 0 else np.nan

    return result


# ──────────────────────────────────────────────
# Step 5: 2차원 시나리오 테이블 생성
# ──────────────────────────────────────────────

def build_2d_scenario_table(base_drift, dur_r, dur_g=None,
                            r_shocks_bp=None, g_shocks_pct=None):
    """
    할인율 × 임금상승률 2차원 시나리오 테이블 (%)

    Parameters:
        base_drift: 기본 drift (decimal, e.g., 0.086)
        dur_r: 할인율 duration (년)
        dur_g: 임금 duration (년), None이면 dur_r과 동일
        r_shocks_bp: 할인율 변동 시나리오 (bp), default [-100,-50,0,50,100]
        g_shocks_pct: 임금상승률 변동 시나리오 (%p), default [-1.0,-0.5,0,0.5,1.0]

    Returns:
        pd.DataFrame: 2차원 시나리오 테이블 (% 단위)
    """
    if dur_g is None:
        dur_g = dur_r
    if r_shocks_bp is None:
        r_shocks_bp = [-100, -50, 0, 50, 100]
    if g_shocks_pct is None:
        g_shocks_pct = [-1.0, -0.5, 0, 0.5, 1.0]

    rows = []
    for dr_bp in r_shocks_bp:
        row = {}
        for dg_pct in g_shocks_pct:
            dr_dec = dr_bp / 10000  # bp → decimal
            dg_dec = dg_pct / 100   # %p → decimal
            liability_change = base_drift + (-dur_r * dr_dec) + (dur_g * dg_dec)
            row[f"Δg={dg_pct:+.1f}%p"] = liability_change * 100  # → %
        rows.append(row)

    table = pd.DataFrame(rows, index=[f"ΔR={bp:+d}bp" for bp in r_shocks_bp])
    return table


# ──────────────────────────────────────────────
# Step 6: 사모 OCIO 개별 산출
# ──────────────────────────────────────────────

# 사모 대상 기업 (법인코드)
PRIVATE_TARGETS = {
    '00583424': '아모레퍼시픽',
    '00154462': '아모레퍼시픽홀딩스',
    '00503668': 'LIG넥스원',
    '00148504': 'SC제일은행(한국스탠다드차타드은행)',
}


def compute_private_ocio(df, public_assumptions):
    """사모 대상 기업별 개별 drift + 2차원 시나리오"""
    results = []

    for corp_code, corp_label in PRIVATE_TARGETS.items():
        row = df[df['법인코드'] == corp_code]
        if len(row) == 0:
            print(f"  [사모] {corp_label} ({corp_code}): 데이터 미보유")
            continue

        row = row.iloc[0]
        dbo = row['확정급여채무']
        sc = row['당기근무원가']
        ic = row['이자비용']
        bp = row['급여지급액']

        if pd.isna(dbo) or dbo <= 0 or pd.isna(sc) or pd.isna(ic) or pd.isna(bp):
            print(f"  [사모] {corp_label}: 필수변수 부족")
            continue

        drift = (sc - bp + ic) / dbo

        # Duration: 자체 보유 시 사용, 없으면 공모 대표값
        dur = row['듀레이션_중간']
        dur_source = '자체'
        if pd.isna(dur) or dur <= 0:
            dur = public_assumptions['dur_dbo_weighted']
            dur_source = '공모대표값'

        # 임금상승률: 자체 보유 시 사용
        g = row['임금상승률_중간']
        g_source = '자체'
        if pd.isna(g) or g <= 0:
            g = public_assumptions['sg_median']
            g_source = '공모대표값'

        # 임금효과 분해
        sc_dbo = sc / dbo
        if not pd.isna(g) and g > 0 and not pd.isna(dur) and dur > 0:
            g_dec = g / 100
            wage_factor = 1 - 1 / (1 + g_dec) ** dur
            wage_effect = sc_dbo * wage_factor
            accrual_effect = sc_dbo * (1 - wage_factor)
        else:
            wage_effect = np.nan
            accrual_effect = np.nan

        # 2차원 시나리오
        if not pd.isna(dur) and dur > 0:
            scenario = build_2d_scenario_table(drift, dur)
        else:
            scenario = None

        info = {
            '법인코드': corp_code,
            '법인명': corp_label,
            'DBO(억)': dbo / 1e8,
            'SC(억)': sc / 1e8,
            'IC(억)': ic / 1e8,
            'BP(억)': bp / 1e8,
            'PA(억)': row['사외적립자산'] / 1e8 if not pd.isna(row['사외적립자산']) else np.nan,
            'drift(%)': drift * 100,
            'SC/DBO(%)': sc_dbo * 100,
            'BP/DBO(%)': (bp / dbo) * 100,
            'IC/DBO(%)': (ic / dbo) * 100,
            'Duration': dur,
            'Duration출처': dur_source,
            '임금상승률(%)': g,
            '임금상승률출처': g_source,
            '임금효과(%p)': wage_effect * 100 if not pd.isna(wage_effect) else np.nan,
            '순수근속효과(%p)': accrual_effect * 100 if not pd.isna(accrual_effect) else np.nan,
            'scenario_table': scenario,
        }
        results.append(info)

        # 콘솔 출력
        print(f"\n  ── {corp_label} ({corp_code}) ──")
        print(f"  DBO: {dbo/1e8:,.0f}억  PA: {info['PA(억)']:,.0f}억  적립비율: {row['사외적립자산']/dbo*100:.1f}%")
        print(f"  SC/DBO: {sc_dbo*100:.1f}%  BP/DBO: {bp/dbo*100:.1f}%  IC/DBO: {ic/dbo*100:.1f}%")
        print(f"  drift: {drift*100:.1f}%  Duration: {dur:.1f} ({dur_source})  g: {g}% ({g_source})")
        if not pd.isna(wage_effect):
            print(f"  임금효과: {wage_effect*100:.1f}%p  순수근속: {accrual_effect*100:.1f}%p")
        if scenario is not None:
            print(f"\n  [2차원 시나리오: 부채증가율 %]")
            print(scenario.to_string(float_format='{:.1f}'.format))

    return results


# ──────────────────────────────────────────────
# Step 7: 출력
# ──────────────────────────────────────────────

def save_public_csv(stats, assumptions, scenario_table, uni_df, path='pension_target_return_public.csv'):
    """공모 OCIO 결과 CSV 저장"""
    rows = []

    # 섹션 1: 유니버스 통계
    rows.append({'항목': '=== 유니버스 통계 ===', '값': ''})
    rows.append({'항목': '유니버스 기업수', '값': stats['n_total']})
    rows.append({'항목': 'DBO 합계(조)', '값': f"{stats['dbo_total']/1e12:.1f}"})
    rows.append({'항목': '임금/Duration 보유 기업수', '값': stats['n_wage_dur']})

    # 섹션 2: Drift 분해
    rows.append({'항목': '', '값': ''})
    rows.append({'항목': '=== Drift 분해 (median) ===', '값': ''})
    rows.append({'항목': 'SC/DBO (%)', '값': f"{stats['sc_dbo_median']*100:.2f}"})
    rows.append({'항목': 'BP/DBO (%)', '값': f"{stats['bp_dbo_median']*100:.2f}"})
    rows.append({'항목': 'IC/DBO (%)', '값': f"{stats['ic_dbo_median']*100:.2f}"})
    rows.append({'항목': 'Drift = (SC-BP+IC)/DBO (%)', '값': f"{stats['drift_median']*100:.2f}"})
    rows.append({'항목': 'Drift mean (%)', '값': f"{stats['drift_mean']*100:.2f}"})
    rows.append({'항목': 'Drift std (%)', '값': f"{stats['drift_std']*100:.2f}"})
    rows.append({'항목': 'Drift P25 (%)', '값': f"{stats['drift_p25']*100:.2f}"})
    rows.append({'항목': 'Drift P75 (%)', '값': f"{stats['drift_p75']*100:.2f}"})
    if not pd.isna(stats['wage_effect_median']):
        rows.append({'항목': '임금효과 median (%p)', '값': f"{stats['wage_effect_median']*100:.2f}"})
        rows.append({'항목': '순수근속효과 median (%p)', '값': f"{stats['accrual_effect_median']*100:.2f}"})

    # 섹션 3: 가정 통계
    rows.append({'항목': '', '값': ''})
    rows.append({'항목': '=== 가정 대표값 ===', '값': ''})
    rows.append({'항목': f"Duration median (n={assumptions['dur_n']}, {assumptions['dur_coverage']*100:.0f}%)",
                 '값': f"{assumptions['dur_median']:.2f}" if not pd.isna(assumptions['dur_median']) else 'N/A'})
    rows.append({'항목': f"Duration DBO가중 (n={assumptions['dur_n']})",
                 '값': f"{assumptions['dur_dbo_weighted']:.2f}" if not pd.isna(assumptions['dur_dbo_weighted']) else 'N/A'})
    rows.append({'항목': f"할인율 median (n={assumptions['dr_n']}, {assumptions['dr_coverage']*100:.0f}%)",
                 '값': f"{assumptions['dr_median']:.2f}" if not pd.isna(assumptions['dr_median']) else 'N/A'})
    rows.append({'항목': f"임금상승률 median (n={assumptions['sg_n']}, {assumptions['sg_coverage']*100:.0f}%)",
                 '값': f"{assumptions['sg_median']:.2f}" if not pd.isna(assumptions['sg_median']) else 'N/A'})

    # 섹션 4: 2차원 시나리오 테이블
    rows.append({'항목': '', '값': ''})
    rows.append({'항목': '=== 2차원 시나리오: 부채증가율(%) ===', '값': ''})

    meta_df = pd.DataFrame(rows)

    # 시나리오 테이블을 별도 시트처럼 아래에 이어 붙이기
    scenario_with_index = scenario_table.copy()
    scenario_with_index.insert(0, '할인율변동', scenario_table.index)
    scenario_with_index = scenario_with_index.reset_index(drop=True)

    # 단일 CSV로 저장
    with open(path, 'w', encoding='utf-8-sig', newline='') as f:
        meta_df.to_csv(f, index=False)
        f.write('\n')
        scenario_with_index.to_csv(f, index=False)

    print(f"\n[저장] {path}")


def save_private_csv(results, path='pension_target_return_private.csv'):
    """사모 OCIO 결과 CSV 저장"""
    all_rows = []

    for info in results:
        corp = info['법인명']
        all_rows.append({'기업': corp, '항목': '법인코드', '값': info['법인코드']})
        all_rows.append({'기업': corp, '항목': 'DBO(억)', '값': f"{info['DBO(억)']:,.0f}"})
        all_rows.append({'기업': corp, '항목': 'PA(억)', '값': f"{info['PA(억)']:,.0f}" if not pd.isna(info['PA(억)']) else 'N/A'})
        all_rows.append({'기업': corp, '항목': 'SC/DBO(%)', '값': f"{info['SC/DBO(%)']:.1f}"})
        all_rows.append({'기업': corp, '항목': 'BP/DBO(%)', '값': f"{info['BP/DBO(%)']:.1f}"})
        all_rows.append({'기업': corp, '항목': 'IC/DBO(%)', '값': f"{info['IC/DBO(%)']:.1f}"})
        all_rows.append({'기업': corp, '항목': 'Drift(%)', '값': f"{info['drift(%)']:.1f}"})
        all_rows.append({'기업': corp, '항목': 'Duration', '값': f"{info['Duration']:.1f}"})
        all_rows.append({'기업': corp, '항목': 'Duration출처', '값': info['Duration출처']})
        all_rows.append({'기업': corp, '항목': '임금상승률(%)', '값': f"{info['임금상승률(%)']}" if not pd.isna(info['임금상승률(%)']) else 'N/A'})
        if not pd.isna(info['임금효과(%p)']):
            all_rows.append({'기업': corp, '항목': '임금효과(%p)', '값': f"{info['임금효과(%p)']:.1f}"})
            all_rows.append({'기업': corp, '항목': '순수근속효과(%p)', '값': f"{info['순수근속효과(%p)']:.1f}"})

        # 시나리오 테이블
        if info['scenario_table'] is not None:
            all_rows.append({'기업': corp, '항목': '', '값': ''})
            all_rows.append({'기업': corp, '항목': '시나리오(부채증가율%)', '값': ''})
            tbl = info['scenario_table']
            # 헤더
            all_rows.append({'기업': corp, '항목': '할인율변동＼임금변동',
                             '값': '  '.join(tbl.columns)})
            for idx_name, row_data in tbl.iterrows():
                all_rows.append({
                    '기업': corp, '항목': idx_name,
                    '값': '  '.join(f"{v:.1f}" for v in row_data)
                })

        all_rows.append({'기업': '', '항목': '', '값': ''})

    pd.DataFrame(all_rows).to_csv(path, index=False, encoding='utf-8-sig')
    print(f"[저장] {path}")


def print_report(stats, assumptions, scenario_table):
    """콘솔 리포트 출력"""
    print("\n" + "=" * 60)
    print("  공모 OCIO 퇴직연금 부채증가율 → 목표수익률")
    print("  (2024년 사업보고서 기준)")
    print("=" * 60)

    print(f"\n■ 유니버스: {stats['n_total']}사, DBO 합계 {stats['dbo_total']/1e12:.1f}조")

    print(f"\n■ Drift 분해 (median)")
    print(f"  SC/DBO:   {stats['sc_dbo_median']*100:6.2f}%  (당기근무원가/확정급여채무)")
    print(f"  BP/DBO:  -{stats['bp_dbo_median']*100:6.2f}%  (급여지급/확정급여채무)")
    print(f"  IC/DBO:   {stats['ic_dbo_median']*100:6.2f}%  (이자비용/확정급여채무)")
    print(f"  ─────────────────────")
    print(f"  Drift:    {stats['drift_median']*100:6.2f}%  (= SC - BP + IC) / DBO")
    print(f"  [P25={stats['drift_p25']*100:.1f}%, P75={stats['drift_p75']*100:.1f}%, std={stats['drift_std']*100:.1f}%]")

    if not pd.isna(stats.get('wage_effect_median', np.nan)):
        print(f"\n  ── 임금효과 분해 (g+Dur 보유 {stats['n_wage_dur']}사) ──")
        print(f"  임금효과:     {stats['wage_effect_median']*100:6.2f}%p")
        print(f"  순수근속효과: {stats['accrual_effect_median']*100:6.2f}%p")

    print(f"\n■ 가정 대표값")
    print(f"  Duration median:       {assumptions['dur_median']:.2f}년 (n={assumptions['dur_n']}, {assumptions['dur_coverage']*100:.0f}%)")
    print(f"  Duration DBO가중:      {assumptions['dur_dbo_weighted']:.2f}년")
    print(f"  할인율 median:         {assumptions['dr_median']:.2f}% (n={assumptions['dr_n']}, {assumptions['dr_coverage']*100:.0f}%)")
    print(f"  임금상승률 median:     {assumptions['sg_median']:.2f}% (n={assumptions['sg_n']}, {assumptions['sg_coverage']*100:.0f}%)")
    print(f"  Dur_g = Dur_r (근사)")

    print(f"\n■ 2차원 시나리오 테이블: 부채증가율 (%)")
    print(f"  행: 할인율 변동(bp)  열: 임금상승률 변동(%p)")
    print(scenario_table.to_string(float_format='{:.1f}'.format))

    print(f"\n  * 부채증가율 = base_drift({stats['drift_median']*100:.1f}%) + (-Dur×ΔR) + (Dur×Δg)")
    print(f"  * 목표수익률 = 부채증가율 (자산수익률이 부채증가율을 추적해야 적립비율 유지)")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    # Step 1: 로드
    df = load_llm_extract()

    # Step 2: 공모 유니버스
    uni = build_public_universe(df)

    # Step 3: Drift 산출
    uni, stats = compute_drift(uni)

    # Step 4: 가정 통계
    assumptions = compute_assumptions(uni)

    # Step 5: 2차원 시나리오
    dur_r = assumptions['dur_dbo_weighted']
    if pd.isna(dur_r):
        dur_r = assumptions['dur_median']
    if pd.isna(dur_r):
        print("[경고] Duration 정보 없음, 시나리오 테이블 생성 불가")
        return

    scenario_table = build_2d_scenario_table(
        base_drift=stats['drift_median'],
        dur_r=dur_r,
        dur_g=dur_r,  # Dur_g = Dur_r 근사
    )

    # 콘솔 리포트
    print_report(stats, assumptions, scenario_table)

    # Step 6: 사모 OCIO
    print("\n" + "=" * 60)
    print("  사모 OCIO 개별 기업 산출")
    print("=" * 60)
    private_results = compute_private_ocio(df, assumptions)

    # Step 7: CSV 저장
    save_public_csv(stats, assumptions, scenario_table, uni)
    save_private_csv(private_results)

    print("\n[완료]")


if __name__ == '__main__':
    main()
