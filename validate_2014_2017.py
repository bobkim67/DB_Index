"""2014~2017 LLM 추출 데이터 검증 파이프라인

검증 단계:
1. Status 확인 (이미 OK/부분 형태)
2. DR/SG 이상치 보정 (금액 혼입, 소수 단위)
3. Shock 환산 (pension_extracts JSON에서 판별 → 0.5%p→×2)
4. 원본 매칭 검증 (민감도 LLM값 vs JSON 숫자 대조)
5. 환각 제거 (Duration>50년, DBO=0인데 민감도 존재)
6. 비대칭 플래그 (|DR_up/DR_down| ratio)
7. merged CSV 생성
"""
import pandas as pd
import numpy as np
import json, os, re, sys

sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# 유틸리티
# ============================================================
PCT_KW = ['1%', '1.0%', '1.00%', '0.0100', '0.01', '0.5%', '0.5%p', '0.005',
          '0.25%', '0.25%p', '0.0025', 'basis point', 'bp', '100bp', 'bps']
SENS_KW = ['민감도', '증가', '상승', '감소', '하락', '변동', '1%p', '0.5%', 'basis']


def find_sensitivity_tables(tables):
    """민감도 관련 테이블만 추출"""
    result = []
    for t in tables:
        if any(k in t for k in PCT_KW) and any(k in t for k in SENS_KW):
            result.append(t)
    return result


def detect_shock(tables):
    """민감도 테이블에서 shock 값(변동폭) 판별"""
    sens_tables = find_sensitivity_tables(tables)
    if not sens_tables:
        return None, None

    text = ' '.join(sens_tables)

    # DR shock
    dr_shock = 1.0  # 기본값
    if any(k in text for k in ['0.25%', '0.25%p', '0.0025', '25bp']):
        dr_shock = 0.25
    elif any(k in text for k in ['0.5%', '0.5%p', '0.005', '50bp']):
        # '0.5%'가 있지만 '1.0%'나 '1%'도 있으면 1%p가 맞음
        if not any(k in text for k in ['1%p', '1.0%', '1.00%']):
            dr_shock = 0.5

    # SG shock (보통 DR과 동일하지만 별도 확인)
    sg_shock = dr_shock

    return dr_shock, sg_shock


def extract_numbers_from_text(text):
    """텍스트에서 모든 숫자 추출 (콤마 제거, 괄호 음수 처리)"""
    numbers = set()
    # 괄호 음수: (1,234,567) → -1234567
    for m in re.finditer(r'\(([0-9,]+(?:\.[0-9]+)?)\)', text):
        try:
            numbers.add(-float(m.group(1).replace(',', '')))
        except ValueError:
            pass
    # 일반 숫자 (음수 포함)
    for m in re.finditer(r'[-]?[0-9,]+(?:\.[0-9]+)?', text):
        try:
            numbers.add(float(m.group().replace(',', '')))
        except ValueError:
            pass
    return numbers


def validate_sensitivity_match(llm_val, dbo, numbers):
    """4단계 매칭 검증 — LLM 민감도 값이 원본에 존재하는지"""
    if llm_val is None or np.isnan(llm_val):
        return 'no_value'

    abs_val = abs(llm_val)

    # Stage 1: 금액 직접 매칭 (원, 천원, 백만원)
    for scale in [1, 1e3, 1e6]:
        check = abs_val / scale
        if check in numbers or -check in numbers:
            return 'match_direct'
        # ±1% 허용
        for n in numbers:
            if abs(n) > 0 and abs(abs(n) - check) / abs(n) < 0.01:
                return 'match_direct'

    # Stage 2: 비율 매칭 (LLM값/DBO*100이 원본에 존재)
    if dbo and dbo > 0:
        pct = abs_val / dbo * 100
        for n in numbers:
            if abs(n) > 0 and abs(abs(n) - pct) < 0.5:
                return 'match_pct'

    # Stage 3: 역산 매칭 (원본비율 × DBO ≈ LLM값)
    if dbo and dbo > 0:
        for n in numbers:
            if 0 < abs(n) < 100:  # 비율 후보
                calc = abs(n) / 100 * dbo
                if abs_val > 0 and abs(calc - abs_val) / abs_val < 0.05:
                    return 'match_reverse'

    # Stage 4: 절대값 매칭 (|원본×단위 - DBO| ≈ LLM값)
    if dbo and dbo > 0:
        for n in numbers:
            for scale in [1, 1e3, 1e6]:
                diff = abs(n * scale - dbo)
                if abs_val > 0 and diff > 0 and abs(diff - abs_val) / abs_val < 0.05:
                    return 'match_abs'

    return 'no_match'


# ============================================================
# 메인 검증
# ============================================================
def validate_year(year):
    csv_path = f'llm_extract_{year}.csv'
    ext_dir = f'pension_extracts/{year}'
    out_csv = f'llm_extract_{year}_merged.csv'

    df = pd.read_csv(csv_path, dtype={'corp_code': str, 'rcept_no': str})
    print(f'\n{"="*60}')
    print(f'{year}년 검증 시작 (n={len(df)})')
    print(f'{"="*60}')

    # 숫자 컬럼 변환
    numeric_cols = ['DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
                    'InterestIncome', 'NetInterest', 'BenefitPayment', 'ActuarialGL',
                    'ActuarialGL_Financial', 'ActuarialGL_Demographic', 'ActuarialGL_Experience',
                    'RetirementBenefitCost', 'ExpectedContribution', 'DCPlanCost',
                    'SensitivityDR_1pct', 'SensitivitySG_1pct',
                    'SensitivityDR_1pct_down', 'SensitivitySG_1pct_down',
                    'DiscountRate_Min', 'DiscountRate_Max', 'DiscountRate_Mid',
                    'SalaryGrowth_Min', 'SalaryGrowth_Max', 'SalaryGrowth_Mid',
                    'Duration_Min', 'Duration_Max', 'Duration_Mid']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --------------------------------------------------------
    # Step 1: Status 확인
    # --------------------------------------------------------
    print(f'\n[Step 1] Status 분포:')
    print(f'  {df["status"].value_counts().to_dict()}')

    # --------------------------------------------------------
    # Step 2: DR/SG 이상치 보정
    # --------------------------------------------------------
    print(f'\n[Step 2] DR/SG 이상치 보정')

    # 2-1: DR/SG에 금액이 들어간 건 → null 처리
    for prefix in ['DiscountRate', 'SalaryGrowth']:
        for suffix in ['Min', 'Max', 'Mid']:
            col = f'{prefix}_{suffix}'
            v = df[col]
            # 할인율/임금상승률은 절대값 30% 이하여야 정상
            mask_outlier = v.notna() & (v.abs() > 30)
            n_outlier = mask_outlier.sum()
            if n_outlier > 0:
                print(f'  {col}: {n_outlier}건 이상치(>30%) → null')
                df.loc[mask_outlier, col] = np.nan

    # 2-2: 소수 단위 보정 (0 < val < 1 → ×100)
    for prefix in ['DiscountRate', 'SalaryGrowth']:
        for suffix in ['Min', 'Max', 'Mid']:
            col = f'{prefix}_{suffix}'
            mask_decimal = df[col].notna() & (df[col] > 0) & (df[col] < 1)
            n_decimal = mask_decimal.sum()
            if n_decimal > 0:
                print(f'  {col}: {n_decimal}건 소수(0~1) → ×100')
                df.loc[mask_decimal, col] = df.loc[mask_decimal, col] * 100

    # 2-3: DR/SG 음수 → null (음수 할인율/임금상승률은 비정상)
    for prefix in ['DiscountRate', 'SalaryGrowth']:
        for suffix in ['Min', 'Max', 'Mid']:
            col = f'{prefix}_{suffix}'
            mask_neg = df[col].notna() & (df[col] < 0)
            n_neg = mask_neg.sum()
            if n_neg > 0:
                print(f'  {col}: {n_neg}건 음수 → null')
                df.loc[mask_neg, col] = np.nan

    # 2-4: Mid 재계산 (Min/Max 보정 후)
    for prefix in ['DiscountRate', 'SalaryGrowth']:
        mn = df[f'{prefix}_Min']
        mx = df[f'{prefix}_Max']
        md = df[f'{prefix}_Mid']
        # Min=Max인 단일값 기업: Mid = Min
        mask_single = mn.notna() & mx.notna() & (mn == mx)
        df.loc[mask_single, f'{prefix}_Mid'] = df.loc[mask_single, f'{prefix}_Min']
        # Min/Max 있고 Mid 없는 건: Mid = (Min+Max)/2
        mask_no_mid = mn.notna() & mx.notna() & md.isna()
        df.loc[mask_no_mid, f'{prefix}_Mid'] = (mn[mask_no_mid] + mx[mask_no_mid]) / 2

    # 보정 후 통계
    dr = df['DiscountRate_Mid']
    sg = df['SalaryGrowth_Mid']
    print(f'  보정 후: DR 유효={dr.notna().sum()}, 범위={dr.min():.2f}~{dr.max():.2f}%')
    print(f'  보정 후: SG 유효={sg.notna().sum()}, 범위={sg.min():.2f}~{sg.max():.2f}%')

    # --------------------------------------------------------
    # Step 3: Shock 환산 + 원본 매칭 (pension_extracts JSON)
    # --------------------------------------------------------
    print(f'\n[Step 3] Shock 판별 + [Step 4] 원본 매칭 검증')

    df['DR_shock_raw'] = np.nan
    df['SG_shock_raw'] = np.nan

    match_stats = {'match_direct': 0, 'match_pct': 0, 'match_reverse': 0,
                   'match_abs': 0, 'no_match': 0, 'no_value': 0, 'no_json': 0}
    shock_stats = {0.25: 0, 0.5: 0, 1.0: 0}
    n_shock_converted = 0

    has_sens = df['SensitivityDR_1pct'].notna()
    sens_idx = df[has_sens].index

    for idx in sens_idx:
        corp_code = df.loc[idx, 'corp_code']
        corp_code_padded = str(corp_code).zfill(8)
        json_path = os.path.join(ext_dir, f'{corp_code_padded}.json')

        if not os.path.exists(json_path):
            match_stats['no_json'] += 1
            continue

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        tables = data.get('tables', [])

        # Shock 판별
        dr_shock, sg_shock = detect_shock(tables)
        if dr_shock:
            df.loc[idx, 'DR_shock_raw'] = dr_shock
            if dr_shock in shock_stats:
                shock_stats[dr_shock] += 1
        if sg_shock:
            df.loc[idx, 'SG_shock_raw'] = sg_shock

        # Shock 환산 (0.5%p → ×2, 0.25%p → ×4)
        if dr_shock and dr_shock < 1:
            multiplier = 1.0 / dr_shock
            for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down']:
                val = df.loc[idx, col]
                if pd.notna(val):
                    df.loc[idx, col] = val * multiplier
            n_shock_converted += 1
        if sg_shock and sg_shock < 1:
            multiplier = 1.0 / sg_shock
            for col in ['SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
                val = df.loc[idx, col]
                if pd.notna(val):
                    df.loc[idx, col] = val * multiplier

        # 원본 매칭 검증
        sens_tables = find_sensitivity_tables(tables)
        if sens_tables:
            all_text = ' '.join(sens_tables)
            numbers = extract_numbers_from_text(all_text)
            dbo = df.loc[idx, 'DBO']

            for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down',
                        'SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
                val = df.loc[idx, col]
                result = validate_sensitivity_match(val, dbo, numbers)
                match_stats[result] += 1

    total_checked = sum(v for k, v in match_stats.items() if k != 'no_json')
    total_matched = sum(v for k, v in match_stats.items() if k.startswith('match_'))
    match_rate = total_matched / total_checked * 100 if total_checked > 0 else 0

    print(f'  Shock 분포: {shock_stats}')
    print(f'  Shock 환산: {n_shock_converted}건')
    print(f'  원본 매칭: {total_matched}/{total_checked} ({match_rate:.1f}%)')
    print(f'  매칭 상세: {match_stats}')

    # --------------------------------------------------------
    # Step 5: 환각 제거
    # --------------------------------------------------------
    print(f'\n[Step 5] 환각 제거')

    # 5-1: Duration > 50년 → null
    dur = df['Duration_Mid']
    mask_dur_halluc = dur.notna() & (dur > 50)
    n_dur = mask_dur_halluc.sum()
    if n_dur > 0:
        print(f'  Duration > 50년: {n_dur}건 → null')
        for col in ['Duration_Min', 'Duration_Max', 'Duration_Mid']:
            df.loc[mask_dur_halluc, col] = np.nan

    # 5-2: Duration 역산 체크 (SensitivityDR → Duration)
    has_both = df['SensitivityDR_1pct'].notna() & (df['DBO'] > 0)
    if has_both.any():
        dur_calc = df.loc[has_both, 'SensitivityDR_1pct'].abs() / (df.loc[has_both, 'DBO'] * 0.01)
        mask_halluc_dur = dur_calc > 50
        n_halluc = mask_halluc_dur.sum()
        if n_halluc > 0:
            print(f'  Duration 역산 > 50년: {n_halluc}건 → 민감도 null')
            halluc_idx = dur_calc[mask_halluc_dur].index
            for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down',
                        'SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
                df.loc[halluc_idx, col] = np.nan

    # 5-3: DBO=0 또는 NaN인데 민감도 존재 → null
    mask_no_dbo = (df['DBO'].isna() | (df['DBO'] == 0))
    for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down',
                'SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
        mask = mask_no_dbo & df[col].notna()
        n = mask.sum()
        if n > 0:
            print(f'  DBO 없는데 {col} 존재: {n}건 → null')
            df.loc[mask, col] = np.nan

    # 5-4: 민감도 테이블 없는데 값 존재 (n_tables <= 1)
    n_tables = pd.to_numeric(df['n_tables'], errors='coerce')
    mask_no_table = (n_tables <= 1) & df['SensitivityDR_1pct'].notna()
    n_no_table = mask_no_table.sum()
    if n_no_table > 0:
        # JSON 실제 확인 (n_tables=1이라도 민감도 테이블 있을 수 있음)
        actual_no_sens = 0
        for idx in df[mask_no_table].index:
            corp_code = str(df.loc[idx, 'corp_code']).zfill(8)
            json_path = os.path.join(ext_dir, f'{corp_code}.json')
            if os.path.exists(json_path):
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                sens_tables = find_sensitivity_tables(data.get('tables', []))
                if not sens_tables:
                    actual_no_sens += 1
                    for col in ['SensitivityDR_1pct', 'SensitivityDR_1pct_down',
                                'SensitivitySG_1pct', 'SensitivitySG_1pct_down']:
                        df.loc[idx, col] = np.nan
        if actual_no_sens > 0:
            print(f'  민감도 테이블 없는데 값 존재: {actual_no_sens}건 → null')

    # --------------------------------------------------------
    # Step 6: 비대칭 플래그
    # --------------------------------------------------------
    print(f'\n[Step 6] 비대칭 체크')

    has_both_dr = df['SensitivityDR_1pct'].notna() & df['SensitivityDR_1pct_down'].notna()
    if has_both_dr.any():
        dr_up = df.loc[has_both_dr, 'SensitivityDR_1pct'].abs()
        dr_down = df.loc[has_both_dr, 'SensitivityDR_1pct_down'].abs()
        ratio = dr_up / dr_down.replace(0, np.nan)
        mask_asym = (ratio > 2) | (ratio < 0.5)
        n_asym = mask_asym.sum()
        print(f'  DR 비대칭 (ratio>2 or <0.5): {n_asym}건')
        if n_asym > 0:
            asym_idx = ratio[mask_asym].index
            for i in asym_idx[:5]:
                print(f'    {df.loc[i,"corp_name"]}: up={df.loc[i,"SensitivityDR_1pct"]:.0f}, '
                      f'down={df.loc[i,"SensitivityDR_1pct_down"]:.0f}, ratio={ratio[i]:.2f}')

    # 부호 반전 체크
    # DR 증가 → DBO 감소 (음수여야 정상), DR 감소 → DBO 증가 (양수여야 정상)
    dr_up_val = df['SensitivityDR_1pct']
    dr_down_val = df['SensitivityDR_1pct_down']
    mask_sign_dr_up = dr_up_val.notna() & (dr_up_val > 0)  # 양수면 부호 반전
    mask_sign_dr_down = dr_down_val.notna() & (dr_down_val < 0)  # 음수면 부호 반전
    n_sign_up = mask_sign_dr_up.sum()
    n_sign_down = mask_sign_dr_down.sum()
    print(f'  DR 부호 반전: 증가시양수={n_sign_up}건, 감소시음수={n_sign_down}건')

    sg_up_val = df['SensitivitySG_1pct']
    sg_down_val = df['SensitivitySG_1pct_down']
    mask_sign_sg_up = sg_up_val.notna() & (sg_up_val < 0)  # 음수면 부호 반전
    mask_sign_sg_down = sg_down_val.notna() & (sg_down_val > 0)  # 양수면 부호 반전
    n_sign_sg_up = mask_sign_sg_up.sum()
    n_sign_sg_down = mask_sign_sg_down.sum()
    print(f'  SG 부호 반전: 증가시음수={n_sign_sg_up}건, 감소시양수={n_sign_sg_down}건')

    # --------------------------------------------------------
    # 최종 통계
    # --------------------------------------------------------
    print(f'\n[최종 통계]')
    ok = df[df['status'] == 'OK']
    has_dbo = ok['DBO'].notna() & (ok['DBO'] > 0)
    has_dr = ok['DiscountRate_Mid'].notna() & (ok['DiscountRate_Mid'] > 0)
    has_sg = ok['SalaryGrowth_Mid'].notna() & (ok['SalaryGrowth_Mid'] > 0)
    has_dur = ok['Duration_Mid'].notna() & (ok['Duration_Mid'] > 0)
    has_sens = ok['SensitivityDR_1pct'].notna()

    print(f'  전체: {len(df)}')
    print(f'  OK: {len(ok)}')
    print(f'  DBO有: {has_dbo.sum()}')
    print(f'  할인율有: {has_dr.sum()} ({has_dr.sum()/len(ok)*100:.1f}%)')
    print(f'  임금상승률有: {has_sg.sum()} ({has_sg.sum()/len(ok)*100:.1f}%)')
    print(f'  Duration有: {has_dur.sum()} ({has_dur.sum()/len(ok)*100:.1f}%)')
    print(f'  민감도有: {has_sens.sum()} ({has_sens.sum()/len(ok)*100:.1f}%)')

    if has_dbo.any():
        dbo_sum = ok.loc[has_dbo, 'DBO'].sum()
        print(f'  DBO 합계: {dbo_sum/1e12:.1f}조원')
        if has_dr.any():
            # DBO 가중평균 할인율
            mask = has_dbo & has_dr
            wt_dr = (ok.loc[mask, 'DiscountRate_Mid'] * ok.loc[mask, 'DBO']).sum() / ok.loc[mask, 'DBO'].sum()
            print(f'  할인율 DBO가중: {wt_dr:.2f}%')
        if has_dur.any():
            mask = has_dbo & has_dur
            wt_dur = (ok.loc[mask, 'Duration_Mid'] * ok.loc[mask, 'DBO']).sum() / ok.loc[mask, 'DBO'].sum()
            print(f'  Duration DBO가중: {wt_dur:.1f}년')

    # --------------------------------------------------------
    # merged CSV 저장
    # --------------------------------------------------------
    # 컬럼 순서를 2020 merged와 동일하게
    col_order = ['corp_code', 'corp_name', 'rcept_no', 'year', 'fs_type', 'status',
                 'extract_method', 'n_tables', 'input_tokens', 'output_tokens',
                 'DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
                 'InterestIncome', 'NetInterest', 'BenefitPayment', 'ActuarialGL',
                 'ActuarialGL_Financial', 'ActuarialGL_Demographic', 'ActuarialGL_Experience',
                 'RetirementBenefitCost', 'ExpectedContribution', 'DCPlanCost',
                 'SensitivityDR_1pct', 'SensitivitySG_1pct',
                 'DiscountRate_Min', 'DiscountRate_Max', 'DiscountRate_Mid',
                 'SalaryGrowth_Min', 'SalaryGrowth_Max', 'SalaryGrowth_Mid',
                 'Duration_Min', 'Duration_Max', 'Duration_Mid',
                 'SensitivityDR_1pct_down', 'SensitivitySG_1pct_down',
                 'DR_shock_raw', 'SG_shock_raw']

    # 없는 컬럼은 NaN
    for c in col_order:
        if c not in df.columns:
            df[c] = np.nan

    df = df[col_order]
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f'\n  → {out_csv} 저장 ({len(df)}건)')

    return df


if __name__ == '__main__':
    years = [int(y) for y in sys.argv[1:]] if len(sys.argv) > 1 else [2014, 2015, 2016, 2017]
    for year in years:
        validate_year(year)
