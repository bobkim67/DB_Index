"""YTM 시계열 xlsx → 월말 spot rate curve(EAR) 산출
- 입력: ytm/{period}_{grade}.xlsx (일별 YTM, 만기별)
- 로직: 선형보간(0.5년 단위) → 반기 쿠폰 par bond bootstrapping → EAR 변환
- 출력: ytm/spot_rate_ear.csv (일자 × 등급 × tenor → EAR%)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).parent
YTM_DIR = BASE / 'ytm'

GRADES = ['국고채', 'AAA', 'AA+', 'AA', 'AA-']
PERIODS = ['20200101_20211231', '20220101_20231231', '20240101_20251231']

# YTM 컬럼 → tenor(년)
TENOR_MAP = {
    '3M': 0.25, '6M': 0.5, '9M': 0.75,
    '1Y': 1, '1.5Y': 1.5, '2Y': 2, '2.5Y': 2.5,
    '3Y': 3, '4Y': 4, '5Y': 5,
    '7Y': 7, '10Y': 10, '15Y': 15, '20Y': 20,
    '30Y': 30, '50Y': 50,
}

# 산출 tenor: 0.5년 ~ 30년 (0.5년 단위)
TARGET_TENORS = [t * 0.5 for t in range(1, 61)]


def bootstrap_spot_ear(ytm_dict):
    """YTM dict {tenor: ytm%} → spot rate EAR dict {tenor: ear%}

    1. 선형보간 (0.5년 단위)
    2. 반기 쿠폰 par bond bootstrapping
    3. APR → EAR 변환
    """
    # 유효 포인트만 (tenor >= 0.5)
    pts = {t: v for t, v in ytm_dict.items() if t >= 0.5 and v is not None}
    if len(pts) < 2:
        return None

    pts_t = sorted(pts.keys())
    pts_v = [pts[t] for t in pts_t]

    # 선형보간
    ytm_interp = np.interp(TARGET_TENORS, pts_t, pts_v)
    ytm_curve = {t: y / 100 for t, y in zip(TARGET_TENORS, ytm_interp)}

    # Bootstrapping
    spot_ear = {}
    spot_half = {}  # 반기 spot rate 저장 (bootstrapping용)

    for tenor in TARGET_TENORS:
        n = int(tenor * 2)  # 반기 수
        ytm = ytm_curve[tenor]
        c_half = ytm / 2  # 반기 쿠폰율

        if n == 1:
            s_h = ytm / 2  # 0.5년: 반기 rate = ytm/2
        else:
            pv_coupons = 0
            for i in range(1, n):
                s_prev = spot_half[i * 0.5]
                pv_coupons += (c_half * 100) / (1 + s_prev) ** i

            remaining = 100 - pv_coupons
            face_coupon = c_half * 100 + 100
            s_h = (face_coupon / remaining) ** (1 / n) - 1

        spot_half[tenor] = s_h
        spot_ear[tenor] = ((1 + s_h) ** 2 - 1) * 100  # EAR %

    return spot_ear


def load_ytm_timeseries():
    """모든 YTM xlsx 로드 → DataFrame (일자, 등급, 만기별 YTM)"""
    all_rows = []

    for period in PERIODS:
        for grade in GRADES:
            fp = YTM_DIR / f'{period}_{grade}.xlsx'
            if not fp.exists():
                print(f'  파일 없음: {fp.name}')
                continue

            df = pd.read_excel(fp)
            df['일자'] = pd.to_datetime(df['일자'])

            for _, row in df.iterrows():
                ytm_dict = {}
                for col, tenor in TENOR_MAP.items():
                    val = row.get(col)
                    if val is not None and val != '-' and pd.notna(val):
                        try:
                            ytm_dict[tenor] = float(val)
                        except (ValueError, TypeError):
                            pass
                if ytm_dict:
                    all_rows.append({
                        'date': row['일자'],
                        'grade': grade,
                        'ytm': ytm_dict,
                    })

    return all_rows


def main():
    print('=== YTM 로드 ===')
    ytm_data = load_ytm_timeseries()
    print(f'총 {len(ytm_data)}행 (일자 × 등급)')

    # 월말만 필터
    df_all = pd.DataFrame(ytm_data)
    df_all['year_month'] = df_all['date'].dt.to_period('M')

    # 각 월의 마지막 영업일 선택
    monthly = df_all.loc[df_all.groupby(['grade', 'year_month'])['date'].idxmax()]
    print(f'월말 {len(monthly)}행')

    # Bootstrapping
    print('\n=== Bootstrapping ===')
    results = []
    for _, row in monthly.iterrows():
        spot = bootstrap_spot_ear(row['ytm'])
        if spot is None:
            continue
        for tenor, ear in spot.items():
            results.append({
                'date': row['date'].strftime('%Y-%m-%d'),
                'grade': row['grade'],
                'tenor': tenor,
                'spot_ear': round(ear, 6),
            })

    df_out = pd.DataFrame(results)
    print(f'산출: {len(df_out)}행 ({df_out.date.nunique()}월 × {len(GRADES)}등급 × {len(TARGET_TENORS)}tenor)')

    # 피봇: 행=date×grade, 열=tenor
    out_path = YTM_DIR / 'spot_rate_ear_v2.csv'
    df_out.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'저장: {out_path}')

    # 요약
    print(f'\n=== 요약 ===')
    print(f'기간: {df_out.date.min()} ~ {df_out.date.max()}')
    print(f'등급: {df_out.grade.unique().tolist()}')
    print(f'월수: {df_out.date.nunique()}')
    print(f'tenor: {df_out.tenor.min()} ~ {df_out.tenor.max()}년')


if __name__ == '__main__':
    main()
