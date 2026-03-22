"""XBRL vs LLM 교차검증 테스트"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import numpy as np

# === 로드 ===
xbrl = pd.read_csv('pension_cik_mapping.csv', dtype=str, encoding='utf-8-sig')
xbrl_24 = xbrl[xbrl['quarter'] == '2024_4Q'].copy()
xbrl_24['val'] = pd.to_numeric(xbrl_24['sample_VALUE'], errors='coerce')
xbrl_24['CIK'] = xbrl_24['CIK'].str.strip()

llm = pd.read_csv('llm_extract_2024.csv', encoding='utf-8-sig')
llm['corp_code'] = llm['corp_code'].astype(str).str.zfill(8)

dbo_type = pd.read_csv('pension_dbo_type.csv', dtype=str, encoding='utf-8-sig')
dbo_type['CIK'] = dbo_type['CIK'].str.strip()
dbo_map = dict(zip(dbo_type.CIK, dbo_type.dbo_type))

common = set(xbrl_24.CIK.unique()) & set(llm.corp_code)
print(f'공통 CIK: {len(common)}')

# === 변수별 비교 ===
AMOUNT_VARS = ['DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
               'InterestIncome', 'BenefitPayment', 'ActuarialGL', 'NetInterest',
               'RetirementBenefitCost']
RATE_VARS = ['DiscountRate', 'SalaryGrowth', 'Duration']

rows = []
for cik in common:
    lr = llm[llm.corp_code == cik]
    if lr.empty:
        continue
    lr = lr.iloc[0]
    xc = xbrl_24[xbrl_24.CIK == cik]
    dt = dbo_map.get(cik, '')

    for var in AMOUNT_VARS:
        xr = xc[xc.variable == var]
        if xr.empty:
            continue
        xval = xr.iloc[0]['val']
        lval = pd.to_numeric(lr.get(var), errors='coerce')
        if pd.isna(xval) or pd.isna(lval) or xval == 0:
            continue
        ratio = lval / xval
        rel_err = abs(lval - xval) / abs(xval)
        rows.append({
            'CIK': cik, 'name': lr.corp_name, 'variable': var,
            'xbrl_val': xval, 'llm_val': lval, 'ratio': ratio, 'rel_err': rel_err,
            'unit': xr.iloc[0]['UNIT_ID'], 'dbo_type': dt,
            'xbrl_eid': xr.iloc[0]['ELEMENT_ID'],
        })

    for var in RATE_VARS:
        xr = xc[xc.variable == var]
        if xr.empty:
            continue
        xval = xr.iloc[0]['val']
        lval = pd.to_numeric(lr.get(f'{var}_Mid'), errors='coerce')
        unit = xr.iloc[0]['UNIT_ID']
        if pd.isna(xval) or pd.isna(lval):
            continue
        # XBRL PURE(0.045) → %(4.5) 변환
        xval_cmp = xval * 100 if (unit == 'PURE' and abs(xval) < 1) else xval
        abs_diff = abs(lval - xval_cmp)
        rows.append({
            'CIK': cik, 'name': lr.corp_name, 'variable': var,
            'xbrl_val': xval, 'llm_val': lval,
            'ratio': lval / xval_cmp if xval_cmp != 0 else np.nan,
            'rel_err': abs_diff, 'unit': unit, 'dbo_type': dt,
            'xbrl_eid': xr.iloc[0]['ELEMENT_ID'],
        })

df = pd.DataFrame(rows)
print(f'비교 가능 건수: {len(df)}')
print()

# === 변수별 일치율 ===
print('=' * 90)
print(f'{"변수":25s} | {"비교":>5s} | {"일치":>8s} | {"근사":>8s} | {"불일치":>8s}')
print('=' * 90)
for var in AMOUNT_VARS:
    sub = df[df.variable == var]
    if sub.empty:
        continue
    exact = (sub.rel_err < 0.001).sum()
    close = (sub.rel_err < 0.05).sum()
    far = (sub.rel_err >= 0.05).sum()
    n = len(sub)
    print(f'{var:25s} | {n:5d} | {exact:4d}({exact/n*100:4.1f}%) | {close:4d}({close/n*100:4.1f}%) | {far:4d}({far/n*100:4.1f}%)')

print('-' * 90)
for var in RATE_VARS:
    sub = df[df.variable == var]
    if sub.empty:
        continue
    m1 = (sub.rel_err < 0.1).sum()   # 0.1%p 이내
    m5 = (sub.rel_err < 0.5).sum()   # 0.5%p 이내
    far = (sub.rel_err >= 0.5).sum()
    n = len(sub)
    print(f'{var:25s} | {n:5d} | <0.1%p={m1:4d}({m1/n*100:4.1f}%) | <0.5%p={m5:4d}({m5/n*100:4.1f}%) | >0.5%p={far:4d}({far/n*100:4.1f}%)')

# === 금액 ratio 분포 (단위 오류 패턴) ===
print()
print('=== 금액 변수 ratio(LLM/XBRL) 분포 ===')
amt_df = df[df.variable.isin(AMOUNT_VARS)]
r = amt_df.ratio.dropna()
if not r.empty:
    bins = [(-np.inf, -10), (-10, -1.05), (-1.05, -0.95), (-0.95, -0.5),
            (-0.5, 0), (0, 0.5), (0.5, 0.95), (0.95, 1.05),
            (1.05, 2), (2, 10), (10, 100), (100, 1000), (1000, np.inf)]
    labels = ['<-10', '-10~-1.05', '-1.05~-0.95', '-0.95~-0.5',
              '-0.5~0', '0~0.5', '0.5~0.95', '*0.95~1.05*',
              '1.05~2', '2~10', '10~100', '100~1K', '>1K']
    for lo, hi in bins:
        cnt = ((r >= lo) & (r < hi)).sum()
        if cnt > 0:
            label = labels[bins.index((lo, hi))]
            bar = '#' * min(cnt, 80)
            print(f'  {label:>15s}: {cnt:5d} {bar}')

# === DBO 불일치 상세 (ratio != ~1.0) ===
print()
print('=== DBO 불일치 상세 TOP 15 ===')
dbo = df[df.variable == 'DBO'].copy()
dbo = dbo[dbo.rel_err >= 0.05].sort_values('rel_err', ascending=False).head(15)
for _, row in dbo.iterrows():
    print(f"  {row.CIK} {str(row['name']):15s} XBRL={row.xbrl_val:>15,.0f} LLM={row.llm_val:>15,.0f} "
          f"ratio={row.ratio:8.3f} dbo_type={row.dbo_type} eid={row.xbrl_eid[:50]}")

# === 비율 변수 불일치 상세 ===
print()
print('=== 할인율 불일치 (>0.5%p) 상세 ===')
dr = df[(df.variable == 'DiscountRate') & (df.rel_err >= 0.5)].sort_values('rel_err', ascending=False).head(10)
for _, row in dr.iterrows():
    xpct = row.xbrl_val * 100 if row.xbrl_val < 1 else row.xbrl_val
    print(f"  {row.CIK} {str(row['name']):15s} XBRL={xpct:.2f}% LLM={row.llm_val:.2f}% diff={row.rel_err:.2f}%p eid={row.xbrl_eid[:60]}")
