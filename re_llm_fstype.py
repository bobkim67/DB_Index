"""fstype_changed.json 대상 LLM 재추출 → CSV 행 교체"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json, time, re
import pandas as pd
from pathlib import Path

from dart_llm_batch import (
    call_llm, AMOUNT_VARS, RANGE_VARS,
)

BASE = Path(__file__).parent
EXTRACT_DIR = BASE / 'pension_extracts'
LOG_FILE = BASE / 'llm_batch_log.txt'


def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def main():
    with open(BASE / 'fstype_changed.json', encoding='utf-8') as f:
        changed = json.load(f)

    log(f'\n=== fstype 변경 건 LLM 재추출 시작: {len(changed)}건 ===')

    # 연도별로 묶기
    by_year = {}
    for year, corp_code, corp_name, fs_type in changed:
        by_year.setdefault(year, []).append((corp_code, corp_name, fs_type))

    for year in sorted(by_year.keys()):
        items = by_year[year]
        out_csv = BASE / f'llm_extract_{year}.csv'
        llm_df = pd.read_csv(out_csv, encoding='utf-8-sig')
        llm_df['corp_code'] = llm_df['corp_code'].astype(str).str.zfill(8)

        n_ok = 0
        n_err = 0
        total_input = 0
        total_output = 0

        for i, (corp_code, corp_name, fs_type) in enumerate(items):
            json_path = EXTRACT_DIR / str(year) / f'{corp_code}.json'
            with open(json_path, encoding='utf-8') as f:
                d = json.load(f)

            tables = d.get('tables', [])
            method = d.get('extract_method', '')
            if not tables:
                continue

            tables_text = '\n\n'.join(tables)

            try:
                llm_result, usage = call_llm(tables_text, year)
                total_input += usage['input_tokens']
                total_output += usage['output_tokens']

                result_row = {
                    'corp_code': corp_code,
                    'corp_name': corp_name,
                    'rcept_no': d.get('rcept_no', ''),
                    'year': year,
                    'fs_type': fs_type,
                    'extract_method': method,
                    'n_tables': len(tables),
                    'input_tokens': usage['input_tokens'],
                    'output_tokens': usage['output_tokens'],
                }
                for var in AMOUNT_VARS:
                    result_row[var] = llm_result.get(var)
                for var in RANGE_VARS:
                    result_row[f'{var}_Min'] = llm_result.get(f'{var}_Min')
                    result_row[f'{var}_Max'] = llm_result.get(f'{var}_Max')
                    result_row[f'{var}_Mid'] = llm_result.get(f'{var}_Mid')

                essential = ['DBO', 'PlanAsset', 'ServiceCost', 'InterestCost',
                             'BenefitPayment', 'DiscountRate_Mid', 'Duration_Mid']
                n_filled = sum(1 for v in essential if result_row.get(v) is not None)
                result_row['status'] = 'OK' if n_filled >= 5 else f'부분({n_filled}/7)'

                mask = llm_df.corp_code == corp_code
                if mask.any():
                    idx = llm_df[mask].index[0]
                    for col, val in result_row.items():
                        llm_df.at[idx, col] = val
                else:
                    llm_df = pd.concat([llm_df, pd.DataFrame([result_row])], ignore_index=True)

                n_ok += 1

            except Exception as e:
                n_err += 1
                log(f'  LLM오류: {corp_name} - {e}')
                if '429' in str(e):
                    time.sleep(30)
                elif '500' in str(e) or '529' in str(e):
                    time.sleep(10)

            if (i + 1) % 50 == 0:
                cost = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
                log(f'  {year} [{i+1}/{len(items)}] {corp_name} | ok={n_ok} err={n_err} | ${cost:.2f}')

        llm_df.to_csv(out_csv, index=False, encoding='utf-8-sig')
        cost = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
        log(f'=== {year}년 완료: ok={n_ok} err={n_err} | ${cost:.2f} ===')


if __name__ == '__main__':
    main()
