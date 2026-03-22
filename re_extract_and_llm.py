"""재발췌 + LLM 재추출 — 키워드 미포함 건만 대상
1) re_extract_targets.json에서 대상 읽기
2) DART API → XML 다운로드 → 수정된 발췌 로직 적용
3) 개선된 건만 pension_extracts JSON 덮어쓰기
4) 개선된 건만 LLM 재추출 → llm_extract_{year}.csv 행 교체
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json, time, re, os
import pandas as pd
from pathlib import Path

# dart_llm_batch.py에서 함수 임포트 (수정된 _find_pension_section 포함)
from dart_llm_batch import (
    download_consolidated_xml, extract_pension_tables,
    call_llm, CSV_COLUMNS, AMOUNT_VARS, RANGE_VARS,
)

BASE = Path(__file__).parent
EXTRACT_DIR = BASE / 'pension_extracts'
LOG_FILE = BASE / 'llm_batch_log.txt'

PENSION_KW = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가',
              '순확정급여', '퇴직급여부채']


def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def has_pension_keywords(tables):
    """테이블 리스트에 퇴직급여 핵심 키워드 포함 여부"""
    text = re.sub(r'<[^>]+>', ' ', ' '.join(tables))
    return any(kw in text for kw in PENSION_KW)


def process_targets(year, corp_codes):
    """단일 연도의 대상 법인들 재발췌 + LLM 재추출"""
    year_dir = EXTRACT_DIR / str(year)
    out_csv = BASE / f'llm_extract_{year}.csv'

    # 기존 LLM 결과 로드
    llm_df = pd.read_csv(out_csv, encoding='utf-8-sig')
    llm_df['corp_code'] = llm_df['corp_code'].astype(str).str.zfill(8)
    original_len = len(llm_df)

    n_improved = 0
    n_llm_ok = 0
    n_skip = 0
    n_err = 0
    total_input = 0
    total_output = 0

    for i, corp_code in enumerate(corp_codes):
        # 기존 JSON에서 rcept_no, corp_name 가져오기
        json_path = year_dir / f'{corp_code}.json'
        if not json_path.exists():
            n_skip += 1
            continue

        with open(json_path, encoding='utf-8') as f:
            old_data = json.load(f)

        rcept_no = old_data.get('rcept_no', '')
        corp_name = old_data.get('corp_name', '')

        if not rcept_no:
            n_skip += 1
            continue

        # 1) XML 재다운로드
        content, status = download_consolidated_xml(rcept_no)
        if content is None:
            n_skip += 1
            continue

        fs_type = '별도'
        if '|' in status:
            fs_type = status.split('|')[1]

        # 2) 수정된 발췌 로직 적용
        tables, method = extract_pension_tables(content)

        # 3) 개선 여부 판단
        if not tables or not has_pension_keywords(tables):
            # 새 로직으로도 키워드 없음 → 스킵 (기존 유지)
            n_skip += 1
            continue

        # 개선됨 → JSON 덮어쓰기
        n_improved += 1
        new_data = {
            'corp_code': corp_code,
            'corp_name': corp_name,
            'year': year,
            'rcept_no': rcept_no,
            'fs_type': fs_type,
            'extract_method': method,
            'n_tables': len(tables),
            'tables': tables,
        }
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=1)

        # 4) LLM 재추출
        tables_text = '\n\n'.join(tables)
        try:
            llm_result, usage = call_llm(tables_text, year)
            total_input += usage['input_tokens']
            total_output += usage['output_tokens']

            result_row = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'rcept_no': rcept_no,
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

            # CSV에서 해당 행 교체
            mask = llm_df.corp_code == corp_code
            if mask.any():
                idx = llm_df[mask].index[0]
                for col, val in result_row.items():
                    llm_df.at[idx, col] = val
            else:
                llm_df = pd.concat([llm_df, pd.DataFrame([result_row])], ignore_index=True)

            n_llm_ok += 1

        except Exception as e:
            n_err += 1
            log(f'  LLM오류: {corp_name} - {e}')
            if '429' in str(e):
                time.sleep(30)
            elif '500' in str(e) or '529' in str(e):
                time.sleep(10)

        # 진행 출력 (50건마다)
        if (i + 1) % 50 == 0:
            cost = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
            log(f'  [{i+1}/{len(corp_codes)}] {corp_name} | 개선={n_improved} LLM={n_llm_ok} skip={n_skip} err={n_err} | ${cost:.2f}')

        # DART rate limit
        time.sleep(0.7)

    # CSV 저장
    llm_df.to_csv(out_csv, index=False, encoding='utf-8-sig')

    cost_total = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
    log(f'=== {year}년 재추출 완료: 대상={len(corp_codes)} 개선={n_improved} LLM={n_llm_ok} skip={n_skip} err={n_err} | ${cost_total:.2f} ===')

    return n_improved, n_llm_ok


def main():
    with open(BASE / 're_extract_targets.json', encoding='utf-8') as f:
        targets = json.load(f)

    log(f'\n=== 재발췌+재추출 시작 ===')
    for year_str, corp_codes in targets.items():
        year = int(year_str)
        log(f'\n--- {year}년: {len(corp_codes)}건 ---')
        process_targets(year, corp_codes)


if __name__ == '__main__':
    main()
