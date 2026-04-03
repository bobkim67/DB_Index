"""
dart_extract_save.py — DART 사업보고서 퇴직연금 발췌 텍스트 저장 (LLM 미호출)

corp_5yr_list_v2.csv 기반 5개년(2020~2024) 발췌 텍스트를 법인별 JSON으로 저장.
DART API → XML 다운로드 → 퇴직급여 섹션 발췌 → pension_extracts/{year}/{corp_code}.json

dart_llm_batch.py의 발췌 로직(v4: heading 기반 → 키워드 fallback)을 그대로 사용.
"""

import requests, ssl, zipfile, io, re, json, sys, time, os
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

sys.stdout.reconfigure(encoding='utf-8')

# === 설정 ===
DART_API_KEY = os.environ.get('DART_API_KEY', '')

BASE = Path(__file__).parent
CORP_CSV = BASE / 'corp_5yr_list_v2.csv'
EXTRACT_DIR = BASE / 'pension_extracts'
LOG_FILE = BASE / 'extract_save_log.txt'

# === dart_llm_batch.py에서 발췌 로직 임포트 ===
from dart_llm_batch import (
    download_consolidated_xml,
    extract_pension_tables,
    session,  # SSL 세션 재사용
)


def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def save_extract(corp_code, corp_name, year, rcept_no, tables, method, fs_type='연결'):
    """발췌 결과를 JSON으로 저장"""
    year_dir = EXTRACT_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    data = {
        'corp_code': corp_code,
        'corp_name': corp_name,
        'year': year,
        'rcept_no': rcept_no,
        'fs_type': fs_type,
        'extract_method': method,
        'n_tables': len(tables),
        'tables': tables,
    }

    out_path = year_dir / f'{corp_code}.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

    return out_path


def get_done_set(year):
    """이미 저장된 법인코드 set"""
    year_dir = EXTRACT_DIR / str(year)
    if not year_dir.exists():
        return set()
    done = set()
    for f in year_dir.glob('*.json'):
        done.add(f.stem)
    return done


def process_year(df, year, rate_limit=1.0):
    """단일 연도 전체 처리"""
    rcept_col = f'rcept_{year}'
    targets = df[df[rcept_col].notna() & (df[rcept_col] != '')].copy()
    total = len(targets)
    log(f'\n=== {year}년 시작: {total}사 ===')

    done = get_done_set(year)
    # 퇴직연금없음도 저장하므로, done에 있으면 스킵
    remaining = targets[~targets['corp_code'].isin(done)]
    log(f'이미 저장: {len(done)}사 -> 잔여: {len(remaining)}사')

    if remaining.empty:
        return

    n_ok = 0
    n_pension_none = 0
    n_err = 0

    for i, (_, row) in enumerate(remaining.iterrows()):
        corp_code = row['corp_code']
        corp_name = row['corp_name']
        rcept_no = row[rcept_col]
        progress = f'[{len(done)+i+1}/{total}]'

        # 1) XML 다운로드
        content, status = download_consolidated_xml(rcept_no)

        if content is None:
            # 퇴직연금없음/ZIP아님 등도 기록 (재처리 방지)
            year_dir = EXTRACT_DIR / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            data = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'year': year,
                'rcept_no': rcept_no,
                'status': status,
                'extract_method': None,
                'n_tables': 0,
                'tables': [],
            }
            with open(year_dir / f'{corp_code}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=1)

            if '퇴직연금없음' in status:
                n_pension_none += 1
            else:
                n_err += 1
                if 'API오류' in status:
                    log(f'{progress} {corp_name}: {status} -> 60초 대기')
                    time.sleep(60)
            continue

        # 2) 연결/별도 플래그 파싱
        fs_type = '별도'
        if '|' in status:
            fs_type = status.split('|')[1]

        # 3) 발췌
        tables, method = extract_pension_tables(content)

        if not tables:
            # 테이블 없음도 기록
            year_dir = EXTRACT_DIR / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            data = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'year': year,
                'rcept_no': rcept_no,
                'fs_type': fs_type,
                'status': '테이블없음',
                'extract_method': method,
                'n_tables': 0,
                'tables': [],
            }
            with open(year_dir / f'{corp_code}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
            n_pension_none += 1
            continue

        # 4) 저장
        save_extract(corp_code, corp_name, year, rcept_no, tables, method, fs_type)
        n_ok += 1

        # 진행 출력 (100건마다)
        if (i + 1) % 100 == 0:
            log(f'{progress} {year} {corp_name} | OK={n_ok} none={n_pension_none} err={n_err}')

        # DART rate limit
        time.sleep(rate_limit)

    log(f'=== {year}년 완료: OK={n_ok}, 퇴직연금없음={n_pension_none}, 오류={n_err} ===')


def main():
    df = pd.read_csv(CORP_CSV, dtype=str)
    total_corps = len(df)
    log(f'전체 법인: {total_corps}사')
    EXTRACT_DIR.mkdir(exist_ok=True)

    # 연도 순서: 최신 → 과거 (2024가 가장 중요)
    years = [2024, 2023, 2022, 2021, 2020]

    # CLI 인자로 특정 연도만 실행 가능: python dart_extract_save.py 2023
    if len(sys.argv) > 1:
        years = [int(y) for y in sys.argv[1:]]
        log(f'지정 연도: {years}')

    for year in years:
        process_year(df, year, rate_limit=0.7)

    # 최종 용량 확인
    total_size = 0
    total_files = 0
    for year in range(2020, 2025):
        year_dir = EXTRACT_DIR / str(year)
        if year_dir.exists():
            files = list(year_dir.glob('*.json'))
            size = sum(f.stat().st_size for f in files)
            total_files += len(files)
            total_size += size
            log(f'{year}: {len(files)}파일, {size/1e6:.1f}MB')

    log(f'합계: {total_files}파일, {total_size/1e6:.1f}MB')


if __name__ == '__main__':
    main()
