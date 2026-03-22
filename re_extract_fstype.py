"""별도 라벨 건 재발췌 — DART API → 수정된 발췌 로직 → JSON 갱신 (LLM 미호출)
연결로 변경된 건만 JSON 덮어쓰기. 변경 건수 집계.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json, time, re
from pathlib import Path

from dart_llm_batch import (
    download_consolidated_xml, extract_pension_tables,
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


def main():
    log('\n=== 별도→연결 재발췌 시작 ===')

    # 별도 라벨 건 수집
    targets = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_dir = EXTRACT_DIR / str(year)
        for fp in sorted(year_dir.glob('*.json')):
            with open(fp, encoding='utf-8') as f:
                d = json.load(f)
            if d.get('fs_type') == '별도':
                targets.append((year, fp.stem, d))

    log(f'별도 라벨 총: {len(targets)}건')

    n_changed = 0      # 연결로 변경됨
    n_improved = 0      # 발췌 키워드 개선됨
    n_unchanged = 0     # 그대로 별도
    n_err = 0
    changed_list = []   # (year, corp_code, corp_name) 변경된 건

    for i, (year, corp_code, old_data) in enumerate(targets):
        rcept_no = old_data.get('rcept_no', '')
        corp_name = old_data.get('corp_name', '')

        if not rcept_no:
            n_unchanged += 1
            continue

        # DART API
        content, status = download_consolidated_xml(rcept_no)
        if content is None:
            n_unchanged += 1
            continue

        fs_type_new = '별도'
        if '|' in status:
            fs_type_new = status.split('|')[1]

        # 발췌
        tables, method = extract_pension_tables(content)

        # 변경 판단
        changed = False
        if fs_type_new == '연결' and old_data.get('fs_type') == '별도':
            changed = True
            n_changed += 1

        # 발췌 키워드 개선 확인
        old_tables = old_data.get('tables', [])
        old_text = re.sub(r'<[^>]+>', ' ', ' '.join(old_tables))
        old_has_kw = any(kw in old_text for kw in PENSION_KW)
        new_text = re.sub(r'<[^>]+>', ' ', ' '.join(tables)) if tables else ''
        new_has_kw = any(kw in new_text for kw in PENSION_KW)
        if not old_has_kw and new_has_kw:
            changed = True
            n_improved += 1

        if changed:
            # JSON 덮어쓰기
            new_data = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'year': year,
                'rcept_no': rcept_no,
                'fs_type': fs_type_new,
                'extract_method': method,
                'n_tables': len(tables) if tables else 0,
                'tables': tables if tables else [],
            }
            json_path = EXTRACT_DIR / str(year) / f'{corp_code}.json'
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, ensure_ascii=False, indent=1)
            changed_list.append((year, corp_code, corp_name, fs_type_new))
        else:
            n_unchanged += 1

        if (i + 1) % 100 == 0:
            log(f'  [{i+1}/{len(targets)}] {corp_name} | 연결전환={n_changed} 개선={n_improved} 유지={n_unchanged}')

        time.sleep(0.7)

    log(f'=== 완료: 연결전환={n_changed} 발췌개선={n_improved} 유지={n_unchanged} ===')

    # 변경 건 저장
    if changed_list:
        with open(BASE / 'fstype_changed.json', 'w', encoding='utf-8') as f:
            json.dump(changed_list, f, ensure_ascii=False, indent=1)
        log(f'변경 건 목록: fstype_changed.json ({len(changed_list)}건)')

        # 연도별 요약
        from collections import Counter
        year_counts = Counter(y for y, _, _, _ in changed_list)
        for y in sorted(year_counts):
            log(f'  {y}: {year_counts[y]}건 변경')


if __name__ == '__main__':
    main()
