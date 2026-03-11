"""DART API로 2020~2024 사업보고서 5개년 모두 존재하는 상장사 리스트 추출"""
import requests, ssl, zipfile, io, json, sys, time, re, csv, os
sys.stdout.reconfigure(encoding='utf-8')
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import warnings
warnings.filterwarnings('ignore')

class DARTAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.set_ciphers('DEFAULT:@SECLEVEL=1')
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

s = requests.Session()
s.mount('https://', DARTAdapter())
API_KEY = '56a07e920d1f7f0e9aed6c3bc6a62491c21620c2'

CACHE_FILE = 'corp_code_cache.json'
PROGRESS_FILE = 'dart_5yr_progress.json'
TARGET_YEARS = {2020, 2021, 2022, 2023, 2024}
SLEEP_PER_REQ = 1.0  # 건당 1초


def get_listed_corps():
    """상장사 목록 (캐시 사용)"""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding='utf-8') as f:
            listed = json.load(f)
        print(f'캐시에서 로드: {len(listed)}개 상장사')
        return listed

    url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}'
    r = s.get(url, verify=False, timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(z.read(z.namelist()[0]).decode('utf-8'))

    listed = []
    for corp in root.findall('.//list'):
        stock = corp.findtext('stock_code', '').strip()
        if stock:
            listed.append({
                'corp_code': corp.findtext('corp_code', ''),
                'corp_name': corp.findtext('corp_name', ''),
                'stock_code': stock,
            })

    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(listed, f, ensure_ascii=False)
    print(f'corpCode.xml 다운로드 완료: {len(listed)}개 상장사 (캐시 저장)')
    return listed


def query_annual_reports(corp_code, max_retries=3):
    """특정 기업의 사업보고서 rcept_no 조회 (retry 포함)"""
    url = (f'https://opendart.fss.or.kr/api/list.json?crtfc_key={API_KEY}'
           f'&corp_code={corp_code}&bgn_de=20200101&end_de=20260101'
           f'&pblntf_ty=A&page_count=30')

    for attempt in range(max_retries):
        try:
            r = s.get(url, verify=False, timeout=10)
            data = json.loads(r.content.decode('utf-8'))

            if data.get('status') == '020':
                # 요청 제한
                time.sleep(2)
                continue

            year_rcept = {}
            if data.get('status') == '000':
                for item in data['list']:
                    rn = item['report_nm']
                    if '사업보고서' in rn:
                        m = re.search(r'\((\d{4})\.\d{2}\)', rn)
                        if m:
                            yr = int(m.group(1))
                            if yr in TARGET_YEARS:
                                year_rcept[yr] = item['rcept_no']
            return year_rcept

        except Exception:
            time.sleep(1)

    return {}


def load_progress():
    """이전 진행 상태 로드"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {'done_codes': {}, 'errors': 0}


def save_progress(done_codes, errors):
    """진행 상태 저장"""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump({'done_codes': done_codes, 'errors': errors}, f, ensure_ascii=False)


def save_csv(results):
    """결과 CSV 저장"""
    full5 = [r for r in results if set(r['years']) >= TARGET_YEARS]
    with open('corp_5yr_list.csv', 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['corp_code', 'corp_name', 'stock_code',
                     'rcept_2020', 'rcept_2021', 'rcept_2022', 'rcept_2023', 'rcept_2024'])
        for r in full5:
            w.writerow([
                r['corp_code'], r['corp_name'], r['stock_code'],
                r['rcept_nos'].get(2020, ''),
                r['rcept_nos'].get(2021, ''),
                r['rcept_nos'].get(2022, ''),
                r['rcept_nos'].get(2023, ''),
                r['rcept_nos'].get(2024, ''),
            ])
    return len(full5)


def main():
    listed = get_listed_corps()
    print(f'상장사: {len(listed)}개\n')

    # 이전 진행 상태 로드
    progress = load_progress()
    done_codes = progress['done_codes']  # {corp_code: {year: rcept_no, ...} or "empty"}
    errors = progress['errors']

    if done_codes:
        print(f'이전 진행 이어받기: {len(done_codes)}개 완료, {len(listed) - len(done_codes)}개 남음\n')

    results = []
    # 이전 완료분 복원
    for corp in listed:
        cc = corp['corp_code']
        if cc in done_codes and done_codes[cc] != 'empty':
            yr_data = {int(k): v for k, v in done_codes[cc].items()}
            results.append({
                'corp_code': cc,
                'corp_name': corp['corp_name'],
                'stock_code': corp['stock_code'],
                'years': sorted(yr_data.keys()),
                'n_years': len(yr_data),
                'rcept_nos': yr_data,
            })

    t0 = time.time()
    new_count = 0
    for idx, corp in enumerate(listed):
        cc = corp['corp_code']
        if cc in done_codes:
            continue  # 이미 조회 완료

        year_rcept = query_annual_reports(cc)

        if year_rcept:
            done_codes[cc] = year_rcept
            results.append({
                'corp_code': cc,
                'corp_name': corp['corp_name'],
                'stock_code': corp['stock_code'],
                'years': sorted(year_rcept.keys()),
                'n_years': len(year_rcept),
                'rcept_nos': year_rcept,
            })
        else:
            done_codes[cc] = 'empty'
            if year_rcept is None:
                errors += 1

        new_count += 1
        time.sleep(SLEEP_PER_REQ)

        # 100건마다 중간 저장 + 진행 출력
        if new_count % 100 == 0:
            save_progress(done_codes, errors)
            save_csv(results)
            elapsed = time.time() - t0
            total_done = len(done_codes)
            remaining = len(listed) - total_done
            full5 = sum(1 for r in results if set(r['years']) >= TARGET_YEARS)
            eta_min = (elapsed / new_count * remaining) / 60 if new_count > 0 else 0
            print(f'  {total_done}/{len(listed)} (+{new_count}신규, {elapsed:.0f}s) '
                  f'-- 보유:{len(results)}, 5yr:{full5}, err:{errors}, '
                  f'잔여~{eta_min:.0f}분')

    # 최종 저장
    save_progress(done_codes, errors)
    n_full5 = save_csv(results)

    elapsed = time.time() - t0
    print(f'\n완료: {elapsed:.0f}초 (신규 {new_count}건 조회)')
    print(f'사업보고서 보유: {len(results)}개')
    print(f'5개년(2020-2024) 완전: {n_full5}개')
    print(f'오류: {errors}개')

    # 연도별 분포
    from collections import Counter
    yr_counts = Counter()
    for r in results:
        for y in r['years']:
            yr_counts[y] += 1
    print('\n연도별 사업보고서 보유 기업 수:')
    for y in sorted(yr_counts):
        print(f'  {y}: {yr_counts[y]}개')

    # 주요 기업 확인
    check = {'삼성전자', '현대자동차', '아세아제지', 'SK하이닉스', 'POSCO홀딩스'}
    print('\n주요 기업 확인:')
    for r in results:
        if r['corp_name'] in check:
            has_all = set(r['years']) >= TARGET_YEARS
            print(f'  {r["corp_name"]}: years={r["years"]}, 5yr={has_all}')

    # 진행파일 삭제 (완료 시)
    if len(done_codes) >= len(listed):
        os.remove(PROGRESS_FILE)
        print(f'\n진행파일 삭제 (전체 완료)')


if __name__ == '__main__':
    main()
