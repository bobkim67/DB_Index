"""DART API 5개년 사업보고서 리스트업 v2

v1 대비 변경:
- 기업별 개별 조회 → 연도별 일괄 조회 (corp_code 생략)
- 90,000건 → ~150건 API 호출 (99.8% 감소)
- 비상장사 포함
- 출력: corp_report_pivot.csv (전체), corp_5yr_list_v2.csv (5개년 완전)
"""
import requests, ssl, zipfile, io, json, sys, time, re, csv, os
sys.stdout.reconfigure(encoding='utf-8')
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from collections import Counter
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


session = requests.Session()
session.mount('https://', DARTAdapter())
API_KEY = '56a07e920d1f7f0e9aed6c3bc6a62491c21620c2'

TARGET_YEARS = [2020, 2021, 2022, 2023, 2024]
SLEEP_PER_REQ = 1.0
CORP_CODE_CACHE = 'corp_stock_map_cache.json'


def get_corp_stock_map():
    """corpCode.xml에서 corp_code → {corp_name, stock_code} 매핑 (캐시 사용)"""
    if os.path.exists(CORP_CODE_CACHE):
        with open(CORP_CODE_CACHE, encoding='utf-8') as f:
            mapping = json.load(f)
        print(f'캐시에서 로드: {len(mapping)}개 기업 (corp_stock_map)')
        return mapping

    url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}'
    r = session.get(url, verify=False, timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(z.read(z.namelist()[0]).decode('utf-8'))

    mapping = {}
    for corp in root.findall('.//list'):
        corp_code = corp.findtext('corp_code', '').strip()
        corp_name = corp.findtext('corp_name', '').strip()
        stock_code = corp.findtext('stock_code', '').strip()
        if corp_code:
            mapping[corp_code] = {
                'corp_name': corp_name,
                'stock_code': stock_code,
            }

    with open(CORP_CODE_CACHE, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False)
    n_listed = sum(1 for v in mapping.values() if v['stock_code'])
    print(f'corpCode.xml 다운로드 완료: {len(mapping)}개 기업 (상장 {n_listed}개) → 캐시 저장')
    return mapping


def _fetch_period(bgn_de, end_de, label, max_retries=3):
    """단일 기간(3개월 이내)의 사업보고서를 페이지네이션으로 수집

    Returns:
        list[dict]: API 응답의 list 항목들
    """
    base_url = (f'https://opendart.fss.or.kr/api/list.json?crtfc_key={API_KEY}'
                f'&pblntf_detail_ty=A001&bgn_de={bgn_de}&end_de={end_de}'
                f'&page_count=100&last_reprt_at=Y')

    all_items = []
    page = 1

    while True:
        url = f'{base_url}&page_no={page}'

        for attempt in range(max_retries):
            try:
                r = session.get(url, verify=False, timeout=15)
                data = json.loads(r.content.decode('utf-8'))
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f'    [재시도 {attempt+1}] {label} page={page}: {e}')
                    time.sleep(2)
                else:
                    print(f'    [실패] {label} page={page}: {e}')
                    data = {'status': '999'}

        status = data.get('status')

        if status == '013':
            return all_items  # 해당 기간 데이터 없음

        if status == '020':
            print(f'    [차단] 요청 제한. 10초 대기 후 재시도')
            time.sleep(10)
            continue

        if status != '000':
            print(f'    [오류] {label} page={page}: status={status}, message={data.get("message","")}')
            break

        items = data.get('list', [])
        all_items.extend(items)

        total_count = int(data.get('total_count', 0))
        total_page = int(data.get('total_page', 1))

        if page == 1 and total_count > 0:
            print(f'    {label}: {total_count}건, {total_page}페이지')

        if page >= total_page:
            break

        page += 1
        time.sleep(SLEEP_PER_REQ)

    return all_items


def fetch_reports_by_year(year):
    """특정 연도의 전체 사업보고서를 3개월 단위로 분할 조회

    DART API 제약: corp_code 생략 시 검색기간 3개월 이내만 가능.
    사업보고서 제출 시기: 대부분 3~4월 (12월 결산), 일부 6월까지.
    → year+1년 1~3월, 4~6월 2구간으로 조회.

    Returns:
        dict: {corp_code: {corp_name, rcept_no, rcept_dt, report_nm}}
    """
    # 사업보고서 제출 기간: year+1년 1월 ~ 6월
    # 일부 정정보고서는 더 늦을 수 있으나 last_reprt_at=Y로 최종본만 수집
    periods = [
        (f'{year}0101', f'{year}0331', f'{year} Q1'),
        (f'{year}0401', f'{year}0630', f'{year} Q2'),
        (f'{year}0701', f'{year}0930', f'{year} Q3'),
        (f'{year}1001', f'{year}1231', f'{year} Q4'),
        (f'{year+1}0101', f'{year+1}0331', f'{year+1} Q1'),
        (f'{year+1}0401', f'{year+1}0630', f'{year+1} Q2'),
    ]

    all_items = []
    for bgn, end, label in periods:
        items = _fetch_period(bgn, end, label)
        all_items.extend(items)

    # 필터: report_nm에서 연도 파싱, 해당 연도만 유지
    result = {}
    for item in all_items:
        rn = item.get('report_nm', '')
        if '사업보고서' not in rn:
            continue

        m = re.search(r'\((\d{4})\.\d{2}\)', rn)
        if not m:
            continue

        report_year = int(m.group(1))
        if report_year != year:
            continue

        corp_code = item['corp_code']
        rcept_dt = item.get('rcept_dt', '')

        # 같은 corp_code-연도 중복 시 rcept_dt 최신 선택
        if corp_code in result:
            if rcept_dt <= result[corp_code]['rcept_dt']:
                continue

        result[corp_code] = {
            'corp_name': item.get('corp_name', ''),
            'rcept_no': item.get('rcept_no', ''),
            'rcept_dt': rcept_dt,
            'report_nm': rn,
        }

    print(f'  → {year}년 사업보고서: {len(result)}개 기업 (중복 제거 후)')
    return result


def main():
    print('=' * 60)
    print('DART 5개년 사업보고서 리스트업 v2')
    print(f'대상: {TARGET_YEARS[0]}~{TARGET_YEARS[-1]}년, 상장+비상장 전체')
    print('=' * 60)

    # 1. corp_code → stock_code 매핑
    print('\n[1] corpCode.xml 로드...')
    corp_map = get_corp_stock_map()

    # 2. 연도별 사업보고서 일괄 조회
    print('\n[2] 연도별 사업보고서 조회...')
    t0 = time.time()
    year_data = {}  # {year: {corp_code: {corp_name, rcept_no, rcept_dt}}}
    api_calls = 0

    for year in TARGET_YEARS:
        result = fetch_reports_by_year(year)
        year_data[year] = result
        # API 호출 수 추정 (페이지 수)
        time.sleep(SLEEP_PER_REQ)

    elapsed = time.time() - t0
    print(f'\n조회 완료: {elapsed:.0f}초')

    # 3. corp_code별 5개년 피벗
    print('\n[3] 피벗 테이블 생성...')
    all_corps = set()
    for yr_result in year_data.values():
        all_corps.update(yr_result.keys())

    print(f'사업보고서 보유 기업 (1개년 이상): {len(all_corps)}개')

    rows = []
    for corp_code in sorted(all_corps):
        # corp_name: 최신 연도에서 가져오기
        corp_name = ''
        for year in reversed(TARGET_YEARS):
            if corp_code in year_data[year]:
                corp_name = year_data[year][corp_code]['corp_name']
                break

        # stock_code, is_listed
        info = corp_map.get(corp_code, {})
        stock_code = info.get('stock_code', '')
        is_listed = bool(stock_code)

        # 연도별 rcept_no
        rcept_nos = {}
        for year in TARGET_YEARS:
            if corp_code in year_data[year]:
                rcept_nos[year] = year_data[year][corp_code]['rcept_no']

        n_years = len(rcept_nos)

        rows.append({
            'corp_code': corp_code,
            'corp_name': corp_name,
            'stock_code': stock_code,
            'is_listed': is_listed,
            'n_years': n_years,
            'rcept_nos': rcept_nos,
        })

    # 4. corp_report_pivot.csv (전체)
    print('\n[4] CSV 저장...')
    pivot_path = 'corp_report_pivot.csv'
    with open(pivot_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['corp_code', 'corp_name', 'stock_code', 'is_listed', 'n_years',
                     'rcept_2020', 'rcept_2021', 'rcept_2022', 'rcept_2023', 'rcept_2024'])
        for r in rows:
            w.writerow([
                r['corp_code'], r['corp_name'], r['stock_code'], r['is_listed'], r['n_years'],
                r['rcept_nos'].get(2020, ''),
                r['rcept_nos'].get(2021, ''),
                r['rcept_nos'].get(2022, ''),
                r['rcept_nos'].get(2023, ''),
                r['rcept_nos'].get(2024, ''),
            ])
    print(f'  {pivot_path}: {len(rows)}개 기업')

    # 5. corp_5yr_list_v2.csv (5개년 완전 보유만)
    full5 = [r for r in rows if r['n_years'] == 5]
    v2_path = 'corp_5yr_list_v2.csv'
    with open(v2_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['corp_code', 'corp_name', 'stock_code', 'is_listed',
                     'rcept_2020', 'rcept_2021', 'rcept_2022', 'rcept_2023', 'rcept_2024'])
        for r in full5:
            w.writerow([
                r['corp_code'], r['corp_name'], r['stock_code'], r['is_listed'],
                r['rcept_nos'].get(2020, ''),
                r['rcept_nos'].get(2021, ''),
                r['rcept_nos'].get(2022, ''),
                r['rcept_nos'].get(2023, ''),
                r['rcept_nos'].get(2024, ''),
            ])
    n_listed_full5 = sum(1 for r in full5 if r['is_listed'])
    n_unlisted_full5 = len(full5) - n_listed_full5
    print(f'  {v2_path}: {len(full5)}개 기업 (상장 {n_listed_full5}, 비상장 {n_unlisted_full5})')

    # 6. 통계 출력
    print('\n' + '=' * 60)
    print('결과 요약')
    print('=' * 60)

    # 연도별 분포
    print('\n연도별 사업보고서 보유 기업 수:')
    for year in TARGET_YEARS:
        n = len(year_data[year])
        print(f'  {year}: {n:,}개')

    # n_years 분포
    yr_dist = Counter(r['n_years'] for r in rows)
    print('\n보유 연도 수 분포:')
    for n in sorted(yr_dist):
        print(f'  {n}년: {yr_dist[n]:,}개')

    # 상장/비상장
    n_listed = sum(1 for r in rows if r['is_listed'])
    n_unlisted = len(rows) - n_listed
    print(f'\n전체: 상장 {n_listed:,}개, 비상장 {n_unlisted:,}개')
    print(f'5개년 완전: 상장 {n_listed_full5:,}개, 비상장 {n_unlisted_full5:,}개')

    # 7. 기존 v1 검증
    v1_path = 'corp_5yr_list.csv'
    if os.path.exists(v1_path):
        print(f'\n[검증] 기존 {v1_path}와 비교...')
        v1_corps = set()
        with open(v1_path, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                v1_corps.add(row['corp_code'])
        v2_corps = {r['corp_code'] for r in full5}
        overlap = v1_corps & v2_corps
        v1_only = v1_corps - v2_corps
        print(f'  v1: {len(v1_corps)}개, v2: {len(v2_corps)}개')
        print(f'  공통: {len(overlap)}개, v1에만: {len(v1_only)}개')
        if v1_only:
            missing_names = []
            for r in rows:
                if r['corp_code'] in v1_only:
                    missing_names.append(f"{r['corp_name']}({r['n_years']}yr)")
            print(f'  v1에만 있는 기업: {", ".join(missing_names[:10])}')

    # 8. 주요 기업 확인
    check_names = {'삼성전자', '현대자동차', 'SK하이닉스', 'POSCO홀딩스', 'LG에너지솔루션'}
    print('\n주요 기업 확인:')
    for r in full5:
        if r['corp_name'] in check_names:
            print(f'  {r["corp_name"]}: corp_code={r["corp_code"]}, is_listed={r["is_listed"]}')

    print(f'\n총 소요시간: {time.time() - t0:.0f}초')


if __name__ == '__main__':
    main()
