import requests, ssl, zipfile, io, re, json, sys
sys.stdout.reconfigure(encoding='utf-8')
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

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

PENSION_KEYWORDS = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가']


def download_consolidated_xml(rcept_no):
    url = f'https://opendart.fss.or.kr/api/document.xml?crtfc_key={API_KEY}&rcept_no={rcept_no}'
    r = s.get(url, verify=False)
    if r.content[:2] != b'PK':
        return None
    z = zipfile.ZipFile(io.BytesIO(r.content))
    docs = {}
    for name in z.namelist():
        if name.endswith('.xml'):
            docs[name] = z.read(name).decode('utf-8', errors='replace')
    max_size = max(len(c) for c in docs.values())
    candidates = []
    for name, content in docs.items():
        if len(content) >= max_size:
            continue
        cnt = content.count('연결실체')
        has_pension = any(kw in content for kw in PENSION_KEYWORDS)
        if has_pension and cnt > 10:
            candidates.append((name, cnt, content))
    if not candidates:
        return None
    return max(candidates, key=lambda x: x[1])[2]


def html_table_to_rows(table_html):
    """HTML TABLE -> list of rows, each row = list of cell texts"""
    rows = re.findall(r'<TR[^>]*>(.*?)</TR>', table_html, re.DOTALL | re.IGNORECASE)
    result = []
    for row_html in rows:
        cells = re.findall(r'<T[DH][^>]*>(.*?)</T[DH]>', row_html, re.DOTALL | re.IGNORECASE)
        row = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell)
            text = re.sub(r'&nbsp;', ' ', text)
            text = re.sub(r'&amp;', '&', text)
            text = text.strip()
            row.append(text)
        if row:
            result.append(row)
    return result


def parse_number(s):
    """문자열 -> 숫자 파싱. 괄호=음수, 콤마제거"""
    s = s.strip()
    if not s or s in ('-', '\uff0d', '\u2014', '\u3000', ''):
        return None
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1]
    s = s.replace(',', '').replace(' ', '')
    try:
        val = float(s)
        return -val if neg else val
    except:
        return None


def extract_pension_data(content):
    """연결 주석 XML에서 퇴직급여 데이터 추출"""
    tables = re.findall(r'<TABLE[^>]*>.*?</TABLE>', content, re.DOTALL | re.IGNORECASE)

    result = {}

    # 1단계: 핵심 테이블 찾기 (확정급여채무 + 사외적립자산 동시 포함)
    summary_table = None
    movement_table = None
    summary_pos = None

    for i, table_html in enumerate(tables):
        text_flat = re.sub(r'<[^>]+>', ' ', table_html)
        text_flat = re.sub(r'&nbsp;', ' ', text_flat)

        has_dbo = '확정급여채무' in text_flat or '확정급여부채' in text_flat
        has_pa = '사외적립자산' in text_flat
        has_sc = '당기근무원가' in text_flat

        # 수치 행이 2개 이상 있는지 확인 (주석 텍스트만 있는 테이블 제외)
        rows = html_table_to_rows(table_html)
        numeric_rows = sum(1 for r in rows if len(r) >= 2 and parse_number(r[1]) is not None)

        if has_dbo and has_pa and not has_sc and numeric_rows >= 2:
            # DBO/PA 요약 테이블 (변동내역이 아닌 것, 수치 행 2개+)
            if summary_table is None:
                summary_table = table_html
                summary_pos = content.find(table_html)

        if has_sc and (has_dbo or has_pa):
            # 변동내역 테이블 (당기) -- DBO+PA 합산 or DBO만 분리
            if movement_table is None:
                movement_table = table_html

        if has_sc and not has_dbo and not has_pa:
            # 비용 요약 테이블 (당기근무원가 + 순이자원가)
            if movement_table is None:
                movement_table = table_html

    # 2단계: 요약 테이블에서 DBO/PA/NetDBO 추출
    if summary_table:
        rows = html_table_to_rows(summary_table)
        for row in rows:
            if len(row) >= 2:
                label = row[0]
                values = [parse_number(c) for c in row[1:]]

                if '현재가치' in label or ('확정급여채무' in label and '재측정' not in label):
                    if any(v is not None for v in values):
                        if 'DBO' not in result:
                            result['DBO'] = values[0]

                if '소 계' in label or '소계' in label:
                    # 기금적립+미적립 소계 (삼성전자 패턴)
                    if any(v is not None for v in values):
                        result['DBO'] = values[0]

                if '사외적립자산' in label and '수익' not in label and '이자' not in label:
                    if any(v is not None for v in values):
                        val = values[0]
                        result['PlanAsset'] = abs(val) if val and val < 0 else val

                if '순확정급여' in label:
                    if any(v is not None for v in values):
                        if 'NetDBO' not in result:
                            result['NetDBO'] = values[0]

    # 3단계: 변동내역에서 당기근무원가, 이자비용 추출
    if movement_table:
        rows = html_table_to_rows(movement_table)
        for row in rows:
            if len(row) >= 2:
                label = row[0]
                values = [parse_number(c) for c in row[1:]]

                if '당기근무원가' in label:
                    if any(v is not None for v in values):
                        result['ServiceCost'] = values[0]

                if '이자비용' in label or '이자원가' in label or '순이자' in label:
                    if any(v is not None for v in values):
                        # 이자비용(이자수익) 합산 패턴: col0=DBO측이자, col1=PA측이자
                        result['InterestCost'] = values[0]
                        if len(values) >= 2 and values[1] is not None:
                            result['InterestIncome'] = abs(values[1])

    # 4단계: 할인율/임금상승률 -- 전체 테이블에서 "보험수리적 가정" 기반 탐색
    for table_html in tables:
        text_flat = re.sub(r'<[^>]+>', ' ', table_html)
        text_flat = re.sub(r'&nbsp;', ' ', text_flat)
        has_discount = '할인율' in text_flat
        has_salary = '임금상승률' in text_flat or '기대임금' in text_flat or '승급률' in text_flat
        has_assumption = '보험수리적' in text_flat
        # 민감도 테이블 제외 (증가/감소 컬럼 있는 것)
        is_sensitivity = '증가' in text_flat and '감소' in text_flat
        if has_discount and (has_salary or has_assumption) and not is_sensitivity:
            rows = html_table_to_rows(table_html)
            for row in rows:
                if len(row) >= 2:
                    label = row[0]
                    val_str = row[1].strip()
                    if '할인율' in label:
                        result['DiscountRate'] = val_str
                    if '임금' in label or '승급' in label:
                        result['SalaryGrowth'] = val_str
            if 'DiscountRate' in result:
                break

    return result


# === 테스트 ===
test_cases = [
    ('아세아제지', '20250312000916'),
    ('삼성전자', '20250311001085'),
    ('현대자동차', '20250312001148'),
]

for name, rcept_no in test_cases:
    print(f'\n{"="*60}')
    print(f'{name}')
    print(f'{"="*60}')
    content = download_consolidated_xml(rcept_no)
    if not content:
        print('  연결 주석 없음')
        continue
    data = extract_pension_data(content)
    for k, v in data.items():
        if isinstance(v, float):
            print(f'  {k:20s}: {v:>25,.0f}')
        else:
            print(f'  {k:20s}: {v}')
