"""
DART 사업보고서 퇴직연금 변수 LLM 추출 테스트 (5사 × 1년)
- corp_5yr_list_v2.csv에서 상장사 첫 5사 선택
- document.xml ZIP 다운로드 → 연결주석 XML → 퇴직연금 TABLE 발췌
- Claude Haiku로 13개 변수 추출 → JSON 파싱
"""

import requests, ssl, zipfile, io, re, json, sys, time
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import anthropic

# === 설정 ===
DART_API_KEY = '56a07e920d1f7f0e9aed6c3bc6a62491c21620c2'
CLAUDE_API_KEY = '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'

PENSION_KEYWORDS_TABLE = [
    '확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가',
    '할인율', '할인률', '보험수리적', '보험 수리적',
    '임금상승률', '임금 상승률', '임금상승율', '임금 상승율',
    '승급률', '승급율', '퇴직률', '퇴직율',
    '가중평균만기', '듀레이션',
]

# 텍스트 단락 발췌용 키워드 (TABLE 밖에 텍스트로 존재하는 변수)
PENSION_KEYWORDS_TEXT = [
    '예상기여금', '예상 기여금', '기여금',
    '가중평균만기', '가중평균 만기', '가중평균듀레이션',
    '확정기여제도', '확정기여형',
]

PENSION_KEYWORDS_XML = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가']

VARIABLES = [
    'DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
    'InterestIncome', 'NetInterest', 'BenefitPayment', 'ActuarialGL',
    'RetirementBenefitCost', 'DiscountRate', 'SalaryGrowth', 'Duration',
    'ExpectedContribution', 'DCPlanCost',
]

# === DART SSL 어댑터 (dart_pension_test.py 재사용) ===
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


def download_consolidated_xml(rcept_no):
    """ZIP 다운로드 → 연결주석 XML 선택 (dart_pension_test.py 재사용)"""
    url = f'https://opendart.fss.or.kr/api/document.xml?crtfc_key={DART_API_KEY}&rcept_no={rcept_no}'
    r = session.get(url, verify=False)
    if r.content[:2] != b'PK':
        return None
    z = zipfile.ZipFile(io.BytesIO(r.content))
    docs = {}
    for name in z.namelist():
        if name.endswith('.xml'):
            docs[name] = z.read(name).decode('utf-8', errors='replace')
    if not docs:
        return None
    max_size = max(len(c) for c in docs.values())
    candidates = []
    for name, content in docs.items():
        if len(content) >= max_size and len(docs) > 1:
            continue
        cnt_연결 = content.count('연결실체') + content.count('연결재무제표')
        has_pension = any(kw in content for kw in PENSION_KEYWORDS_XML)
        if has_pension:
            candidates.append((name, cnt_연결, content))
    if not candidates:
        return None
    # 연결실체 언급 많은 XML 우선, 동점이면 큰 파일 우선 (연결주석이 별도주석보다 큼)
    return max(candidates, key=lambda x: (x[1], len(x[2])))[2]


def extract_pension_tables(content):
    """XML에서 퇴직연금 키워드 포함 TABLE + 텍스트 단락 발췌"""
    # === 1. TABLE 발췌 (키워드 2개+) ===
    # TABLE 시작 위치 기록 (앞 텍스트에서 단위 표기 추출용)
    table_spans = [(m.start(), m.end()) for m in re.finditer(r'<TABLE[^>]*>.*?</TABLE>', content, re.DOTALL | re.IGNORECASE)]
    tables = [content[s:e] for s, e in table_spans]
    selected = []
    for idx, table_html in enumerate(tables):
        text = re.sub(r'<[^>]+>', ' ', table_html)
        text = re.sub(r'&nbsp;', ' ', text)
        text_nospace = re.sub(r'\s+', '', text)
        matched = sum(1 for kw in PENSION_KEYWORDS_TABLE if kw in text or kw in text_nospace)
        if matched >= 2:
            # TABLE 바로 앞 300자에서 단위 표기 추출
            tbl_start = table_spans[idx][0]
            prefix_raw = content[max(0, tbl_start - 300):tbl_start]
            prefix_text = re.sub(r'<[^>]+>', ' ', prefix_raw)
            prefix_text = re.sub(r'&nbsp;', ' ', prefix_text).strip()
            unit_match = re.search(r'\(단위\s*:\s*\S+\)', prefix_text)
            if not unit_match:
                unit_match = re.search(r'단위\s*:\s*(천원|백만원|원)', prefix_text)
            unit_label = f'[{unit_match.group()}] ' if unit_match else ''

            cleaned = re.sub(r'\s+(style|class|width|height|colspan|rowspan|valign|align|border|cellpadding|cellspacing|id)="[^"]*"', '', table_html, flags=re.IGNORECASE)
            cleaned = re.sub(r'\s+(style|class|width|height|colspan|rowspan|valign|align|border|cellpadding|cellspacing|id)=\'[^\']*\'', '', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r'[ \t]+', ' ', cleaned)
            cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
            cleaned = unit_label + cleaned
            selected.append((matched, cleaned))

    selected.sort(key=lambda x: -x[0])

    result = []
    total_len = 0
    for _, tbl in selected:
        if total_len + len(tbl) > 18000 and result:
            break
        result.append(tbl)
        total_len += len(tbl)

    # === 2. 텍스트 단락 발췌 (TABLE 밖 키워드 포함 문장) ===
    # TABLE 제거한 텍스트에서 P/SPAN/DIV 등 블록 단위로 추출
    content_no_table = re.sub(r'<TABLE[^>]*>.*?</TABLE>', '', content, flags=re.DOTALL | re.IGNORECASE)
    # 블록 태그 기준 분할 → 텍스트 단락 추출
    paragraphs = re.findall(r'<(?:P|DIV|SPAN|TD|BODY)[^>]*>(.*?)</(?:P|DIV|SPAN|TD|BODY)>', content_no_table, re.DOTALL | re.IGNORECASE)
    # 매칭 안 되면 <br> 등으로 분할된 텍스트도 시도
    if not paragraphs:
        paragraphs = re.split(r'<br\s*/?>|<BR\s*/?>', content_no_table)

    text_excerpts = []
    seen = set()
    for p in paragraphs:
        plain = re.sub(r'<[^>]+>', '', p)
        plain = re.sub(r'&nbsp;', ' ', plain).strip()
        if not plain or len(plain) < 10:
            continue
        plain_nospace = re.sub(r'\s+', '', plain)
        for kw in PENSION_KEYWORDS_TEXT:
            kw_nospace = kw.replace(' ', '')
            if kw in plain or kw_nospace in plain_nospace:
                # 중복 방지 (같은 문장 반복 추출 방지)
                sig = plain_nospace[:50]
                if sig not in seen:
                    seen.add(sig)
                    text_excerpts.append(plain)
                break

    if text_excerpts:
        text_block = '\n\n[텍스트 발췌]\n' + '\n'.join(text_excerpts)
        if total_len + len(text_block) <= 22000:
            result.append(text_block)

    return result


def call_llm(tables_text, year):
    """Claude Haiku로 변수 추출"""
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

    system_prompt = f"""한국 상장사 사업보고서의 퇴직급여(확정급여제도) 주석에서 변수를 추출합니다.
아래 HTML 테이블들에서 {year}년(당기) 값만 추출하세요.

추출 대상 15개 변수:
- DBO: 확정급여채무 현재가치 (총액, 원). "확정급여채무의 현재가치" 항목
- PlanAsset: 사외적립자산 공정가치 (원). 양수로 기재
- NetDBO: 순확정급여부채(자산) (원). DBO - PlanAsset
- ServiceCost: 당기근무원가 (원)
- InterestCost: 이자비용 - DBO측 (원). "확정급여채무의 이자비용" 또는 이자원가
- InterestIncome: 이자수익 - 사외적립자산측 (원). 양수로 기재
- NetInterest: 순이자원가/순이자비용 (원). InterestCost - InterestIncome
- BenefitPayment: 급여지급액/퇴직금지급액 (원). 양수로 기재
- ActuarialGL: 보험수리적손익/재측정요소의 합계 (원). "보험수리적손실(이익)" 또는 "재측정요소" 항목. 3개 하위항목의 합산: (1)인구통계적가정 변동 (2)재무적가정 변동 (3)경험조정(기타). 합계행이 있으면 합계 사용, 없으면 3개 합산. 손실=양수, 이익=음수(괄호).
- RetirementBenefitCost: 퇴직급여비용 합계 (원)
- DiscountRate: 할인율 (%). 예: 4.5. 범위면 중간값.
- SalaryGrowth: 임금상승률/승급률 (%). 예: 3.0. 범위면 중간값.
- Duration: 확정급여채무 가중평균 듀레이션/가중평균만기 (년). 예: 8.2. 텍스트에 "가중평균만기는 X년" 형태로 기재될 수 있음.
- ExpectedContribution: 차기 예상기여금 (원). "예상기여금은 X백만원" 형태로 텍스트에 기재.
- DCPlanCost: 확정기여제도 퇴직급여 비용 (원). "확정기여제도로 인식한 퇴직급여는 X백만원" 형태.

규칙:
1. 금액은 원 단위 정수. 백만원/천원 단위면 곱하여 원으로 변환. (단위)가 표기된 경우 참고.
2. 비율은 % 소수 (예: 4.5). 소수점 형태(0.045)면 100 곱하여 % 변환.
3. 값이 없거나 찾을 수 없으면 null.
4. 사외적립자산, 급여지급액, 이자수익은 양수로 기재 (음수면 절대값).
5. 반드시 JSON 객체만 응답. 설명 텍스트 없이 JSON만.
6. 당기(={year}년) 값만. 전기 값 무시.
7. 테이블뿐 아니라 [텍스트 발췌] 구간의 텍스트도 참고하여 값을 추출."""

    user_prompt = f"아래는 사업보고서({year}년 결산)의 퇴직급여 관련 테이블과 텍스트입니다. 15개 변수를 추출하세요.\n\n{tables_text}"

    response = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=1024,
        system=system_prompt,
        messages=[{'role': 'user', 'content': user_prompt}],
    )

    # 토큰 사용량
    usage = {
        'input_tokens': response.usage.input_tokens,
        'output_tokens': response.usage.output_tokens,
    }

    # JSON 파싱
    text = response.content[0].text.strip()
    # ```json ... ``` 래핑 제거
    if text.startswith('```'):
        text = re.sub(r'^```\w*\n?', '', text)
        text = re.sub(r'\n?```$', '', text)
        text = text.strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        print(f'  [LLM JSON 파싱 실패] {text[:200]}')
        result = {}

    return result, usage


# === regex 기반 추출 (dart_pension_test.py 재사용) ===
def html_table_to_rows(table_html):
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


def extract_pension_regex(content):
    """regex 기반 추출 (dart_pension_test.py 로직)"""
    tables = re.findall(r'<TABLE[^>]*>.*?</TABLE>', content, re.DOTALL | re.IGNORECASE)
    result = {}

    summary_table = None
    movement_table = None

    for i, table_html in enumerate(tables):
        text_flat = re.sub(r'<[^>]+>', ' ', table_html)
        text_flat = re.sub(r'&nbsp;', ' ', text_flat)
        has_dbo = '확정급여채무' in text_flat or '확정급여부채' in text_flat
        has_pa = '사외적립자산' in text_flat
        has_sc = '당기근무원가' in text_flat
        rows = html_table_to_rows(table_html)
        numeric_rows = sum(1 for r in rows if len(r) >= 2 and parse_number(r[1]) is not None)

        if has_dbo and has_pa and not has_sc and numeric_rows >= 2:
            if summary_table is None:
                summary_table = table_html
        if has_sc and (has_dbo or has_pa):
            if movement_table is None:
                movement_table = table_html
        if has_sc and not has_dbo and not has_pa:
            if movement_table is None:
                movement_table = table_html

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
                        result['InterestCost'] = values[0]
                        if len(values) >= 2 and values[1] is not None:
                            result['InterestIncome'] = abs(values[1])

    for table_html in tables:
        text_flat = re.sub(r'<[^>]+>', ' ', table_html)
        text_flat = re.sub(r'&nbsp;', ' ', text_flat)
        has_discount = '할인율' in text_flat
        has_salary = '임금상승률' in text_flat or '기대임금' in text_flat or '승급률' in text_flat
        has_assumption = '보험수리적' in text_flat
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


def format_value(val):
    """출력 포맷팅"""
    if val is None:
        return '-'
    if isinstance(val, (int, float)):
        if abs(val) >= 1e6:
            return f'{val:>18,.0f}'
        return f'{val}'
    return str(val)


def main():
    # CSV 로드 → 상장사 첫 5사
    df = pd.read_csv('corp_5yr_list_v2.csv', dtype=str)
    listed = df[df['is_listed'] == 'True'].head(5)
    print(f'테스트 대상: {len(listed)}사\n')

    total_input = 0
    total_output = 0
    results_llm = []
    results_regex = []

    for _, row in listed.iterrows():
        corp_name = row['corp_name']
        rcept_no = row['rcept_2024']
        print(f'{"="*70}')
        print(f'{corp_name} (rcept_no={rcept_no})')
        print(f'{"="*70}')

        # 1) XML 다운로드
        content = download_consolidated_xml(rcept_no)
        if not content:
            print('  연결 주석 XML 없음 → 스킵\n')
            results_llm.append({'corp': corp_name})
            results_regex.append({'corp': corp_name})
            continue
        print(f'  XML 크기: {len(content):,}자')

        # 2) 퇴직연금 TABLE 발췌
        tables = extract_pension_tables(content)
        if not tables:
            print('  퇴직연금 테이블 없음 → 스킵\n')
            results_llm.append({'corp': corp_name})
            results_regex.append({'corp': corp_name})
            continue
        tables_text = '\n\n'.join(tables)
        print(f'  발췌 테이블: {len(tables)}개, {len(tables_text):,}자')

        # 3) LLM 추출
        llm_result, usage = call_llm(tables_text, 2024)
        total_input += usage['input_tokens']
        total_output += usage['output_tokens']
        print(f'  LLM 토큰: input={usage["input_tokens"]:,}, output={usage["output_tokens"]:,}')

        # 4) Regex 추출
        regex_result = extract_pension_regex(content)

        # 5) 결과 저장
        llm_row = {'corp': corp_name}
        llm_row.update(llm_result)
        results_llm.append(llm_row)

        regex_row = {'corp': corp_name}
        regex_row.update(regex_result)
        results_regex.append(regex_row)

        # 개별 출력
        print(f'\n  {"변수":<25s} {"LLM":>20s}  {"Regex":>20s}')
        print(f'  {"-"*67}')
        for var in VARIABLES:
            lv = llm_result.get(var)
            rv = regex_result.get(var)
            print(f'  {var:<25s} {format_value(lv):>20s}  {format_value(rv):>20s}')
        print()

        time.sleep(1)  # DART API rate limit

    # === 요약 ===
    print(f'\n{"="*70}')
    print(f'총 토큰 사용: input={total_input:,}, output={total_output:,}')
    cost_input = total_input * 0.80 / 1_000_000
    cost_output = total_output * 4.00 / 1_000_000
    print(f'예상 비용: ${cost_input + cost_output:.4f} (input ${cost_input:.4f} + output ${cost_output:.4f})')
    print(f'{"="*70}')

    # === 법인별 × 변수 피봇 테이블 ===
    corp_names = [r['corp'] for r in results_llm]
    col_w = max(len(n) for n in corp_names) + 2  # 법인명 최대폭
    col_w = max(col_w, 18)

    def fmt(val):
        """테이블 셀 포맷"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return '-'
        if isinstance(val, float):
            if val == 0:
                return '0'
            if abs(val) >= 1e9:
                return f'{val/1e8:,.0f}억'
            if abs(val) >= 1e4:
                return f'{val:,.0f}'
            return f'{val:g}'
        if isinstance(val, int):
            if abs(val) >= 1e9:
                return f'{val/1e8:,.0f}억'
            if abs(val) >= 1e4:
                return f'{val:,.0f}'
            return str(val)
        return str(val)

    # LLM 결과 피봇
    print(f'\n{"="*70}')
    print('[LLM 추출 결과 — 법인 × 변수]')
    print(f'{"="*70}')
    header = f'{"변수":<22s}' + ''.join(f'{n:>{col_w}s}' for n in corp_names)
    print(header)
    print('-' * len(header))
    for var in VARIABLES:
        row_str = f'{var:<22s}'
        for r in results_llm:
            val = r.get(var)
            row_str += f'{fmt(val):>{col_w}s}'
        print(row_str)

    # Regex 결과 피봇
    print(f'\n{"="*70}')
    print('[Regex 추출 결과 — 법인 × 변수]')
    print(f'{"="*70}')
    header = f'{"변수":<22s}' + ''.join(f'{n:>{col_w}s}' for n in corp_names)
    print(header)
    print('-' * len(header))
    for var in VARIABLES:
        row_str = f'{var:<22s}'
        for r in results_regex:
            val = r.get(var)
            row_str += f'{fmt(val):>{col_w}s}'
        print(row_str)

    # LLM vs Regex 비교 (일치/불일치)
    print(f'\n{"="*70}')
    print('[LLM vs Regex 비교]')
    print(f'{"="*70}')
    header = f'{"변수":<22s}' + ''.join(f'{n:>{col_w}s}' for n in corp_names)
    print(header)
    print('-' * len(header))
    compare_data = []
    for var in VARIABLES:
        row_str = f'{var:<22s}'
        cmp_row = {'변수': var}
        for llm_r, reg_r in zip(results_llm, results_regex):
            cn = llm_r['corp']
            lv = llm_r.get(var)
            rv = reg_r.get(var)
            l_none = lv is None or (isinstance(lv, float) and pd.isna(lv))
            r_none = rv is None or (isinstance(rv, float) and pd.isna(rv))
            if l_none and r_none:
                mark = '-'
            elif l_none:
                mark = 'Regex만'
            elif r_none:
                mark = 'LLM만'
            else:
                try:
                    lf = float(str(lv).replace(',', '').replace('%', '').split('~')[0].strip())
                    rf = float(str(rv).replace(',', '').replace('%', '').split('~')[0].strip())
                    if abs(lf - rf) < max(abs(lf), 1) * 0.01:
                        mark = 'O 일치'
                    else:
                        mark = 'X 불일치'
                except:
                    mark = '?'
            row_str += f'{mark:>{col_w}s}'
            cmp_row[cn] = mark
        print(row_str)
        compare_data.append(cmp_row)

    # === 엑셀 저장 ===
    excel_path = 'dart_llm_test_result.xlsx'
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        # Sheet 1: LLM 추출
        llm_rows = []
        for var in VARIABLES:
            r = {'변수': var}
            for d in results_llm:
                r[d['corp']] = d.get(var)
            llm_rows.append(r)
        df_llm_pivot = pd.DataFrame(llm_rows)
        df_llm_pivot.to_excel(writer, sheet_name='LLM 추출', index=False)

        # Sheet 2: Regex 추출
        regex_rows = []
        for var in VARIABLES:
            r = {'변수': var}
            for d in results_regex:
                r[d['corp']] = d.get(var)
            regex_rows.append(r)
        df_regex_pivot = pd.DataFrame(regex_rows)
        df_regex_pivot.to_excel(writer, sheet_name='Regex 추출', index=False)

        # Sheet 3: 비교
        df_cmp = pd.DataFrame(compare_data)
        df_cmp.to_excel(writer, sheet_name='LLM vs Regex', index=False)

        # 각 시트 서식
        for sheet_name in ['LLM 추출', 'Regex 추출', 'LLM vs Regex']:
            ws = writer.sheets[sheet_name]
            ws.set_column('A:A', 24)
            ws.set_column('B:F', 22)

    print(f'\n엑셀 저장 완료: {excel_path}')


if __name__ == '__main__':
    main()
