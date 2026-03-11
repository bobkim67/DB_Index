"""
dart_llm_batch.py — DART 사업보고서 퇴직연금 LLM 추출 (배치)
- corp_5yr_list_v2.csv 전체 대상, 2024년 우선
- 건별 즉시 CSV 저장 (중단 후 재개 가능)
- 발췌 로직 v2: 키워드 확대, 단위 탐색 강화
"""

import requests, ssl, zipfile, io, re, json, sys, time, os
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import anthropic
from pathlib import Path

# === 설정 ===
DART_API_KEY = '56a07e920d1f7f0e9aed6c3bc6a62491c21620c2'
CLAUDE_API_KEY = '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'

BASE = Path(__file__).parent
CORP_CSV = BASE / 'corp_5yr_list_v2.csv'
OUT_CSV = BASE / 'llm_extract_2024_v2.csv'
LOG_FILE = BASE / 'llm_batch_log.txt'

# === 발췌 키워드 v2 (확대) ===
# TABLE 매칭: 2개+ 히트 시 발췌
PENSION_KEYWORDS_TABLE = [
    # 핵심 (BS)
    '확정급여채무', '확정급여부채', '사외적립자산', '순확정급여',
    # 변동내역 (PL / movement)
    '당기근무원가', '이자비용', '이자원가', '이자수익',
    '급여지급', '퇴직금지급', '퇴직급여지급',
    '재측정', '재측정요소', '보험수리적', '보험 수리적',
    # 가정
    '할인율', '할인률',
    '임금상승률', '임금 상승률', '임금상승율', '임금 상승율',
    '임금인상률', '임금 인상률', '임금인상율', '임금 인상율',
    '승급률', '승급율',
    # 기타
    '퇴직급여', '가중평균만기', '듀레이션',
]

# 고신호 키워드: 1개만 매칭돼도 발췌 (오탐 위험 낮음)
HIGH_SIGNAL_KEYWORDS = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가']

# 가정/듀레이션 키워드: 1개만 매칭돼도 발췌 + 바스켓 우선 담기
ASSUMPTION_KEYWORDS = [
    '할인율', '할인률',
    '임금상승률', '임금상승율', '임금인상률', '임금인상율', '승급률', '승급율',
    '가중평균만기', '듀레이션',
]

# 텍스트 단락 발췌용
PENSION_KEYWORDS_TEXT = [
    '예상기여금', '예상 기여금', '기여금',
    '가중평균만기', '가중평균 만기', '가중평균듀레이션',
    '확정기여제도', '확정기여형',
    '듀레이션', '가중평균잔존만기',
]

# XML 내 퇴직연금 존재 판별용
PENSION_KEYWORDS_XML = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가']

# 금액 변수 (원 단위)
AMOUNT_VARS = [
    'DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
    'InterestIncome', 'NetInterest', 'BenefitPayment', 'ActuarialGL',
    'ActuarialGL_Financial',    # 재무적가정(할인율) 변동
    'ActuarialGL_Demographic',  # 인구통계적가정(임금상승) 변동
    'ActuarialGL_Experience',   # 경험조정
    'RetirementBenefitCost', 'ExpectedContribution', 'DCPlanCost',
]
# 범위 변수 (Min/Max/Mid 3컬럼)
RANGE_VARS = ['DiscountRate', 'SalaryGrowth', 'Duration']
# 전체 변수 (CSV 컬럼 생성용)
ALL_VAR_COLUMNS = (
    AMOUNT_VARS
    + [f'{v}_{s}' for v in RANGE_VARS for s in ('Min', 'Max', 'Mid')]
)


# === DART SSL 어댑터 ===
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


# ═══════════════════════════════════════════════════════════
# DART XML 다운로드
# ═══════════════════════════════════════════════════════════
def download_consolidated_xml(rcept_no):
    """ZIP 다운로드 → 연결주석 XML 선택"""
    url = f'https://opendart.fss.or.kr/api/document.xml?crtfc_key={DART_API_KEY}&rcept_no={rcept_no}'
    try:
        r = session.get(url, verify=False, timeout=30)
    except Exception as e:
        return None, f'HTTP오류:{e}'
    if r.status_code != 200:
        return None, f'HTTP{r.status_code}'
    if r.content[:2] != b'PK':
        # JSON 에러 응답 체크
        try:
            err = json.loads(r.content)
            return None, f'API오류:{err.get("message", err.get("status", "unknown"))}'
        except:
            return None, 'ZIP아님'
    z = zipfile.ZipFile(io.BytesIO(r.content))
    docs = {}
    for name in z.namelist():
        if name.endswith('.xml'):
            docs[name] = z.read(name).decode('utf-8', errors='replace')
    if not docs:
        return None, 'XML없음'
    # 가장 큰 XML = 본문(재무제표 원문), 나머지가 주석
    # XML이 1개면 본문+주석 통합 → 그대로 사용
    candidates = []
    if len(docs) == 1:
        name, content = next(iter(docs.items()))
        has_pension = any(kw in content for kw in PENSION_KEYWORDS_XML)
        if has_pension:
            candidates.append((name, 0, content))
    else:
        max_size = max(len(c) for c in docs.values())
        for name, content in docs.items():
            if len(content) >= max_size:
                continue  # 본문 스킵 (주석만 탐색)
            cnt_연결 = content.count('연결실체') + content.count('연결재무제표')
            has_pension = any(kw in content for kw in PENSION_KEYWORDS_XML)
            if has_pension:
                candidates.append((name, cnt_연결, content))
        # 주석에서 못 찾으면 본문에서도 시도
        if not candidates:
            for name, content in docs.items():
                has_pension = any(kw in content for kw in PENSION_KEYWORDS_XML)
                if has_pension:
                    cnt_연결 = content.count('연결실체') + content.count('연결재무제표')
                    candidates.append((name, cnt_연결, content))
    if not candidates:
        return None, '퇴직연금없음'
    return max(candidates, key=lambda x: (x[1], len(x[2])))[2], 'OK'


# ═══════════════════════════════════════════════════════════
# 발췌 로직 v2
# ═══════════════════════════════════════════════════════════
def _find_unit_label(content, tbl_start):
    """TABLE 앞 500자에서 단위 표기 탐색, 없으면 뒤 200자도 탐색"""
    for start, end in [(max(0, tbl_start - 500), tbl_start),
                       (tbl_start, min(len(content), tbl_start + 200))]:
        region = content[start:end]
        text = re.sub(r'<[^>]+>', ' ', region)
        text = re.sub(r'&nbsp;', ' ', text).strip()
        # (단위 : 백만원) 또는 (단위: 원) 등
        m = re.search(r'\(단위\s*[:\uff1a]\s*([^)]+)\)', text)
        if m:
            return f'(단위: {m.group(1).strip()})'
        m = re.search(r'단위\s*[:\uff1a]\s*(천원|백만원|원|천\s*원|백만\s*원)', text)
        if m:
            return f'(단위: {m.group(1).strip()})'
    return ''


def _clean_table_html(table_html):
    """HTML 속성 제거, 공백 정리"""
    cleaned = re.sub(
        r'\s+(style|class|width|height|colspan|rowspan|valign|align|'
        r'border|cellpadding|cellspacing|id)="[^"]*"',
        '', table_html, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\s+(style|class|width|height|colspan|rowspan|valign|align|"
        r"border|cellpadding|cellspacing|id)='[^']*'",
        '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
    return cleaned


def extract_pension_tables(content):
    """XML에서 퇴직연금 관련 TABLE + 텍스트 발췌 (v3)

    v3 개선:
    - 가정/듀레이션 키워드 1개 매칭으로 발췌 허용
    - 가정 테이블을 바스켓에 우선 담기 (할인율/임금상승률 누락 방지)
    """
    table_spans = [(m.start(), m.end())
                   for m in re.finditer(r'<TABLE[^>]*>.*?</TABLE>', content,
                                        re.DOTALL | re.IGNORECASE)]
    tables = [content[s:e] for s, e in table_spans]

    priority_tables = []   # 가정/듀레이션 테이블 (우선 담기)
    normal_tables = []     # 나머지 퇴직연금 테이블
    for idx, table_html in enumerate(tables):
        text = re.sub(r'<[^>]+>', ' ', table_html)
        text = re.sub(r'&nbsp;', ' ', text)
        text_nospace = re.sub(r'\s+', '', text)

        # 일반 키워드 매칭 (2개+)
        matched = sum(1 for kw in PENSION_KEYWORDS_TABLE
                      if kw in text or kw in text_nospace)
        # 고신호 키워드 매칭 (1개만으로 충분)
        high_matched = sum(1 for kw in HIGH_SIGNAL_KEYWORDS
                          if kw in text or kw in text_nospace)
        # 가정/듀레이션 키워드 매칭 (1개만으로 충분)
        assumption_matched = sum(1 for kw in ASSUMPTION_KEYWORDS
                                if kw in text or kw in text_nospace)

        if matched >= 2 or high_matched >= 1 or assumption_matched >= 1:
            # 10KB 이상 테이블 스킵 (재무제표 본문 노이즈 제거)
            if len(table_html) > 10000:
                continue
            unit_label = _find_unit_label(content, table_spans[idx][0])
            cleaned = _clean_table_html(table_html)
            prefix = f'[{unit_label}] ' if unit_label else ''
            score = matched + high_matched * 2
            entry = (score, prefix + cleaned)
            if assumption_matched >= 1:
                priority_tables.append(entry)
            else:
                normal_tables.append(entry)

    # 가정 테이블 먼저, 나머지는 점수 내림차순
    priority_tables.sort(key=lambda x: -x[0])
    normal_tables.sort(key=lambda x: -x[0])

    result = []
    total_len = 0
    # 1) 가정/듀레이션 테이블 우선 담기
    for _, tbl in priority_tables:
        if total_len + len(tbl) > 20000 and result:
            break
        result.append(tbl)
        total_len += len(tbl)
    # 2) 나머지 테이블 채우기
    for _, tbl in normal_tables:
        if total_len + len(tbl) > 20000 and result:
            break
        result.append(tbl)
        total_len += len(tbl)

    # === 텍스트 단락 발췌 ===
    content_no_table = re.sub(r'<TABLE[^>]*>.*?</TABLE>', '', content,
                              flags=re.DOTALL | re.IGNORECASE)
    paragraphs = re.findall(
        r'<(?:P|DIV|SPAN|TD|BODY)[^>]*>(.*?)</(?:P|DIV|SPAN|TD|BODY)>',
        content_no_table, re.DOTALL | re.IGNORECASE)
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
                sig = plain_nospace[:50]
                if sig not in seen:
                    seen.add(sig)
                    text_excerpts.append(plain)
                break

    if text_excerpts:
        text_block = '\n\n[텍스트 발췌]\n' + '\n'.join(text_excerpts)
        if total_len + len(text_block) <= 24000:
            result.append(text_block)

    return result


# ═══════════════════════════════════════════════════════════
# LLM 호출
# ═══════════════════════════════════════════════════════════
def call_llm(tables_text, year):
    """Claude Haiku로 18개 변수 추출"""
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

    system_prompt = f"""한국 상장사 사업보고서의 퇴직급여(확정급여제도) 주석에서 변수를 추출합니다.
아래 HTML 테이블들에서 {year}년(당기) 값만 추출하세요.

추출 대상 변수:

[금액 변수 — 원 단위 정수로 변환]
- DBO: 확정급여채무 현재가치 (총액). "확정급여채무의 현재가치" 항목
- PlanAsset: 사외적립자산 공정가치. 양수로 기재
- NetDBO: 순확정급여부채(자산). DBO - PlanAsset
- ServiceCost: 당기근무원가
- InterestCost: 이자비용 - DBO측. "확정급여채무의 이자비용" 또는 이자원가
- InterestIncome: 이자수익 - 사외적립자산측. 양수로 기재
- NetInterest: 순이자원가/순이자비용. InterestCost - InterestIncome
- BenefitPayment: 급여지급액/퇴직금지급액. 양수로 기재
- ActuarialGL: 보험수리적손익/재측정요소 합계. 손실=양수, 이익=음수.
- ActuarialGL_Financial: 재무적가정(할인율)의 변동에서 발생한 보험수리적손익. 손실=양수, 이익=음수.
- ActuarialGL_Demographic: 인구통계적가정(임금상승률 등)의 변동에서 발생한 보험수리적손익. 손실=양수, 이익=음수.
- ActuarialGL_Experience: 경험조정에서 발생한 보험수리적손익. 손실=양수, 이익=음수.
- RetirementBenefitCost: 퇴직급여비용 합계
- ExpectedContribution: 차기 예상기여금. 텍스트에 "예상기여금은 X백만원" 형태.
- DCPlanCost: 확정기여제도 퇴직급여 비용.

[비율/기간 변수 — 범위/단일 구분 필수]
다음 3개 변수는 범위값과 단일값을 구분하여 반환합니다:
- DiscountRate: 할인율. 보험수리적가정 테이블의 "할인율/할인률" 행에서 추출. "듀레이션/만기" 숫자와 혼동 금지. 9% 이상이면 듀레이션을 잘못 가져온 것일 수 있으니 재확인.
- SalaryGrowth: 임금상승률. 보험수리적가정 테이블의 "임금상승률/기대임금상승률/임금인상률/승급률" 행에서 추출.
- Duration: 확정급여채무 가중평균 듀레이션/만기 (년). "가중평균만기/가중평균듀레이션/듀레이션" 텍스트 또는 테이블에서 추출. 할인율과 혼동 금지.

이 3개 변수는 각각 아래 형태로 반환:
  - 범위값인 경우 (예: "4.0~5.0", "4.0% ~ 5.0%"):
    {{"DiscountRate_Min": 4.0, "DiscountRate_Max": 5.0, "DiscountRate_Mid": 4.5}}
  - 단일값인 경우 (예: "4.5%"):
    {{"DiscountRate_Min": null, "DiscountRate_Max": null, "DiscountRate_Mid": 4.5}}
  - 값이 없는 경우:
    {{"DiscountRate_Min": null, "DiscountRate_Max": null, "DiscountRate_Mid": null}}

규칙:
1. 금액은 원 단위 정수. 백만원/천원 단위면 곱하여 원으로 변환. (단위) 표기 참고.
2. 비율은 % 숫자 (예: 4.5). 소수점 형태(0.045)면 ×100. Duration은 년 단위.
3. 값이 없으면 null.
4. 사외적립자산, 급여지급액, 이자수익은 양수 (음수면 절대값).
5. 반드시 JSON 객체만 응답. 설명 텍스트 없이 JSON만.
6. 당기(={year}년) 값만. 전기 값 무시.
7. 텍스트 발췌 구간도 참고."""

    user_prompt = (
        f"아래는 사업보고서({year}년 결산)의 퇴직급여 관련 테이블과 텍스트입니다. "
        f"18개 변수를 추출하세요.\n\n{tables_text}"
    )

    response = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=1024,
        system=system_prompt,
        messages=[{'role': 'user', 'content': user_prompt}],
    )

    usage = {
        'input_tokens': response.usage.input_tokens,
        'output_tokens': response.usage.output_tokens,
    }

    text = response.content[0].text.strip()
    if text.startswith('```'):
        text = re.sub(r'^```\w*\n?', '', text)
        text = re.sub(r'\n?```$', '', text)
        text = text.strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        result = {}

    return result, usage


# ═══════════════════════════════════════════════════════════
# CSV 저장/로드 (중간 저장)
# ═══════════════════════════════════════════════════════════
CSV_COLUMNS = (
    ['corp_code', 'corp_name', 'rcept_no', 'year', 'status',
     'n_tables', 'input_tokens', 'output_tokens']
    + ALL_VAR_COLUMNS
)


def load_done_set(csv_path):
    """이미 처리된 corp_code set 로드"""
    if not csv_path.exists():
        return set()
    df = pd.read_csv(csv_path, dtype=str, usecols=['corp_code'])
    return set(df['corp_code'])


def append_row(csv_path, row_dict):
    """CSV에 1행 추가 (헤더 자동 처리)"""
    write_header = not csv_path.exists()
    row = {c: row_dict.get(c) for c in CSV_COLUMNS}
    df = pd.DataFrame([row])
    df.to_csv(csv_path, mode='a', header=write_header,
              index=False, encoding='utf-8-sig')


def log(msg):
    """로그 출력 + 파일 기록"""
    ts = time.strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


# ═══════════════════════════════════════════════════════════
# 메인 배치
# ═══════════════════════════════════════════════════════════
def main():
    # 대상 로드
    df = pd.read_csv(CORP_CSV, dtype=str)
    # 2024 rcept_no 있는 것만
    df = df[df['rcept_2024'].notna() & (df['rcept_2024'] != '')]
    total = len(df)
    log(f'전체 대상: {total}사 (rcept_2024 보유)')

    # 이미 처리된 건 스킵
    done = load_done_set(OUT_CSV)
    remaining = df[~df['corp_code'].isin(done)]
    log(f'이미 처리: {len(done)}사 → 잔여: {len(remaining)}사')

    if remaining.empty:
        log('처리할 건이 없습니다.')
        return

    total_input = 0
    total_output = 0
    n_ok = 0
    n_skip = 0
    n_err = 0

    for i, (_, row) in enumerate(remaining.iterrows()):
        corp_code = row['corp_code']
        corp_name = row['corp_name']
        rcept_no = row['rcept_2024']
        progress = f'[{len(done) + i + 1}/{total}]'

        result_row = {
            'corp_code': corp_code,
            'corp_name': corp_name,
            'rcept_no': rcept_no,
            'year': 2024,
        }

        # 1) XML 다운로드
        content, status = download_consolidated_xml(rcept_no)

        if content is None:
            result_row['status'] = status
            append_row(OUT_CSV, result_row)
            n_skip += 1
            if (i + 1) % 50 == 0 or 'API오류' in status:
                log(f'{progress} {corp_name}: {status} (누적 skip={n_skip})')
            if 'API오류' in status:
                # API 차단 가능성 → 대기
                log('API 오류 감지 → 60초 대기')
                time.sleep(60)
            continue

        # 2) 발췌
        tables = extract_pension_tables(content)
        if not tables:
            result_row['status'] = '테이블없음'
            result_row['n_tables'] = 0
            append_row(OUT_CSV, result_row)
            n_skip += 1
            continue

        tables_text = '\n\n'.join(tables)
        result_row['n_tables'] = len(tables)

        # 3) LLM 추출
        try:
            llm_result, usage = call_llm(tables_text, 2024)
            result_row['input_tokens'] = usage['input_tokens']
            result_row['output_tokens'] = usage['output_tokens']
            total_input += usage['input_tokens']
            total_output += usage['output_tokens']

            # 금액 변수
            for var in AMOUNT_VARS:
                result_row[var] = llm_result.get(var)

            # 범위 변수 (Min/Max/Mid)
            for var in RANGE_VARS:
                result_row[f'{var}_Min'] = llm_result.get(f'{var}_Min')
                result_row[f'{var}_Max'] = llm_result.get(f'{var}_Max')
                result_row[f'{var}_Mid'] = llm_result.get(f'{var}_Mid')

            # null 개수로 성공/부분 판정 (필수 7개: DBO~Duration_Mid)
            essential = ['DBO', 'PlanAsset', 'ServiceCost', 'InterestCost',
                         'BenefitPayment', 'DiscountRate_Mid', 'Duration_Mid']
            n_filled = sum(1 for v in essential
                           if result_row.get(v) is not None)
            result_row['status'] = 'OK' if n_filled >= 5 else f'부분({n_filled}/7)'
            n_ok += 1

        except anthropic.APIStatusError as e:
            result_row['status'] = f'LLM오류:{e.status_code}'
            n_err += 1
            log(f'{progress} {corp_name}: LLM 오류 {e.status_code}')
            if e.status_code == 429:
                log('Rate limit → 30초 대기')
                time.sleep(30)
            elif e.status_code in (500, 529):
                log('서버 오류 → 10초 대기')
                time.sleep(10)
        except Exception as e:
            result_row['status'] = f'오류:{str(e)[:50]}'
            n_err += 1
            log(f'{progress} {corp_name}: {e}')

        # 4) 즉시 저장
        append_row(OUT_CSV, result_row)

        # 진행 상황 출력 (50건마다 + 100건마다 비용)
        if (i + 1) % 50 == 0:
            cost = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
            log(f'{progress} {corp_name} | OK={n_ok} skip={n_skip} err={n_err} '
                f'| 토큰: {total_input:,}+{total_output:,} | ${cost:.2f}')

        # DART rate limit (1초 간격)
        time.sleep(1.0)

    # === 최종 요약 ===
    cost_in = total_input * 0.80 / 1e6
    cost_out = total_output * 4.00 / 1e6
    log(f'\n{"="*60}')
    log(f'배치 완료: OK={n_ok}, skip={n_skip}, err={n_err}')
    log(f'토큰: input={total_input:,}, output={total_output:,}')
    log(f'비용: ${cost_in + cost_out:.2f} (in=${cost_in:.2f} + out=${cost_out:.2f})')
    log(f'결과: {OUT_CSV}')
    log(f'{"="*60}')


if __name__ == '__main__':
    main()
