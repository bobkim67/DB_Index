"""
dart_llm_batch.py — DART 사업보고서 퇴직연금 LLM 추출 (배치)
- corp_5yr_list_v2.csv 전체 대상, 2024년 우선
- 건별 즉시 CSV 저장 (중단 후 재개 가능)
- 발췌 로직 v4: heading 기반 섹션 추출 → 키워드 fallback
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

# === 발췌 키워드 ===
# 퇴직급여 섹션 heading 매칭용 (TITLE 태그 / top-level heading)
PENSION_HEADING_KEYWORDS = [
    '퇴직급여', '종업원급여', '확정급여부채', '확정급여제도',
    '순확정급여부채', '순확정급여자산', '퇴직급여충당부채',
    '퇴직급여충당금', '확정급여채무',
]

# TABLE 키워드 매칭 (fallback용): 2개+ 히트 시 발췌
PENSION_KEYWORDS_TABLE = [
    '확정급여채무', '확정급여부채', '사외적립자산', '순확정급여',
    '당기근무원가',
    '급여지급', '퇴직금지급', '퇴직급여지급',
    '재측정', '재측정요소', '보험수리적', '보험 수리적',
    '할인율', '할인률',
    '임금상승률', '임금 상승률', '임금상승율', '임금 상승율',
    '임금인상률', '임금 인상률', '임금인상율', '임금 인상율',
    '승급률', '승급율',
    '가중평균만기', '듀레이션',
]

# 고신호 키워드: 1개만 매칭돼도 발췌 (오탐 위험 낮음)
HIGH_SIGNAL_KEYWORDS = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가']

# 가정/듀레이션 키워드: 1개만 매칭돼도 발췌 + 바스켓 우선 담기
ASSUMPTION_KEYWORDS = [
    '할인율', '할인률',
    '임금상승률', '임금상승율', '임금인상률', '임금인상율', '승급률', '승급율',
    '가중평균만기', '평균만기', '듀레이션',
]

# 텍스트 단락 발췌용
PENSION_KEYWORDS_TEXT = [
    '예상기여금', '예상 기여금', '기여금',
    '가중평균만기', '가중평균 만기', '가중평균듀레이션', '평균만기',
    '확정기여제도', '확정기여형',
    '듀레이션', '가중평균잔존만기',
]

# 재무제표 본문 노이즈 필터 (테이블 첫 500자에 포함 시 스킵)
FS_NOISE_KEYWORDS = [
    '유동자산', '비유동자산',
    '매출액', '매출원가',
    '영업활동', '투자활동', '재무활동',
]

# XML 내 퇴직연금 존재 판별용 (v4: heading 키워드와 통일)
PENSION_KEYWORDS_XML = [
    '확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가',
    '퇴직급여', '종업원급여', '확정급여제도',
    '퇴직급여충당부채', '퇴직급여충당금', '퇴직연금운용자산',
]

# 금액 변수 (원 단위)
AMOUNT_VARS = [
    'DBO', 'PlanAsset', 'NetDBO', 'ServiceCost', 'InterestCost',
    'InterestIncome', 'NetInterest', 'BenefitPayment', 'ActuarialGL',
    'ActuarialGL_Financial',    # 재무적가정(할인율) 변동
    'ActuarialGL_Demographic',  # 인구통계적가정(임금상승) 변동
    'ActuarialGL_Experience',   # 경험조정
    'RetirementBenefitCost', 'ExpectedContribution', 'DCPlanCost',
    'SensitivityDR_1pct',   # 할인율 1%p 증가시 DBO 변동액
    'SensitivitySG_1pct',   # 임금상승률 1%p 증가시 DBO 변동액
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
            raw = z.read(name)
            # 인코딩 자동 감지: UTF-8 → EUC-KR → CP949 순 시도
            for enc in ('utf-8', 'euc-kr', 'cp949'):
                try:
                    docs[name] = raw.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                docs[name] = raw.decode('utf-8', errors='replace')
    if not docs:
        return None, 'XML없음'
    # 가장 큰 XML = 본문(재무제표 원문), 나머지가 주석
    # XML이 1개면 본문+주석 통합 → 그대로 사용
    candidates = []
    if len(docs) == 1:
        name, content = next(iter(docs.items()))
        has_pension = any(kw in content for kw in PENSION_KEYWORDS_XML)
        if has_pension:
            cnt_연결 = content.count('연결실체') + content.count('연결재무제표')
            candidates.append((name, cnt_연결, content))
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
    best = max(candidates, key=lambda x: (x[1], len(x[2])))
    fs_type = '연결' if best[1] > 0 else '별도'
    return best[2], f'OK|{fs_type}'


# ═══════════════════════════════════════════════════════════
# 발췌 로직 v4 (heading 기반 섹션 추출 → 키워드 fallback)
# ═══════════════════════════════════════════════════════════
def _strip_tags(html):
    """HTML 태그 제거, &nbsp; → 공백"""
    text = re.sub(r'<[^>]+>', '', html)
    return re.sub(r'&nbsp;', ' ', text).strip()


def _find_unit_label(content, tbl_start):
    """TABLE 앞 500자에서 단위 표기 탐색, 없으면 뒤 200자도 탐색"""
    for start, end in [(max(0, tbl_start - 500), tbl_start),
                       (tbl_start, min(len(content), tbl_start + 200))]:
        region = content[start:end]
        text = re.sub(r'<[^>]+>', ' ', region)
        text = re.sub(r'&nbsp;', ' ', text).strip()
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


# ─── 섹션 탐색 ───
def _parse_top_headings(content):
    """<P> 태그에서 top-level 주석 heading 추출.

    매칭 패턴: '숫자. 한글제목' / '숫자) 한글제목' / '숫자 한글제목'
    숫자.숫자 (sub-heading like 19.1)는 한글 시작 조건으로 자동 제외.

    Returns: [(offset, num, title_text), ...]
    """
    results = []
    for m in re.finditer(r'<P[^>]*>(.*?)</P>', content,
                         re.DOTALL | re.IGNORECASE):
        text = _strip_tags(m.group(1))
        # 숫자 + 구분자(. ) ]) + 한글로 시작하는 제목
        hm = re.match(r'(\d{1,3})[\.\)\s]\s*([가-힣].*)', text)
        if hm:
            num = int(hm.group(1))
            title = hm.group(2)[:60]
            results.append((m.start(), num, title))
    return results


def _find_pension_section(content):
    """퇴직급여 주석 섹션의 (start, end, strategy) 반환.

    Strategy 1: <TITLE> 태그에서 퇴직급여 제목 직접 매칭
    Strategy 2: top-level heading (숫자. 제목) 기반 구간 추출
    Returns: (start, end, 'TITLE'|'HEADING') or None
    """
    # ── Strategy 1: TITLE 태그 ──
    titles = list(re.finditer(
        r'<TITLE[^>]*>(.*?)</TITLE>', content,
        re.DOTALL | re.IGNORECASE))

    if titles:
        for i, tm in enumerate(titles):
            text = _strip_tags(tm.group(1))
            text_nospace = text.replace(' ', '')
            if any(kw in text or kw in text_nospace
                   for kw in PENSION_HEADING_KEYWORDS):
                start = tm.start()
                end = (titles[i + 1].start()
                       if i + 1 < len(titles) else len(content))
                return (start, end, 'TITLE')

    # ── Strategy 2: top-level heading ──
    headings = _parse_top_headings(content)
    if not headings:
        return None

    # 퇴직급여 관련 heading 후보 수집
    candidates = []
    for i, (offset, num, title) in enumerate(headings):
        title_nospace = title.replace(' ', '')
        if any(kw in title or kw in title_nospace
               for kw in PENSION_HEADING_KEYWORDS):
            candidates.append(i)

    if not candidates:
        return None

    # 여러 매칭 시 퇴직급여 데이터 TABLE 포함 섹션 우선
    # TABLE이 있어도 퇴직급여 핵심 키워드 없으면 skip (회계정책 내 유형자산 테이블 등 방지)
    PENSION_TABLE_KEYWORDS = ['확정급여채무', '확정급여부채', '사외적립자산', '당기근무원가',
                              '순확정급여', '퇴직급여부채', '퇴직급여충당']
    # 퇴직급여 키워드 TABLE 있는 후보 수집 (연결/별도 구분)
    good_candidates = []  # (idx, is_consolidated) — 키워드 TABLE 있는 후보
    fallback_idx = None   # 키워드 없는 TABLE 섹션 (최후 fallback)
    for idx in candidates:
        sec_start = headings[idx][0]
        sec_end = (headings[idx + 1][0]
                   if idx + 1 < len(headings) else len(content))
        section = content[sec_start:sec_end]
        heading_title = headings[idx][2]
        tables = re.findall(r'<TABLE[^>]*>.*?</TABLE>', section,
                            re.DOTALL | re.IGNORECASE)
        if tables:
            table_text = ' '.join(tables)
            table_text_clean = re.sub(r'<[^>]+>', '', table_text)
            if any(kw in table_text_clean for kw in PENSION_TABLE_KEYWORDS):
                # 연결 우선: heading/섹션에 '연결' 포함이면 우선, '별도' 포함이면 후순위
                is_consolidated = ('연결' in heading_title or
                                   '연결실체' in section[:500])
                is_separate = ('별도' in heading_title)
                good_candidates.append((idx, is_consolidated, is_separate))
            elif fallback_idx is None:
                fallback_idx = idx

    best_idx = None
    if good_candidates:
        # 연결 후보 우선, 별도 후보 후순위, 그 외 중립
        good_candidates.sort(key=lambda x: (-x[1], x[2]))
        best_idx = good_candidates[0][0]

    if best_idx is None:
        best_idx = fallback_idx if fallback_idx is not None else candidates[-1]

    start = headings[best_idx][0]
    end = (headings[best_idx + 1][0]
           if best_idx + 1 < len(headings) else len(content))
    return (start, end, 'HEADING')


# ─── 섹션 기반 발췌 ───
def _extract_from_section(content, sec_start, sec_end):
    """퇴직급여 섹션 구간에서 모든 TABLE + 관련 텍스트 발췌.

    섹션이 이미 퇴직급여 범위이므로 키워드 필터 없이 TABLE 전체 수집.
    텍스트는 예상기여금/듀레이션 등 키워드 매칭으로 선별.
    """
    section = content[sec_start:sec_end]

    result = []
    total_len = 0

    # 1) 섹션 내 모든 TABLE
    for m in re.finditer(r'<TABLE[^>]*>.*?</TABLE>', section,
                         re.DOTALL | re.IGNORECASE):
        table_html = m.group(0)
        if len(table_html) > 35000:
            continue

        tbl_offset = sec_start + m.start()
        unit_label = _find_unit_label(content, tbl_offset)
        cleaned = _clean_table_html(table_html)
        prefix = f'[{unit_label}] ' if unit_label else ''
        entry = prefix + cleaned

        if total_len + len(entry) > 150000 and result:
            break
        result.append(entry)
        total_len += len(entry)

    # 2) 텍스트 발췌 (TABLE 바깥 P 태그에서 키워드 매칭)
    section_no_table = re.sub(r'<TABLE[^>]*>.*?</TABLE>', '', section,
                              flags=re.DOTALL | re.IGNORECASE)
    text_excerpts = []
    seen = set()
    text_keywords = PENSION_KEYWORDS_TEXT + ASSUMPTION_KEYWORDS

    for pm in re.finditer(r'<P[^>]*>(.*?)</P>', section_no_table,
                          re.DOTALL | re.IGNORECASE):
        plain = _strip_tags(pm.group(1))
        if len(plain) < 10:
            continue
        plain_nospace = re.sub(r'\s+', '', plain)
        for kw in text_keywords:
            kw_nospace = kw.replace(' ', '')
            if kw in plain or kw_nospace in plain_nospace:
                sig = plain_nospace[:50]
                if sig not in seen:
                    seen.add(sig)
                    text_excerpts.append(plain)
                break

    if text_excerpts:
        text_block = '\n\n[텍스트 발췌]\n' + '\n'.join(text_excerpts)
        if total_len + len(text_block) <= 150000:
            result.append(text_block)

    return result


# ─── 키워드 기반 발췌 (fallback, 기존 v3) ───
def _extract_by_keywords(content):
    """TITLE/heading 모두 실패 시 전체 XML에서 키워드 매칭으로 발췌."""
    table_spans = [(m.start(), m.end())
                   for m in re.finditer(r'<TABLE[^>]*>.*?</TABLE>', content,
                                        re.DOTALL | re.IGNORECASE)]
    tables = [content[s:e] for s, e in table_spans]

    priority_tables = []
    normal_tables = []
    for idx, table_html in enumerate(tables):
        text = re.sub(r'<[^>]+>', ' ', table_html)
        text = re.sub(r'&nbsp;', ' ', text)
        text_nospace = re.sub(r'\s+', '', text)

        matched = sum(1 for kw in PENSION_KEYWORDS_TABLE
                      if kw in text or kw in text_nospace)
        high_matched = sum(1 for kw in HIGH_SIGNAL_KEYWORDS
                          if kw in text or kw in text_nospace)
        assumption_matched = sum(1 for kw in ASSUMPTION_KEYWORDS
                                if kw in text or kw in text_nospace)

        if matched >= 2 or high_matched >= 1 or assumption_matched >= 1:
            if len(table_html) > 35000:
                continue
            text_head = re.sub(r'\s+', '', text[:500])
            if any(fskw in text_head for fskw in FS_NOISE_KEYWORDS):
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

    priority_tables.sort(key=lambda x: -x[0])
    normal_tables.sort(key=lambda x: -x[0])

    result = []
    total_len = 0
    for _, tbl in priority_tables:
        if total_len + len(tbl) > 150000 and result:
            break
        result.append(tbl)
        total_len += len(tbl)
    for _, tbl in normal_tables:
        if total_len + len(tbl) > 150000 and result:
            break
        result.append(tbl)
        total_len += len(tbl)

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
        if total_len + len(text_block) <= 150000:
            result.append(text_block)

    return result


# ─── 메인 발췌 함수 ───
def extract_pension_tables(content):
    """XML에서 퇴직연금 관련 TABLE + 텍스트 발췌 (v4)

    v4: heading 기반 섹션 추출 우선 → 키워드 fallback
    Returns: (tables_list, strategy) where strategy = 'TITLE'|'HEADING'|'KEYWORD'
    """
    # 1) 섹션 기반 (TITLE 태그 / top-level heading)
    section_info = _find_pension_section(content)

    if section_info:
        start, end, strategy = section_info
        tables = _extract_from_section(content, start, end)
        if tables:
            return tables, strategy

    # 2) fallback: 키워드 기반 (기존 v3)
    tables = _extract_by_keywords(content)
    return tables, 'KEYWORD'


# ═══════════════════════════════════════════════════════════
# LLM 호출
# ═══════════════════════════════════════════════════════════
def call_llm(tables_text, year):
    """Claude Haiku로 20개 변수 추출"""
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
- SensitivityDR_1pct: 민감도 분석에서 할인율 1%p(또는 1%) 증가시 확정급여채무 변동액. 감소하면 음수. 원 단위 정수.
- SensitivitySG_1pct: 민감도 분석에서 임금상승률 1%p(또는 1%) 증가시 확정급여채무 변동액. 증가하면 양수. 원 단위 정수.

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
7. 연결재무제표와 별도재무제표가 모두 있으면, 연결재무제표 기준 값을 우선 사용.
8. 텍스트 발췌 구간도 참고."""

    user_prompt = (
        f"아래는 사업보고서({year}년 결산)의 퇴직급여 관련 테이블과 텍스트입니다. "
        f"20개 변수를 추출하세요.\n\n{tables_text}"
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
    ['corp_code', 'corp_name', 'rcept_no', 'year', 'fs_type', 'status',
     'extract_method', 'n_tables', 'input_tokens', 'output_tokens']
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
EXTRACT_DIR = BASE / 'pension_extracts'


def process_year(year):
    """단일 연도 LLM 배치 처리 (pension_extracts JSON 기반)"""
    year_dir = EXTRACT_DIR / str(year)
    out_csv = BASE / f'llm_extract_{year}.csv'

    if not year_dir.exists():
        log(f'{year}년 발췌 디렉토리 없음: {year_dir}')
        return

    # 발췌 JSON 목록 (테이블 보유 기업만)
    targets = []
    for fp in sorted(year_dir.glob('*.json')):
        with open(fp, encoding='utf-8') as f:
            d = json.load(f)
        if d.get('n_tables', 0) > 0:
            targets.append(d)
    total = len(targets)
    log(f'\n=== {year}년 시작: {total}사 (테이블 보유) ===')

    # 이미 처리된 건 스킵
    done = load_done_set(out_csv)
    remaining = [d for d in targets if d['corp_code'] not in done]
    log(f'이미 처리: {len(done)}사 -> 잔여: {len(remaining)}사')

    if not remaining:
        log('처리할 건이 없습니다.')
        return

    total_input = 0
    total_output = 0
    n_ok = 0
    n_skip = 0
    n_err = 0

    for i, d in enumerate(remaining):
        corp_code = d['corp_code']
        corp_name = d['corp_name']
        rcept_no = d.get('rcept_no', '')
        extract_method = d.get('extract_method', '')
        fs_type = d.get('fs_type', '')
        progress = f'[{len(done) + i + 1}/{total}]'

        result_row = {
            'corp_code': corp_code,
            'corp_name': corp_name,
            'rcept_no': rcept_no,
            'year': year,
            'fs_type': fs_type,
            'extract_method': extract_method,
            'n_tables': d['n_tables'],
        }

        tables_text = '\n\n'.join(d['tables'])

        # LLM 추출
        try:
            llm_result, usage = call_llm(tables_text, year)
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
                log('Rate limit -> 30초 대기')
                time.sleep(30)
            elif e.status_code in (500, 529):
                log('서버 오류 -> 10초 대기')
                time.sleep(10)
        except Exception as e:
            result_row['status'] = f'오류:{str(e)[:50]}'
            n_err += 1
            log(f'{progress} {corp_name}: {e}')

        # 즉시 저장
        append_row(out_csv, result_row)

        # 진행 상황 출력 (50건마다)
        if (i + 1) % 50 == 0:
            cost = total_input * 0.80 / 1e6 + total_output * 4.00 / 1e6
            log(f'{progress} {corp_name} | OK={n_ok} skip={n_skip} err={n_err} '
                f'| 토큰: {total_input:,}+{total_output:,} | ${cost:.2f}')

    # === 연도별 요약 ===
    cost_in = total_input * 0.80 / 1e6
    cost_out = total_output * 4.00 / 1e6
    log(f'=== {year}년 완료: OK={n_ok}, skip={n_skip}, err={n_err} ===')
    log(f'토큰: input={total_input:,}, output={total_output:,}')
    log(f'비용: ${cost_in + cost_out:.2f} (in=${cost_in:.2f} + out=${cost_out:.2f})')
    log(f'결과: {out_csv}')


def main():
    # CLI 인자로 연도 지정: python dart_llm_batch.py 2024 2023
    # 기본값: 2024만
    years = [2024]
    if len(sys.argv) > 1:
        years = [int(y) for y in sys.argv[1:]]
    log(f'대상 연도: {years}')

    for year in years:
        process_year(year)


if __name__ == '__main__':
    main()
