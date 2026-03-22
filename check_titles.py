"""DBO 값으로 XML 위치 찾고 → 가장 가까운 TITLE/heading 역추적"""
import sys, requests, ssl, zipfile, io, re, csv, time, json, os
sys.stdout.reconfigure(encoding='utf-8')
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

PROGRESS_FILE = 'title_check_progress.json'
OUT_CSV = 'title_check_result.csv'


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(done):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(done, f, ensure_ascii=False)


def get_xml_content(rcept_no):
    url = f'https://opendart.fss.or.kr/api/document.xml?crtfc_key={API_KEY}&rcept_no={rcept_no}'
    try:
        r = s.get(url, verify=False, timeout=30)
    except Exception as e:
        return None, f'HTTP오류:{e}'
    if r.status_code != 200:
        return None, f'HTTP{r.status_code}'
    if r.content[:2] != b'PK':
        return None, 'ZIP아님'
    z = zipfile.ZipFile(io.BytesIO(r.content))
    docs = {}
    for name in z.namelist():
        if name.endswith('.xml'):
            docs[name] = z.read(name).decode('utf-8', errors='replace')
    if not docs:
        return None, 'XML없음'

    if len(docs) == 1:
        return next(iter(docs.values())), 'OK'

    max_size = max(len(c) for c in docs.values())
    candidates = []
    for name, content in docs.items():
        if len(content) >= max_size:
            continue
        cnt = content.count('연결실체') + content.count('연결재무제표')
        candidates.append((name, cnt, content))
    if not candidates:
        for name, content in docs.items():
            cnt = content.count('연결실체') + content.count('연결재무제표')
            candidates.append((name, cnt, content))
    if not candidates:
        return None, 'XML없음'
    return max(candidates, key=lambda x: (x[1], len(x[2])))[2], 'OK'


def make_dbo_patterns(dbo_won):
    """원 단위 DBO 값 → XML에서 찾을 숫자 패턴 목록 생성"""
    dbo_int = int(float(dbo_won))
    if dbo_int <= 0:
        return []

    patterns = []

    # 각 단위로 나눈 값 (원/천원/백만원/억원)
    candidates = set()
    candidates.add(dbo_int)                          # 원
    if dbo_int % 1000 == 0:
        candidates.add(dbo_int // 1000)              # 천원
    if dbo_int % 1000000 == 0:
        candidates.add(dbo_int // 1000000)           # 백만원
    if dbo_int % 100000000 == 0:
        candidates.add(dbo_int // 100000000)         # 억원

    for val in candidates:
        if val == 0:
            continue
        s_plain = str(val)
        # 콤마 포맷: 1234567 → 1,234,567
        s_comma = ''
        for i, c in enumerate(reversed(s_plain)):
            if i > 0 and i % 3 == 0:
                s_comma = ',' + s_comma
            s_comma = c + s_comma

        patterns.append(s_plain)
        if ',' in s_comma:
            patterns.append(s_comma)

    # 길이 긴 것 우선 (더 구체적인 매칭)
    patterns.sort(key=lambda x: -len(x))
    # 너무 짧은 숫자는 오탐 위험 → 4자리 이상만
    patterns = [p for p in patterns if len(p.replace(',', '')) >= 4]
    return patterns


def find_dbo_offsets(content, patterns):
    """XML content에서 DBO 패턴 등장 위치 찾기"""
    # HTML 태그 내부가 아닌 텍스트 영역에서만 탐색
    offsets = []
    for pat in patterns:
        start = 0
        while True:
            idx = content.find(pat, start)
            if idx == -1:
                break
            # 태그 내부인지 확인 (< 와 > 사이인지)
            # 간단한 휴리스틱: 앞으로 가면서 < 또는 >를 먼저 만나는지
            before = content[max(0, idx - 200):idx]
            lt_pos = before.rfind('<')
            gt_pos = before.rfind('>')
            if lt_pos > gt_pos:
                # 태그 내부 → 스킵
                start = idx + 1
                continue
            offsets.append((idx, pat))
            start = idx + len(pat)
    return offsets


def find_nearest_title(content, offset):
    """offset 위치에서 위로 올라가며 가장 가까운 <TITLE> 태그 찾기"""
    region = content[:offset]
    # 마지막 TITLE 태그 찾기
    matches = list(re.finditer(r'<TITLE[^>]*>(.*?)</TITLE>', region,
                               re.DOTALL | re.IGNORECASE))
    if not matches:
        return None, -1
    last = matches[-1]
    title_text = re.sub(r'<[^>]+>', '', last.group(1)).strip()
    title_text = re.sub(r'&nbsp;', ' ', title_text).strip()
    title_text = re.sub(r'\s+', ' ', title_text).strip()
    return title_text, last.start()


def find_nearest_heading(content, offset):
    """offset 위치에서 위로 올라가며 가장 가까운 heading 패턴 찾기"""
    # offset 앞 텍스트에서 heading 찾기
    region = content[max(0, offset - 50000):offset]
    text = re.sub(r'<[^>]+>', ' ', region)
    text = re.sub(r'&nbsp;', ' ', text)

    heading_re = re.compile(
        r'(?:\d+[\.\)]\s*|\d+\s+|\(\d+\)\s*|\d+\)\s*|[가-힣][\.\)]\s*)'
        r'[^\n]{0,50}',
    )
    matches = list(heading_re.finditer(text))
    if not matches:
        return None
    last = matches[-1]
    heading = re.sub(r'\s+', ' ', last.group(0)).strip()
    return heading


def main():
    # llm_extract_2024.csv 로드
    corps = []
    with open('llm_extract_2024.csv', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rcept = row.get('접수번호', row.get('rcept_no', ''))
            name = row.get('법인명', row.get('corp_name', ''))
            code = row.get('법인코드', row.get('corp_code', ''))
            dbo = row.get('확정급여채무', row.get('DBO', ''))
            if rcept and rcept != 'rcept_no' and dbo and dbo != 'DBO':
                try:
                    dbo_val = float(dbo)
                    if dbo_val > 0:
                        corps.append((code, name, rcept, dbo_val))
                except ValueError:
                    pass

    print(f'대상: {len(corps)}사 (DBO > 0)')

    done = load_progress()
    if done:
        print(f'이전 진행: {len(done)}사 완료, {len(corps) - len(done)}사 남음')

    if not os.path.exists(OUT_CSV):
        with open(OUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            w.writerow(['corp_code', 'corp_name', 'rcept_no', 'dbo_value',
                        'found_in_xml', 'matched_pattern', 'dbo_offset',
                        'nearest_title', 'title_offset',
                        'nearest_heading'])

    t0 = time.time()
    new_count = 0

    for idx, (code, name, rcept, dbo_val) in enumerate(corps):
        if rcept in done:
            continue

        content, status = get_xml_content(rcept)

        if content is None:
            result = {
                'status': status, 'found': False,
                'pattern': '', 'dbo_offset': -1,
                'title': '', 'title_offset': -1, 'heading': ''
            }
        else:
            patterns = make_dbo_patterns(dbo_val)
            offsets = find_dbo_offsets(content, patterns)

            if offsets:
                # 첫 번째 매칭 사용
                dbo_offset, matched_pat = offsets[0]
                title, title_offset = find_nearest_title(content, dbo_offset)
                heading = find_nearest_heading(content, dbo_offset)

                result = {
                    'status': 'OK', 'found': True,
                    'pattern': matched_pat, 'dbo_offset': dbo_offset,
                    'title': title or '', 'title_offset': title_offset,
                    'heading': heading or ''
                }
            else:
                result = {
                    'status': 'OK', 'found': False,
                    'pattern': '', 'dbo_offset': -1,
                    'title': '', 'title_offset': -1, 'heading': ''
                }

        done[rcept] = result

        with open(OUT_CSV, 'a', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            w.writerow([code, name, rcept, int(dbo_val),
                       result['found'], result['pattern'], result['dbo_offset'],
                       result['title'], result['title_offset'],
                       result['heading']])

        new_count += 1
        time.sleep(1.0)

        if new_count % 50 == 0:
            save_progress(done)
            elapsed = time.time() - t0
            remaining = len(corps) - len(done)
            eta = (elapsed / new_count * remaining) / 60 if new_count > 0 else 0

            found_cnt = sum(1 for v in done.values() if v.get('found'))
            has_title = sum(1 for v in done.values() if v.get('found') and v.get('title'))
            print(f'  {len(done)}/{len(corps)} ({elapsed:.0f}s, ETA {eta:.0f}분) '
                  f'| DBO발견:{found_cnt} | TITLE있음:{has_title}')

    save_progress(done)
    elapsed = time.time() - t0

    total = len(done)
    ok_cnt = sum(1 for v in done.values() if v.get('status') == 'OK')
    found_cnt = sum(1 for v in done.values() if v.get('found'))
    has_title = sum(1 for v in done.values() if v.get('found') and v.get('title'))
    no_title = found_cnt - has_title

    print(f'\n=== 완료 ({elapsed:.0f}초, 신규 {new_count}건) ===')
    print(f'전체: {total}사')
    print(f'XML OK: {ok_cnt}사')
    print(f'DBO 값 XML에서 발견: {found_cnt}사 ({found_cnt/ok_cnt*100:.1f}%)' if ok_cnt else '')
    print(f'DBO 값 못 찾음: {ok_cnt - found_cnt}사')
    print(f'TITLE 역추적 성공: {has_title}사')
    print(f'TITLE 없음 (DBO는 찾음): {no_title}사')

    # TITLE 패턴 빈도
    from collections import Counter
    title_patterns = Counter()
    heading_patterns = Counter()
    for v in done.values():
        if v.get('title'):
            pattern = re.sub(r'^\d+[\.\)\s]+', '', v['title'])
            pattern = re.sub(r'^\([0-9]+\)\s*', '', pattern)
            pattern = re.sub(r'^[가-힣][\.\)]\s*', '', pattern)
            pattern = re.sub(r'\s*\(연결\)\s*', '', pattern)
            pattern = re.sub(r'\s*\(별도\)\s*', '', pattern)
            pattern = pattern.strip()
            title_patterns[pattern] += 1
        if v.get('heading'):
            heading_patterns[v['heading']] += 1

    print(f'\nDBO 근처 TITLE 패턴 (빈도순):')
    for pattern, cnt in title_patterns.most_common(30):
        print(f'  {cnt:4d} | {pattern}')

    print(f'\nDBO 근처 HEADING 패턴 (빈도순 상위 30):')
    for pattern, cnt in heading_patterns.most_common(30):
        print(f'  {cnt:4d} | {pattern}')


if __name__ == '__main__':
    main()
