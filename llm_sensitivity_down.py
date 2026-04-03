"""민감도 감소분 경량 추출 — 기존 사업보고서 건에서 민감도 테이블만 추려 LLM 호출

기존 pension_extracts JSON에서 민감도 관련 테이블만 추출 → 경량 프롬프트로 4개 값 추출.
- SensitivityDR_1pct (증가)
- SensitivityDR_1pct_down (감소)
- SensitivitySG_1pct (증가)
- SensitivitySG_1pct_down (감소)

대상: 기존 LLM 추출에서 SensitivityDR_1pct가 있는 기업 (민감도 테이블 보유 확인됨)
"""
import pandas as pd
import json, os, sys, time, re
import anthropic

sys.stdout.reconfigure(encoding='utf-8')

API_KEY = os.environ.get('CLAUDE_API_KEY', '')
MODEL = 'claude-haiku-4-5-20251001'

client = anthropic.Anthropic(api_key=API_KEY)

# 민감도 테이블 식별 키워드
PCT_KW = ['1%', '1.0%', '1.00%', '0.0100', '0.01', '0.5%', '0.5%p', '0.005',
          'basis point', 'bp', '100bp', 'bps']
SENS_KW = ['민감도', '증가', '상승', '감소', '하락', '변동', '1%p', '0.5%', 'basis']

PROMPT = """아래는 한국 기업 퇴직연금 주석의 민감도 분석 테이블입니다.

이 테이블에서 다음 6개 값을 추출하세요:
1. DR_shock: 할인율 변동폭 (%p 단위, 예: 1 또는 0.5)
2. SensitivityDR_up: 할인율 증가시 확정급여채무 변동액. 감소하면 음수. 원 단위 정수.
3. SensitivityDR_down: 할인율 감소시 확정급여채무 변동액. 증가하면 양수. 원 단위 정수.
4. SG_shock: 임금상승률 변동폭 (%p 단위, 예: 1 또는 0.5)
5. SensitivitySG_up: 임금상승률 증가시 확정급여채무 변동액. 증가하면 양수. 원 단위 정수.
6. SensitivitySG_down: 임금상승률 감소시 확정급여채무 변동액. 감소하면 음수. 원 단위 정수.

규칙:
- 금액은 원 단위 정수. 백만원/천원 단위면 곱하여 원으로 변환. (단위) 표기 참고.
- 비율(%)로 공시된 경우(예: -5.71%): DBO × 비율/100으로 변환. DBO가 없으면 비율 그대로 반환하고 키 앞에 "pct_"를 붙이세요.
- 변동폭이 1%p(또는 1%, 0.01)이면 DR_shock=1. 0.5%p(또는 0.5%, 0.005)이면 DR_shock=0.5.
- 값이 없으면 null.
- 당기 값만 추출. 전기 값은 무시.
- JSON만 반환. 설명 텍스트 없이 JSON만.

{"DR_shock": ..., "SensitivityDR_up": ..., "SensitivityDR_down": ..., "SG_shock": ..., "SensitivitySG_up": ..., "SensitivitySG_down": ...}
"""


def find_sensitivity_tables(tables):
    """민감도 관련 테이블만 추출"""
    result = []
    for t in tables:
        if any(k in t for k in PCT_KW) and any(k in t for k in SENS_KW):
            result.append(t)
    return result


def call_llm(tables_text, dbo=None):
    """LLM 호출하여 민감도 4개 값 추출"""
    user_msg = tables_text
    if dbo and dbo > 0:
        user_msg = f"[DBO(확정급여채무) = {int(dbo):,}원]\n\n" + user_msg

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=300,
            system=PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text.strip()
        # 코드블록 및 설명 텍스트 제거
        text = re.sub(r'```\w*', '', text).strip()
        # JSON 파싱: 첫 번째 닫는 } 까지만 추출, 콤마 구분자 제거
        # 설명 텍스트가 뒤에 붙는 경우 대비
        brace_start = text.find('{')
        brace_end = text.find('}', brace_start) if brace_start >= 0 else -1
        m_str = text[brace_start:brace_end+1] if brace_start >= 0 and brace_end >= 0 else None
        m = type('M', (), {'group': lambda self: m_str})() if m_str else None
        if m:
            json_str = re.sub(r'(?<=\d),(?=\d)', '', m.group())
            data = json.loads(json_str)
            # pct_ 접두어 값 → DBO로 변환
            if dbo and dbo > 0:
                for key in list(data.keys()):
                    val = data[key]
                    if isinstance(val, str) and val.startswith('pct_'):
                        try:
                            pct = float(val.replace('pct_', '')) / 100
                            data[key] = int(dbo * pct)
                        except ValueError:
                            data[key] = None
            return data, resp.usage.input_tokens, resp.usage.output_tokens
        return None, resp.usage.input_tokens, resp.usage.output_tokens
    except json.JSONDecodeError as e:
        print(f'  JSON 파싱 오류: {e}')
        print(f'  raw text: {text[:200]}')
        return None, resp.usage.input_tokens, resp.usage.output_tokens
    except Exception as e:
        print(f'  LLM 오류: {e}')
        return None, 0, 0


def process_year(year, target_corps=None):
    """연도별 민감도 감소분 추출

    Args:
        year: 대상 연도
        target_corps: 대상 corp_code 리스트 (None이면 민감도 보유 전체)
    """
    ext_dir = f'pension_extracts/{year}'
    merged_csv = f'llm_extract_{year}_merged.csv'
    out_csv = f'llm_sensitivity_down_{year}.csv'

    if not os.path.exists(merged_csv):
        print(f'{merged_csv} 없음, 스킵')
        return

    df = pd.read_csv(merged_csv, dtype={'corp_code': str, 'rcept_no': str})

    # 대상: 기존 민감도 보유 기업
    if target_corps:
        targets = df[df['corp_code'].isin(target_corps)]
    else:
        targets = df[(df['status'] == 'OK') & (df['SensitivityDR_1pct'].notna())]

    print(f'\n{year}: 대상 {len(targets)}건')

    # 이미 완료된 건 스킵
    done_corps = set()
    if os.path.exists(out_csv):
        done = pd.read_csv(out_csv, dtype={'corp_code': str})
        done_corps = set(done['corp_code'])
        print(f'  이미 완료: {len(done_corps)}건')

    results = []
    total_in, total_out = 0, 0
    n_ok, n_fail, n_skip = 0, 0, 0

    for idx, (_, row) in enumerate(targets.iterrows()):
        corp_code = row['corp_code']
        if corp_code in done_corps:
            continue

        corp_code_padded = corp_code.zfill(8)
        json_path = os.path.join(ext_dir, f'{corp_code_padded}.json')
        if not os.path.exists(json_path):
            n_skip += 1
            continue

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 민감도 테이블 추출
        sens_tables = find_sensitivity_tables(data.get('tables', []))
        if not sens_tables:
            n_skip += 1
            continue

        tables_text = '\n\n'.join(sens_tables)
        dbo = row.get('DBO', None)

        result, in_tok, out_tok = call_llm(tables_text, dbo)
        total_in += in_tok
        total_out += out_tok

        rec = {
            'corp_code': corp_code,
            'corp_name': row['corp_name'],
            'year': year,
        }

        if result:
            # 0.5%p → 1%p 환산
            for prefix in ['DR', 'SG']:
                shock = result.get(f'{prefix}_shock')
                if shock and float(shock) == 0.5:
                    for suffix in ['up', 'down']:
                        key = f'Sensitivity{prefix}_{suffix}'
                        val = result.get(key)
                        if val and isinstance(val, (int, float)):
                            result[key] = int(val * 2)
                    result[f'{prefix}_shock'] = 1.0  # 환산 완료 표시

            # 1%p 기준 통일된 컬럼명으로 변환
            rec['SensitivityDR_1pct'] = result.get('SensitivityDR_up')
            rec['SensitivityDR_1pct_down'] = result.get('SensitivityDR_down')
            rec['SensitivitySG_1pct'] = result.get('SensitivitySG_up')
            rec['SensitivitySG_1pct_down'] = result.get('SensitivitySG_down')
            rec['DR_shock_raw'] = result.get('DR_shock')
            rec['SG_shock_raw'] = result.get('SG_shock')
            n_ok += 1
        else:
            n_fail += 1

        results.append(rec)

        if (idx + 1) % 50 == 0:
            cost = total_in / 1e6 * 1.0 + total_out / 1e6 * 5.0
            print(f'  {idx+1}/{len(targets)}: OK={n_ok} fail={n_fail} skip={n_skip} cost=${cost:.2f}')

        # 50건마다 중간 저장
        if len(results) >= 50:
            _save_results(out_csv, results, done_corps)
            done_corps.update(r['corp_code'] for r in results)
            results = []

    # 잔여 저장
    if results:
        _save_results(out_csv, results, done_corps)

    cost = total_in / 1e6 * 1.0 + total_out / 1e6 * 5.0
    print(f'  완료: OK={n_ok} fail={n_fail} skip={n_skip} cost=${cost:.2f}')


def _save_results(out_csv, results, existing_corps):
    """결과를 CSV에 추가 저장"""
    new_df = pd.DataFrame(results)
    if os.path.exists(out_csv):
        old_df = pd.read_csv(out_csv, dtype={'corp_code': str})
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_csv(out_csv, index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    years = [int(y) for y in sys.argv[1:]] if len(sys.argv) > 1 else [2024]
    for year in years:
        process_year(year)
