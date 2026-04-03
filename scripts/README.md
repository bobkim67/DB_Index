# DB_Index 파이프라인

DART 사업보고서에서 한국 상장사 DB형 퇴직연금 부채 인덱스(LIDX)를 산출하는 파이프라인.

## 실행 순서

```
[1] 데이터 수집
    dart_extract_save.py → pension_extracts/{year}/*.json

[2] LLM 배치 추출
    dart_llm_batch.py → llm_extract_{year}.csv

[3] 검증/보정
    validate_2014_2017.py → llm_extract_{year}_merged.csv
        ├─ identify_reextract_targets.py (대상 식별)
        ├─ run_reextract.py (no_match/비대칭 재추출)
        ├─ llm_sensitivity_down.py (민감도 경량 재추출)
        ├─ fix_column_swap.py (컬럼 오분류 재추출)
        └─ fix_dr_outliers.py (DR 이상치/삭제 기업 보정)

[별도] Spot Rate Curve
    build_spot_rate.py → ytm/spot_rate_ear.csv

[최종] LIDX 엑셀 생성
    build_lidx_improved.py → lidx_improved.xlsx
```

## 스크립트 상세

### 1단계: 데이터 수집

**dart_extract_save.py**
- DART Open API에서 사업보고서 XML 다운로드 → 퇴직급여 주석 섹션 발췌 → JSON 저장
- 입력: `corp_all_list.csv` (법인 목록 + rcept_no)
- 출력: `pension_extracts/{year}/{corp_code}.json`
- 실행: `python dart_extract_save.py 2024 2023 2022 2021 2020`
- 환경변수: `DART_API_KEY`

### 2단계: LLM 배치 추출

**dart_llm_batch.py**
- pension_extracts JSON에서 테이블 추출 → Claude Haiku로 20개 변수 추출
- 입력: `pension_extracts/{year}/*.json`
- 출력: `llm_extract_{year}.csv`
- 실행: `python dart_llm_batch.py 2024 2023`
- 환경변수: `CLAUDE_API_KEY`, `DART_API_KEY`
- 중단 후 재개 가능 (건별 즉시 저장)

### 3단계: 검증/보정

**validate_2014_2017.py** (메인 검증)
- 6단계 원본 매칭 + shock 환산 + 이상치 제거 + 비대칭 체크
- 입력: `llm_extract_{year}.csv`, `pension_extracts/{year}/`
- 출력: `llm_extract_{year}_merged.csv`
- 실행: `python validate_2014_2017.py 2024 2023 2022` (연도 인자)

**identify_reextract_targets.py**
- 전 연도 no_match/비대칭/부호반전 건 식별 → `reextract_targets.pkl`
- 실행: `python identify_reextract_targets.py`

**run_reextract.py**
- 식별된 대상의 민감도 LLM 재추출 → merged CSV 직접 업데이트
- 입력: `reextract_targets.pkl`, merged CSV, pension_extracts
- 실행: `python run_reextract.py [시작연도]`
- 환경변수: `CLAUDE_API_KEY`

**llm_sensitivity_down.py**
- 민감도 테이블만 경량 LLM 재추출 (4개 값)
- 실행: `python llm_sensitivity_down.py 2024 2023`
- 환경변수: `CLAUDE_API_KEY`

**fix_column_swap.py**
- DBO/SC/IC에 할인율/임금상승률/듀레이션이 혼입된 건 재추출
- 판별: DBO > 0 AND DBO < 100
- 실행: `python fix_column_swap.py`
- 환경변수: `CLAUDE_API_KEY`

**fix_dr_outliers.py**
- DR Mid > 15% → null, 삭제 기업(노무라/인바디) 제거
- 실행: `python fix_dr_outliers.py`

### 별도: Spot Rate Curve

**build_spot_rate.py**
- KIS Pricing YTM 시계열 → bootstrapping → Spot Rate Curve (EAR)
- 입력: `ytm/*.xlsx`
- 출력: `ytm/spot_rate_ear.csv`

### 최종: LIDX 엑셀 생성

**build_lidx_improved.py**
- 12개년 merged CSV → 수식 기반 엑셀 (9시트)
- 입력: `llm_extract_{2014~2025}_merged.csv`
- 출력: `lidx_improved.xlsx`
- 실행: `python build_lidx_improved.py`
- XML 후처리 포함 (openpyxl `<v></v>` 제거)

## 연간 업데이트 절차

새 회계연도 데이터 추가 시:

```bash
# 1. corp_all_list.csv에 새 연도 rcept_no 추가 (수동 또는 DART API)

# 2. 발췌 + LLM 추출
python scripts/dart_extract_save.py 2026
python scripts/dart_llm_batch.py 2026

# 3. 검증
python scripts/validate_2014_2017.py 2026

# 4. 보정 (필요시)
python scripts/identify_reextract_targets.py
python scripts/run_reextract.py 2026
python scripts/fix_column_swap.py
python scripts/fix_dr_outliers.py

# 5. LIDX 재생성 (available_years에 새 연도 추가 후)
python scripts/build_lidx_improved.py
```

## 환경변수

`.env` 파일 (git 미포함):
```
DART_API_KEY=...
CLAUDE_API_KEY=...        # company key
CLAUDE_API_KEY_PERSONAL=... # personal key
```

스크립트 실행 전 환경변수 로드:
```bash
# bash
export $(cat .env | xargs)

# PowerShell
Get-Content .env | ForEach-Object { $k,$v = $_.Split('=',2); [System.Environment]::SetEnvironmentVariable($k,$v) }
```
