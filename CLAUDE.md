# DB_Index — DART XBRL 퇴직연금 부채 인덱스

## Claude API Key(personal) = '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'
## Claude API Key(company) = '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'

## 프로젝트 개요

DART XBRL TSV 데이터에서 한국 상장사 DB형 퇴직연금 부채 인덱스 산출에 필요한 변수들의 ELEMENT_ID 패턴을 탐색하고 매핑한다. 기업마다 사용하는 ELEMENT_ID가 다르므로(IFRS 표준 / DART 확장 / 기업 자체 확장), 변수별 매핑 전략을 수립하는 것이 목표.

## 핵심 파일

| 파일 | 역할 |
|------|------|
| `xbrl_pension_explore.ipynb` | **메인 탐색 노트북** (8 셀, Cell 4부터 재실행 필요) |
| `dart_llm_batch.py` | **DART 사업보고서 LLM 배치 추출** (20개 변수, Claude Haiku 4.5, 발췌 v4.3) |
| `dart_extract_save.py` | **5개년 발췌 텍스트 JSON 저장** (LLM 미호출, pension_extracts/) |
| `build_spot_rate.py` | **YTM → Spot Rate Curve(EAR) 산출** (bootstrapping, 5등급) |
| `pension_liability_index.py` | **부채증가율 산출** (공모/사모 OCIO 2차원 시나리오 테이블) |
| `validate_xbrl_llm.py` | XBRL vs LLM 교차검증 |
| `check_titles.py` | XML heading 탐색 (v4 발췌 근거) |
| `llm_extract_{2020~2024}.csv` | **v4.3 추출 결과** (5개년, 20변수, 연도당 ~1,100 OK) |
| `credit_grade_estimate.csv` | 기업별 추정 신용등급 (spot rate 매칭) |
| `ytm/spot_rate_ear_v2.csv` | **월말 spot rate curve** (5등급, 72월, 0.5~30Y) |
| `ytm/*.xlsx` | KIS Pricing YTM 시계열 (5등급, 2020~2025) |
| `pension_cik_mapping.csv` | CIK별 변수-ELEMENT_ID 매핑 (XBRL 검증용) |
| `pension_dbo_type.csv` | DBO 총액/순액 유형 판별 (XBRL 검증용) |
| `XBRL가이드.pdf` | DART XBRL TSV 파일 스키마 가이드 (7페이지) |

## 데이터 경로 & 파일 구조

```
DB_Index\
├── 2024_4Q\  (sub: ~3,000개사, val: 5.6M rows, 1.6GB)  ← 주 탐색 대상
├── 2023_4Q\  (sub: ~2,800개사, val: 3.8M rows, 1.1GB)
├── 2025_4Q\  (sub: 59개사, val: 72K rows)  ← 미완성
├── pension_extracts\  ← 5개년 발췌 JSON (v4.1, fs_type 포함)
│   ├── 2024\  (1,711파일, ~1,678사 테이블보유)
│   ├── 2023\  (1,711파일, ~1,673사)
│   ├── 2022\  (1,711파일, ~1,650사)
│   ├── 2021\  (1,711파일, ~1,648사)
│   └── 2020\  (1,711파일, ~1,638사)
└── XBRL가이드.pdf
```

각 분기 폴더에 11개 TSV (탭 구분, UTF-8):

| 파일 | 핵심 컬럼 | 용도 |
|------|----------|------|
| **val.tsv** | CIK, ELEMENT_ID, CONTEXT_ID, UNIT_ID, DECIMALS, VALUE | 실제 값 |
| **lab.tsv** | CIK, ELMT_ID, LANG, LABEL | 한글/영문 레이블 |
| **cntxt.tsv** | CIK, CONTEXT_ID, AXIS_ELEMENT_ID, MEMBER_ELEMENT_ID, PERIOD_* | 컨텍스트 상세 |
| **cal.tsv** | CIK, ELEMENT_ID, PARENT_ELEMENT_ID, WEIGHT | 계산관계 (DBO-PA=Net 검증) |
| **def.tsv** | CIK, ELEMENT_ID, PARENT_ELEMENT_ID, ARCROLE | Dimension 구조 |
| **sub.tsv** | CIK, REPORT_DATE, TAXONOMY_ID | 제출 기업 목록 |
| **elmt.tsv** | ELEMENT_ID, DATA_TYPE, PERIOD_TYPE, BALANCE | ELEMENT 정의 |
| pre.tsv, role.tsv, txn.tsv, txn-dts.tsv | | 참조용 |

**주의**: val.tsv는 `ELEMENT_ID`, lab.tsv는 `ELMT_ID` (컬럼명 다름)

## 노트북 셀 구조 (xbrl_pension_explore.ipynb)

| Cell ID | 내용 | 의존 | 메모리 |
|---------|------|------|--------|
| `cell-1-setup` | sub.tsv 로드, CIK 수, ELEMENT_ID prefix 분포 | - | 경량 |
| `cell-2-val-filter` | val.tsv 전체 로드 → 퇴직연금 키워드 필터 → `val_filtered` | Cell 1 | ~2.7GB → 필터 후 del |
| `cell-3-lab-filter` | lab.tsv 전체 로드 → LANG=ko + 키워드 필터 → `lab_filtered`, `lab_std` | Cell 1 | ~1.5GB → 필터 후 del |
| `cell-4-variable-mapping` | 13개 변수별 ELEMENT_ID 후보 분류 → `var_candidates` | Cell 2,3 | 경량 |
| `3gvhmjanf1q` (Cell 4.5) | 단일 CIK 진단 (3년×13변수, Dimension축 처리) | Cell 2,3,4 | 경량 |
| `glbcq0z2ld` (Cell 4.6) | 단일 CIK raw val+lab 전체 테이블 | Cell 1 | val.tsv 직접 로드 |
| `cell-5-cik-mapping` | CIK별 최적 ELEMENT_ID 선택 → `df_mapping` → CSV | Cell 4 | 경량 |
| `cell-6-dbo-type` | DBO 총액/순액 판별 → `df_dbo_type` → CSV | Cell 5 | cal.tsv 추가 로드 |
| `cell-7-context` | CONTEXT_ID, UNIT_ID, DECIMALS, def.tsv, cntxt.tsv 분석 | Cell 2 | def+cntxt 추가 로드 |
| `cell-8-report` | 패턴 요약 리포트 + 이상치 통합 → CSV | Cell 5,6 | 경량 |

**실행 순서**: Cell 1 → 2 → 3 (대용량, 각 1~3분) → 4 → 5 → 6 → 7 → 8
**부분 재실행**: Cell 4~8은 `val_filtered`, `lab_std`만 있으면 반복 실행 가능 (Cell 2,3 스킵)

## TSV 로딩 주의사항

```python
import csv
pd.read_csv(path, sep='\t', dtype=str,
    quoting=csv.QUOTE_NONE,    # LABEL에 따옴표 포함 → 파서 오류 방지
    on_bad_lines='warn',       # 파싱 불가 행은 경고 후 스킵
)
```
- lab.tsv의 LABEL 컬럼에 이스케이프 안 된 따옴표 포함 → `csv.QUOTE_NONE` 필수
- 가이드에는 "탭과 특수문자가 제거된 값"이라 되어있지만 실제로는 남아있음

## val_filtered 키워드 (v2)

Cell 2에서 사용하는 퇴직연금 전용 ELEMENT_ID 키워드 9개:
```
DefinedBenefit, PostemploymentBenefit, RetirementBenefit,
PlanAsset, FairValueOfPlanAsset, ActuarialAssumption,
BenefitPayment, PaymentsFromPlan, Severance
```
v1 대비 22→9개로 축소, val_filtered 481K→177K행 (63% 노이즈 감소)

## 분석 대상 변수 (13개, v2)

### 필수 7개

| 변수 | 설명 | 기대 UNIT | 대표 ELEMENT_ID |
|------|------|----------|----------------|
| DBO | 확정급여채무 (총액) | KRW | `ifrs-full_DefinedBenefitObligationAtPresentValue` |
| PlanAsset | 사외적립자산 공정가치 | KRW | `ifrs-full_PlanAssetsAtFairValue` |
| DiscountRate | 할인율 | PURE | `ifrs-full_ActuarialAssumptionOfDiscountRates` |
| SalaryGrowth | 임금상승률 | PURE | `ifrs-full_ActuarialAssumptionOfExpectedRatesOfSalaryIncreases` |
| Duration | DBO 듀레이션 | PURE | `ifrs-full_WeightedAverageDurationOfDefinedBenefitObligation2019` |
| ServiceCost | 당기근무원가 | KRW | `ifrs-full_CurrentServiceCostNetDefinedBenefitLiabilityAsset` |
| InterestCost | 이자비용 | KRW | `ifrs-full_InterestExpenseOnNetDefinedBenefitLiabilityAsset` |

### 선택 9개

| 변수 | 설명 | 기대 UNIT |
|------|------|----------|
| NetDBO | 순확정급여부채(자산) | KRW |
| BenefitPayment | 급여지급액 | KRW |
| ActuarialGL | 보험수리적손익 (OCI) 합계 | KRW |
| ActuarialGL_Financial | 재무적가정(할인율) 변동 보험수리적손익 | KRW |
| ActuarialGL_Demographic | 인구통계적가정(임금상승) 변동 보험수리적손익 | KRW |
| ActuarialGL_Experience | 경험조정 보험수리적손익 | KRW |
| InterestIncome | 이자수익 (사외적립자산) | KRW |
| RetirementBenefitCost | 퇴직급여 비용 합계 | KRW |
| NetInterest | 순이자(순금융원가) | KRW |

### 제외 (v2에서 삭제)

- SensitivityDiscount, SensitivitySalary: 커버리지 1~2 CIK → 수집 불가

## ELEMENT_ID 매핑 로직 (v2)

### 우선순위 스코어

```
복합 스코어 = ctx_score(0~7) + eid_priority(0~8) + label_penalty

ctx_score:
  Consolidated(not Separate): +4
  CFY(당기):                   +2
  eFY(기말, instant):         +1

eid_priority:
  ifrs-full_:  +8
  dart_:       +4
  entity{CIK}: +0

label_penalty (DBO만):
  ko_label에 '순확정' 포함: -10

dim_bonus (Dimension축):
  VAR_DIM_PREFERENCE에 따라 변수별 dbo_side/pa_side/neutral 지정
  dbo_side 변수: PlanAssetsMember -15, ObligationMember +5
  pa_side 변수: PlanAssetsMember +5, ObligationMember -15

eid_pattern_bonus (DiscountRate 전용):
  ActuarialAssumption 포함: +5
  CashFlowProjection/GoodwillImpairment/RiskFreeInterestRate: -15
```

VAR_DIM_PREFERENCE:
```
DBO: neutral, PlanAsset: pa_side, NetDBO: neutral,
DiscountRate: dbo_side, SalaryGrowth: neutral, Duration: dbo_side,
ServiceCost: dbo_side, InterestCost: dbo_side,
InterestIncome: pa_side, BenefitPayment: dbo_side,
ActuarialGL: dbo_side, RetirementBenefitCost: neutral, NetInterest: neutral
```

### UNIT_ID 필터

- 금액 변수 (DBO, PA, ServiceCost 등): `UNIT_ID == 'KRW'` 만 허용
- 비율 변수 (DiscountRate, SalaryGrowth, Duration): `UNIT_ID == 'PURE'` 만 허용
- NaN은 유지 (Duration에서 빈번)

### exclude_pattern

각 변수에 `exclude_pattern` regex 적용하여 오매핑 방지:
- DBO: OtherComprehensiveIncome, Remeasurement, GainLoss 등 제외
- PlanAsset: AdjustmentsFor 제외
- InterestCost: AdjustmentsFor (단, DefinedBenefit 포함 시 유지)

## DBO 총액/순액 판별 로직 (v2)

대상: DBO 또는 NetDBO 매핑된 전체 CIK

```
판별 우선순위:
1. IFRS DBO + PA 별도 존재 → '총액분리'
   - 교차 검증: DBO매핑값 ≈ IFRS_DBO → '총액(DBO매핑≈IFRS_DBO)'
   - 교차 검증: DBO매핑값 ≈ IFRS_DBO - PA → '순액(DBO매핑=Net)'
2. IFRS DBO만 (PA 없음) → '총액(PA없음)'
3. Dimension축 (NetDefinedBenefitLiabilityAssetAxis) → 'Dimension분리형'
4. cal.tsv WEIGHT 검증 → '순액(cal검증)'
5. 레이블 '순확정급여' → '순액(레이블)'
6. 레이블 '확정급여채무' → '총액추정(레이블)'
7. 레이블 '퇴직급여부채' → '순액추정(퇴직급여부채)'
8. 레이블 '종업원급여/충당' → '오분류의심'
9. DBO없고 NetDBO만 → 'NetDBO만'
10. 그 외 → '판별불가'
```

레이블 정규화: 앞의 번호(`3.`, `(2)` 등), 공백 제거

Anomaly 플래그: `ZERO_VAL`, `NEGATIVE_VAL`, `DBO_IS_NET`, `WRONG_LABEL`

## CONTEXT_ID 구조

```
CFY2024eFY_ifrs-full_ConsolidatedAndSeparateFinancialStatementsAxis_ifrs-full_ConsolidatedMember
│        │   │
│        │   └─ Dimension Axis + Member
│        └─ e=instant(기말) / d=duration(기간)
└─ CFY=당기 / PFY=전기 / BPFY=전전기
```

필터 규칙:
- BS 항목 (DBO, PA, NetDBO): `CFY{YYYY}eFY` + `ConsolidatedMember`
- PL 항목 (ServiceCost, Interest): `CFY{YYYY}dFY` + `ConsolidatedMember`
- 연결 없는 기업: Separate 또는 Default

## UNIT_ID / DECIMALS

- `KRW`: 원화 금액 | `PURE`: 비율(할인율 등) | `SHARES`, `KRWEPS`, `USD`
- DECIMALS: `0`=원, `-3`=천원, `-6`=백만 (VALUE는 이미 실제값 저장)

## CIK Tier 분류

| Tier | 조건 | CIK 수 |
|------|------|--------|
| Tier 1 | ifrs-full DBO + ifrs-full PA 둘다 존재 | ~352 |
| Tier 2 | dart_ DBO + dart_ PA 존재 (ifrs-full 없음) | ~69 |
| Tier 3 | DBO 총액만 (PA 없음) | ~120 |
| Tier 4 | 순액만 (DBO/PA 분리 불가) | ~817 |
| 제외 | 퇴직연금 XBRL 없음 | ~611 |

dart_ DBO/PA EID:
- `dart_PresentValueOfDefinedBenefitObligation`
- `dart_InvestedAssetForPostemploymentBenefit`
- `dart_FairValueOfPlanAssets`

## dart_llm_batch.py 상세

### 추출 변수 (20개, v4.2)

- **금액 17개**: DBO, PlanAsset, NetDBO, ServiceCost, InterestCost, InterestIncome, NetInterest, BenefitPayment, ActuarialGL, ActuarialGL_Financial, ActuarialGL_Demographic, ActuarialGL_Experience, RetirementBenefitCost, ExpectedContribution, DCPlanCost, SensitivityDR_1pct, SensitivitySG_1pct
- **범위 3개** (각 Min/Max/Mid): DiscountRate, SalaryGrowth, Duration
- SensitivityDR_1pct: 할인율 1%p 증가시 DBO 변동액 (감소→음수). Duration 역산용: `Duration = |ΔDBO| / (DBO × 0.01)`
- SensitivitySG_1pct: 임금상승률 1%p 증가시 DBO 변동액 (증가→양수)

### 발췌 로직 (v4, heading 기반 섹션 추출)

**v4 핵심 변경**: 전체 XML에서 키워드 매칭하던 방식 → **퇴직급여 주석 섹션을 먼저 특정**한 뒤 섹션 내 TABLE 전체 수집.

**발췌 전략 (우선순위)**:
1. **TITLE 태그 기반**: `<TITLE>22. 퇴직급여 (연결)</TITLE>` 등 구조화된 태그에서 섹션 범위 특정 (65사 해당)
2. **top-level heading 기반**: `<P>19. 확정급여제도</P>` 등 주석 번호 heading에서 섹션 범위 특정 (2,069사 해당, 96.6%)
3. **키워드 fallback (기존 v3)**: TITLE/heading 모두 실패 시 기존 키워드 매칭 방식

**heading 파싱 로직**:
- `_parse_top_headings()`: `<P>` 태그에서 `숫자. 한글제목` 패턴 추출
- 정규식 `(\d{1,3})[\.\)\s]\s*([가-힣].*)` — 한글 시작 조건으로 sub-heading(19.1, 2.18) 자동 제외
- `_find_pension_section()`: `PENSION_HEADING_KEYWORDS`로 퇴직급여 heading 매칭 → 해당 heading ~ 다음 top-level heading 구간 반환
- 여러 매칭 시 TABLE 포함 섹션 우선 (회계정책=텍스트 vs 데이터=TABLE 구분)

**PENSION_HEADING_KEYWORDS**: `퇴직급여`, `종업원급여`, `확정급여부채`, `확정급여제도`, `순확정급여부채`, `순확정급여자산`, `퇴직급여충당부채`, `퇴직급여충당금`, `확정급여채무`

**PENSION_KEYWORDS_XML (v4 확장)**: 기존 4개 + `퇴직급여`, `종업원급여`, `확정급여제도`, `퇴직급여충당부채`, `퇴직급여충당금`, `퇴직연금운용자산` 추가. K-GAAP 용어 기업도 XML 다운로드 단계에서 필터 통과 (IFRS DBO 기재 기업 70% 포함)

**CSV 컬럼 추가**: `extract_method` (TITLE / HEADING / KEYWORD)

**v4 테스트 결과**:
- 기존 "퇴직연금없음" 247사 중 30개 샘플 → v4: **100% 발견** (v3: 73%)
- v4 신규 발견 8건 (동양건설산업, 두원중공업 등 heading으로만 잡히는 기업)
- 송원산업 (다국적 3지역, 39개 테이블, 71KB): 바스켓 이슈 없음
- 교보증권: 섹션 16KB, 테이블 11개 — 노이즈 제로

**v4.1 추가 수정**:
- `ASSUMPTION_KEYWORDS`에 `평균만기` 추가 (텍스트에만 Duration 기재된 기업 발췌 포함)
- `download_consolidated_xml()` 반환에 `fs_type` (연결/별도) 플래그 추가
- `dart_extract_save.py` JSON에 `fs_type` 필드 추가

**v4.3 추가 수정 (2026-03-23)**:
- **TABLE 키워드 검증**: TABLE이 있어도 퇴직급여 핵심 키워드 없으면 skip → 다음 후보 (대한항공 등 35건 복구)
- **XML 1개 cnt_연결 수정**: `cnt_연결=0` 하드코딩 → 실제 카운트 (별도 오분류 1,016건 해소)
- **연결 우선 heading**: heading/섹션에 '연결' 포함 시 우선, '별도' 포함 시 후순위

**민감도 → Duration 역산 (LLM 배치 시 적용 예정)**:
- 할인율 1%p 변동시 DBO 변동액에서 `Duration = |ΔDBO| / (DBO × 0.01)` 역산
- 임금상승률 없는 기업도 민감도 TABLE에서 Dur_g 역산 가능
- Duration 커버리지: 42% → 51% 예상 (+155사)

**이전 버전 (참고)**:
- v3: ASSUMPTION_KEYWORDS 바스켓 우선순위, 10KB+ 테이블 스킵
- v2: 키워드 확대, 단위 탐색 강화
- v1: 기본 키워드 매칭 → 현금흐름표/재무상태표 노이즈로 할인율 19.3%

### check_titles.py — XML heading 탐색

DBO 값으로 XML 위치 특정 → 가장 가까운 TITLE/heading 역추적. v4 발췌 전략 수립의 근거 데이터.

**title_check_result.csv 결과** (3,236사):

| type | 건수 | 설명 |
|------|------|------|
| HEADING | 2,142 (74%) | 텍스트 heading 역추적 성공 |
| type=1 | 619 (21%) | XML 1개 기업 (heading 미탐색) |
| TITLE | 65 (2%) | 구조화 `<TITLE>` 태그 보유 |
| NONE | 60 (2%) | 퇴직연금 미발견 |

**top-level heading 빈도**:
- 퇴직급여 (4,708) / 종업원급여 (1,642) / 확정급여제도의재측정요소 (1,057) / 퇴직급여충당부채 (412) / 확정급여부채 (381) / 확정급여채무 (306)

**K-GAAP vs IFRS 분석** (퇴직급여충당부채 532사 중 50개 샘플):
- 연결+DBO: 70% — BS 항목명이 '충당부채'일 뿐 주석에 IFRS DBO 상세 있음
- 연결+충당만: 14% — LLM에서 DBO=null로 자연 제외
- 별도+DBO: 6% / 별도+충당만: 10%
→ XML 필터에 `퇴직급여충당부채` 유지 타당

### LLM 프롬프트 주의사항

- **할인율/듀레이션 혼동 방지**: 할인율은 "보험수리적가정 테이블의 할인율/할인률 행"에서, 듀레이션은 "가중평균만기/듀레이션 텍스트"에서 추출하도록 명시. **9% 이상이면 듀레이션 오분류 가능성 재확인** 지시
- **임금상승률 레이블 변형**: 임금상승률, 기대임금상승률, 승급률, 임금인상률 등 다양한 표기 대응 (프롬프트에 `임금인상률` 명시 추가)
- **ActuarialGL 하위항목**: 재무적가정(할인율변화효과) / 인구통계적가정 / 경험조정 3개 분리 추출

### 알려진 이슈

- **할인율==듀레이션 오염**: v1에서 13사 발생 → v4 프롬프트 가드레일("9% 이상이면 듀레이션 오분류 가능성 재확인")로 **v4.2에서 0건**
- **민감도 부호 반전 16건** (v4.2 결과): 할인율↑시 DBO증가(양수) 10건, 임금↑시 DBO감소(음수) 6건. LLM이 "감소"를 양수로 추출. 프롬프트 부호 지시 강화 필요
- **현대제철 SalaryGrowth_Max=82%**: DART 원본 파일링 오류 (8.2%의 소수점 누락). 수동 수정 완료
- **SC/DBO > 50% 이상치 20건**: 소규모 기업(DBO < 20억) 위주. 엠게임(355%), 골프존(184%) 등. 유니버스 필터(DBO>100억)로 자동 제외
- **노무라인터내셔널펀딩피티이(01082834)**: XML 1개(29MB)에 모회사(Nomura Holdings) 20-F 번역본 포함 → 모회사 US GAAP 퇴직연금 데이터(백만엔)를 자사 것으로 오인 추출
- **Haiku 3.5 EOL**: `claude-3-5-haiku-20241022`는 2026-02-19 EOL. API 400 반환. Haiku 4.5 사용 필수

### 2024년 LLM 추출 품질 (교차검증 결과)

| 변수 | 검증건수 | 정확도 | 비고 |
|------|---------|--------|------|
| 할인율/임금상승률 Min/Max | 2,046 | **98.7%** | 불일치 28건: 종속기업 범위 합산, 반올림 |
| Duration_Mid | 677 | **90.4%** | 불일치 65건: 텍스트 추출값(매칭 한계) |
| 전체 비율 변수 | 4,797 | **94.8%** | Mid 포함 (산술평균 계산값) |

## 알려진 한계 & 주의사항

1. **할인율/듀레이션 커버리지**: v1에서 15~20% → v4.2 결과: 할인율 91.1%, 임금상승률 89.3%, Duration 61.5%
2. **DBO 순액 혼입**: `dart_PostemploymentBenefitObligations`는 기업에 따라 총액 또는 순액
3. **Entity 태그**: 기업 자체 정의 → 표준화 안 됨, 매핑 불안정
4. **lab.tsv 파싱**: 따옴표 문제로 `quoting=csv.QUOTE_NONE` 필수
5. **민감도 변수**: XBRL 태그 부재 → LLM 추출로 해결 (SensitivityDR 85.0%, SensitivitySG 84.4%)
6. **NetDBO-only CIK (~496)**: DBO/PA 분리 불가, 순액만 가용
7. **포스코이앤씨(CIK 00100814) 확인**: DART 주석에는 DBO/PA 별도 기재되나 XBRL 미태깅 → 순액만 존재
8. **아세아제지(CIK 00138729) VALUE 오류**: 2024_4Q DECIMALS=-6이나 VALUE가 10^6 과대 저장 (DART 파일링 오류). Cell 5에서 `VALUE_OUTLIER_CIKS` dict로 하드코딩 보정. 향후 분기 데이터 추가 시 이상치 재확인 필요
9. **DECIMALS='INF'**: 일부 CIK(아세아제지 등)에서 DECIMALS가 `'INF'` 문자열 → `pd.to_numeric` 시 `inf` → int 변환 실패. `dec.where(dec.abs() < 100, 0)`으로 0 치환 처리
10. **XML 1개 기업**: 본문+주석 통합 XML에서 첨부 번역본(모회사 재무제표 등)의 퇴직연금 데이터를 자사 것으로 오인할 수 있음
11. **2020-2021 DART XML 인코딩**: EUC-KR 인코딩 사용 (2022+ UTF-8). `download_consolidated_xml()`에서 UTF-8 → EUC-KR → CP949 순 자동 감지 처리

## 코딩 컨벤션

- 한국어 변수명/주석 (XBRL 전문용어는 영문 원문 유지)
- 대용량 TSV 로드 후 반드시 `del df; gc.collect()`
- `usecols` + `dtype=str`로 메모리 절감
- 노트북 형태 유지 (val.tsv 2.7GB 재로드 방지)

---

## 향후 계획: 부채 증가율 Projection

### 목적

내년도 DB형 퇴직연금 부채 증가율(%)을 예측 → 자산운용 목표수익률로 설정.

### 접근: 시나리오 테이블 방식

```
부채증가율 = (SC - BP + IC) / DBO  +  (-Duration × ΔR)
             ─────────────────────     ────────────────
             물량 drift (안정적)        시장 효과 (할인율 시나리오)
```

### Step 1: dart_llm_batch.py 5개년 확장

**완료**: 5개년 추출 완료 (2020~2024, 총 8,311사, ~$57, Haiku 4.5).

**현재 구현**:
- `main()`에 year 인자 지원 (CLI: `python dart_llm_batch.py 2024 2023 2022 2021 2020`)
- pension_extracts JSON 기반 (DART API 미호출)
- OUT_CSV → `llm_extract_{year}.csv` 연도별 분리

**비용**: 총 ~$57 (Haiku 4.5) + 재추출 ~$8 = ~$65

### Step 2: pension_projection.py 신규 작성

5개년 LLM CSV 로드 → drift 추정 → 시나리오 테이블 생성.

**핵심 함수**:
- `load_multiyear_extracts(years)`: 5개년 CSV 병합, DBO>0 필터
- `estimate_drift_params(df)`: SC/DBO, BP/DBO, IC/DBO 비율 추정 (연도별 median + DBO 가중평균)
- `build_scenario_table(drift, duration, shocks)`: 할인율 시나리오별 부채 증가율
- `validate_reconciliation(df)`: DBO_T ≈ DBO_{T-1} + SC - BP + IC + AGL 검증

**시나리오 테이블 예시**:

| 시나리오 | 할인율변동 | 부채증가율 | → 목표수익률 |
|---------|----------|----------|------------|
| 금리 급락 | -100bp | +12~13% | ~12.5% |
| 금리 하락 | -50bp | +8~9% | ~8.5% |
| 금리 유지 | 0bp | +5~6% | ~5.5% |
| 금리 상승 | +50bp | +2~3% | ~2.5% |
| 금리 급등 | +100bp | -1~0% | ~-0.5% |

**검증 기준**:
- drift 안정성: SC/DBO, BP/DBO 변동계수 < 0.3
- reconciliation 잔차: |예측-실제| / 실제 < 5%
- 금리 유지 시 부채증가율 4~7% 범위 (한국 DB형 경험치)

### 현재 데이터 현황 (2024년 기준)

| 항목 | 값 |
|------|---|
| Reconciliation 가능 CIK | ~1,080 (DBO+SC+IC+BP+AGL 모두 보유) |
| DBO 합계 | ~539조 |
| SC/DBO | ~12.1% |
| BP/DBO | ~11.0% |
| 순 drift | ~+1.1% (+ IC/DBO ≈ 할인율) |
| Duration (DBO가중, 커버리지 낮음) | 6.71년 |

---

## 보험계리 방법론 (스캔 문서 S36C-0i26031310440.pdf 분석)

원본 위치: `C:\Users\user\Desktop\scan\` (9페이지, 19쪽)

### I. 해설 (p.1~3)

#### 1. 표준

K-IFRS 1019 적용. 주식회사의 외부감사에 관한 법률에 따라 회계처리기준의 적용을 받는 사업장.

#### 2. 정산

경영정상화조건부 근로조건변경 또는 다수 사업장의 퇴직연금제도 변경 시 정산발생.
- 정산이익: 부채감소(부의 과거근무원가)
- 정산: 근로조건변경으로 퇴직급여채무의 현재가치 변동

#### 3. 적용

K-IFRS 1019 호 76~98에 따른 보험수리적가정(할인율, 임금상승률, 퇴직률 등) 사용.

가. 경제적 가정: 할인율, 임금상승률, 기대수익률
- 가/나: 보고기간 종료일 시점 시장자료 기반 1년 단위 설정
- 기타: 각 보험수리적가정의 적절성을 재무보고일의 시장환경에 맞게 설정

나. 인구통계학적 가정: 사망률/퇴직률/장해율

#### 4. K-IFRS 제 1019 호

① 보험수리적가정 중 할인율 결정: 보고기간말 우량회사채 수익률 참조
② 보험수리적가정은 편의가 없고 서로 양립 가능한(compatible) 것
③ 재무적 가정: 보고기간 말의 시장 기대에 기초

### II. 기초론 (p.3~9)

#### 1. 표준 기호 정의

| 기호 | 의미 |
|------|------|
| r | 할인율 (discount rate) |
| g | 임금상승률 (salary growth rate) |
| w | 탈퇴율 (withdrawal rate, 퇴직률) |
| d | 사망률 (death rate) |
| S_x | x세 시점 급여 수준 |
| beta_x | x세 생존/재직 확률 |
| Y_x | x세 잔여근속연수 |

#### 2. 정산가/원가 관계

① 표준가정에서:
```
SC(당기근무원가) = NC(정상원가) x (1 + r)
```
- NC(Normal Cost): 금년 1년 근무에 대한 순수 채무 증분 (기초 시점 가치)
- (1+r): 기초→기말 이자 환산 팩터
- SC는 기말가치 기준

② 급여산식:
```
SC = x세 표준급여수준 x 비율 x 기간계수 의 형태
   = M_x 표준급여액 x d(death) x 비례할인계수 의 합산
```

③ 총급여현가(PBO = DBO):
```
DBO = x세까지의 표준급여 합산의 현재가치
```

#### 3. 정산구간 및 보유구간

- **정산구간**: 근로조건변경효과 반영 기간 (정산발생 ~ 정산완료)
- **보유구간**: 변경 전 기존 조건 유지 기간

#### 4. DBO의 Duration 및 민감도 지수 (p.8~9)

**DBO 공식** (개인별 합산):

```
DBO_x = sum over t [
    (S_{x+t} * beta_{x+t}) / 2 * (S_{x+t+1} * beta_{x+t+1}) / 2
    * (1 - r_x * d^r) * ... * (C_{x,t} * d^r * v^{t+0.5})
]
```

여기서:
```
A_x,t = S_x * alpha_{x+t} * (1 - r_x * d^r) * v^{t+...}    할인율 관련 항
B_x,t = (S_{x+t} * Y_{x+t}) / 2 + (S_{x+t+1} * Y_{x+t+1}) / 2    급여/근속 관련 항
C_x,t = (S_{x+t} * beta_{x+t}) / 2 + (S_{x+t+1} * beta_{x+t+1}) / 2    생존/재직 관련 항
```

**Duration 산출 공식**:

```
Dur = sum_i [ (0-x) * A_{x,i} * (1 - r_x * d^r) * v^{t+...} * [B * v^{t+0.5}] ]
    + sum_i [ ... * (t + 0.5) * [C * d^r * v^{t+0.5}] ]
    + sum_i [ ... * [D * d^r * v^{t+0.5}] ]
```

DBO의 Duration(=Dur)은 각 미래 현금흐름의 가중평균 지급시점으로, 할인율 1%p 변동 시 DBO 변동률의 근사치.

**민감도 관계**:
```
dDBO/DBO = -Dur x dR      (할인율 민감도)
dDBO/DBO = +Dur_g x dg    (임금상승률 민감도, Dur_g = Dur_r 근사)
```

#### 5. 회귀분석 기반 Duration 추정 (p.9)

대상: 밑면의 데이터를 바탕으로 회귀분석
```
S_t = b + a*x + e_t
```
- 대상: 최소제곱법으로 추정된 b, a로 할인율 민감도 환산
```
s = (C_{x,all}^{int} * I(C_{x,all}^{int}, S_t)) - (C_{x,all}^{der}) / (C_{x,all}) = d + delta * x
```
- 최종 Duration: S_t = b + delta * x

K-IFRS 1019 호 83 조에 따라 Duration은 할인율 민감도 분석에 필수 공시 사항.
피평가회사의 평균근속연수와의 상관관계를 통한 Duration 추정 가능 (보고용 AA 이상 40종 회사채 수익률 사용).

### III. 계산식/산출공식 (p.10~13)

#### 1. 산출요소/계산식

**입력 변수**:
- 근로자 K인 사업장의 확정급여채무(DBO_K)
- DBO_K 산출 시 근로자 K의 수: K개 법인
- 할인율(r), 임금상승률(g), 퇴직률, 사망률

**기초 근속년수별 채무 산출**:
```
beta_t = P_{xdate+t-xdate} / P_{xdate+1-xdate}   (t >= 1 인 경우)
       = 1                                         (t = 0 인 경우)
```

여기서:
- xdate: 평가기준일(=결산일)
- mdate: 측정일
- edate: 탈퇴(퇴직)일
- prt: 기존재직기간

**예시**: 평가기준일 2015.05.01, 총 근속연수가 365.25일 기준으로 계산하여 4단위마다 평가기준일이 존재.

```
t = xdate(or mdate) + edate - 1 = 365.25 일 (실적반영평가4)
```

**DBO_K 전개 (근속년수별 합산)**:

```
DBO_{x,t} = sum_{y=x}^{r+x} [
    (S_{x+t} * beta_{x+t}) / 2 * (beta_{x+t-x} / beta_{x+t}) * d^{r,v^{t+0.5}}
    + (S_{x+t+1} * Y_{x+t+1}) / 2 * ... * d^{r,v^{t+0.5}}
    + S_t * alpha_{x+t,y=x} * (1 - r_y * d^r) * v^{y-x}
]
```

**근속년수별 채무 전개 (전체 합산)**:

```
DBO_x = sum_{t=0}^{T} DBO_{x,t}
      = sum [근무원가 항 + 급여현가 항 + 이자부리 항]
```

#### 2. 총정상원가(NC) 산출 (p.12~13)

**정상원가(Normal Cost) = 근속 1년 증분 채무**:

```
NC_{k,x} = (DBO_{k,x+1} + NC_{k,x}) x (1 + r)
         = (d_t^r * Ben^d * d_t^r * Ben^d) * (1 + r)^0.5
           + (d_t^r * Ben^d + d_t^r * Ben^d) * v^0.5
           - (C_t^r) - (C_t^s) + delta
```

실질적으로:
```
(DBO_{k,x+1} + NC_{k,x}) x (1+1)
= (d_t^r * Ben^d * d_t^r * Ben^d * (1+r)^0.5)
  + (d_t^r * Ben^d + d_t^r * Ben^d) x v^0.5 - C^r - C^s + ...
```

**SC(당기근무원가) = NC x (1+r)**

**총정상원가의 정의**:
```
전체 NC = sum_K NC_K = NE (전체 정상원가)
```

**실제 적용 시**:
- 근로자별로 NC 계산 → 12개월 12기간 분할하여 월별 NC 종목 1기분 DBO 변동치 산출
- 총 12개월 분할 후 전체 합산 = SC

**시뮬레이션 파라미터 예시 (p.12)**:
```
퇴직급여공학적 상세 시뮬레이션 시점 가정 수
= YHAI: 1   = 1.1:HS_h
  YHAI: 1   = 1.1:SYH
  HS: 0.9   > SYH: HS: 0.9 > SYH
  HS_{a2}: 22 = SYH_{a2}
  1.1       = SYH: HS: 0.9 > SYH
  HS_{a1}: 12 = SYH_{a1}
  AY        = SHA + SYH_{a2} + DHA
```

### IV. 부채 인덱스 산출 방법론 (p.6~8, 14~15)

#### Base Up = bu (기본 승급률)

**인덱스 가중치 산정**:

```
w_{t-1} = 정상승급률(beta), 정상승급자수의 기말시점까지의 이미 결정된 것으로 가정
```

**인덱스 산출 공식**:

K-IFRS 1019 호 및 K-IFRS 1019 호 81 과 82 간의 관련에서:

```
bu = sum_{k=1}^{N_{reg}} (S_k / sum_{j=1}^{N_{reg}} S_j) x (NE(beta)/NE(base) - 1) x (w_k)
```

여기서:
- NE(beta): 변경(인상된) 가정 하의 총정상원가
- NE(base): 기본 가정 하의 총정상원가
- w_k: k번째 사업장의 가중치 (DBO 비중)
- S_k: k번째 사업장의 급여 수준

**Base Up 기반 인덱스 전개**:
- 정산 1, 2 벌도 기시채무를 직접 2, 3 벌도 기말채무와 산출
```
EE2_{xt} = EE1_{x+1} - NW1_{x+1} + W1_{x+1,t}
EE3_{xt} = EE2_{x+1} + EE2_{x+1,t} + NW2_{x+1,t} + W2_{x+1,t}
```

- 각각의 산출치를 기준연도에 따라 정리하여 부채지수 산출

**가시채무 및 총퇴직 산출**:

| 기능 | 산출 |
|------|------|
| 기초상각 | NE1_t + NE2_t + NE3_t = NE_all |
| 가시채무 및 총퇴직 | NW1_t + NW2_t + NW3_t = NW_all |
| 제용 | W1_1t + W1_2t + W1_3t = W1_all |

**인덱스 산출 최종 흐름** (p.7 그림):

```
각각의 산출치(EE, NE, NW, W) 기준
→ 파생/비교/지수산출(비율산출/합산) 가능
→ K-IFRS 1019 호 제 2 장 K-IFRS 1019 호 제 3 및 제 4 장의 데이터가 통합되면 산출 가능
```

### V. 부채증가율 분해식 및 목표수익률 산출 (p.14~19)

#### 1. 목표수익률 개요 (p.14)

**적립비율 관계**:

```
적립비율(FR) = 사외적립자산(PA) / 확정급여채무(DBO)

목표: FR >= 100% 유지
→ 자산수익률 >= 부채증가율
```

플로우차트 (p.14):
```
[확정급여채무감소] → [정산/축소/개정] → [전체변동관리]
                                           ↑ x100%
[급여지급] ← [확정기여전환] ← [관리부서/실행]
                                           ↑ x150%
[확정급여채무증가] → [근속효과/임금효과/이자비용] → [부채증가율]
```

#### 2. 부채증가율 분해 공식 (p.15)

**부채증가율 정의**:

```
R(t) = (DBO(T+1) - DBO(T)) / DBO(T)
     = SC / DBO(T)                           ... (가) 당기근무원가효과 (근속+임금 내재)
     - SY / DBO(T)                           ... (나) 급여지급효과
     + (DBO(T) x r - SY x r) / DBO(T)       ... (다) 이자비용효과
     + AL(DBO) / DBO(T)                      ... (라) 보험수리적손익
```

또는 재배열하면:

```
부채증가율 = SC/DBO - BP/DBO + IC/DBO + AGL/DBO
```

**간소화 근사 (p.15)**:

```
R(t) >= 0 일 조건: R(3) >= R(3), 즉 이자비용 >= 급여지급 시
```

**인덱스 산출**:

```
N_{index} = R(3) / (sum_{k} R(3)_k)
```

- 전체 부채증가율에서 각 구성요소의 기여도를 비율로 표시

**시나리오 분석**:
```
0 <= R(3) = R(3) >= 0         → (가) 정상운영상태 (부채 증가)
R(3) = R(3) <= 0              → (나) 부채감소상태
R(3) >= R(3) >= 0             → (다) 이자비용초과
R(3) <= R(3) <= 0             → (라) 급여초과상태
```

#### 3. K-IFRS 1019 호 종합적용 (p.16~17)

**① 근로자 K인 사업장의 확정급여채무(DBO)=이미 산출**: p.10~11의 DBO_K 공식

**② 전체퇴직급여비용의 재무제표상 인식**: AL(DBO) 이하의 시가현가차이가 나타나는 항목

**③ 사외적립자산 관리 관련**:

```
사외적립자산의 보유 및 DBO와의 관계
= 기초잔액 → 기대수익 → 실적차 → 기말잔액
```

순확정급여부채(자산) = DBO - PA

**④ 급여지급(EBP) 산식** (p.17):

```
EBP = (AS / 2) x t                        ... 전체기간동안의 급여지급(평균잔액 방식)
```

보다 정확하게:

```
EBP = (S_t * beta_t + S_{t+1} * beta_{t+1}) / 2 * (S_t * gamma_t + S_{t+1} * gamma_{t+1}) / 2 * d_t^r
```

**⑤ 당기근무원가**:

```
SC = NC x (1 + r)
```

- NC 내부에 임금상승률(g) 내재:
  ```
  NC proportional to S_current x [(1+g)/(1+r)]^{잔여근속}
  ```

**⑥ 이자비용 (Interest Cost)**:

```
이자비용 = (AL - EBP/2) x t
         = (DBO_기초 - 급여지급/2) x r
```

- AL = 기초 DBO (또는 전기말 DBO)
- EBP/2: 기중 급여지급의 반기 조정 (급여가 연중 균등 지급된다는 가정)
- 즉: **이자비용 = (기초DBO - BP/2) x r**
- 단순화: IC = DBO_기초 x r (BP가 작을 때)

**⑦ 기대수익**:

```
기대수익 = 사외적립자산 기초잔액 x 기대수익률
```

**⑧ 사외적립자산 기말 = 기초 + 기대수익 - 급여지급 + 기여금 + 보험수리적손익(자산측)**

#### 4. DBO 변동 조정표 (Reconciliation) (p.18~19)

**주1: AL_e = DBO 변동 조정표**

| 구분 | 기시 DBO | 기시채무 OB.기시가정 1 | 기시채무 OB.기시가정 2 | 기시채무 OB.기시가정 3 | 기시채무 OB.기시가정 4 | 기말 DBO |
|------|---------|---------------------|---------------------|---------------------|---------------------|---------|
| | 기시 | 기말 | 기말 | 기말 | 기말 | 기말 |
| 근무원가 | | 기말 | 기말 | 기말 | 기말 | 기시 |
| 급여지급 | | 기말 | 기말 | 기말 | 기말 | 기시 |
| 할인율변경 | | 기말 | 기말 | 기말 | 기말 | 기시 |
| 인구통계 | | 기말 | 기말 | 기말 | 기말 | 기시 |
| 퇴직률변경 | | | 기말 | 기말 | 기말 | 기시 |
| 사망률변경 | | | | 기말 | 기말 | 기시 |

**DBO 변동 분해**:

```
기말 DBO = 기초 DBO
         + SC (당기근무원가)              ... A
         + IC (이자비용)                  ... B
         - BP (급여지급)                  ... C
         + AGL_재무적 (할인율 변동)       ... D (= 기시DBO.기시가정1 순 변동)
         + AGL_인구통계 (퇴직률/사망률)   ... E (= 기시DBO.기시가정2 순 변동)
         + AGL_경험조정                   ... F(1) (= 실제 vs 가정 차이)
```

**② 기말치 = X = A + B + C + D + E + F(1)**

종합적으로:
```
DBO 변동 = SC - BP + IC + AGL
AGL = AGL_Financial + AGL_Demographic + AGL_Experience
```

**주요 관계**:

1) 보험수리적가정은 = 사전에 DBO 산출 시 반영되어 있는 가정과 보험계리사 보고서의 가정을 포함. 개별 동시 반영.
2) DBO 변동 조정표 = 기시 DBO → (각 원인별 변동 합산) → 기말 DBO
   - ① 기초잔액 DBO - DBO기시가정 변동 순 효과(B)
   - ② 기시잔액 DBO.기시가정 변동 후 순 변동분(C)
   - ③ 당기근무원가 적립 후 보험수리적손익 순 변동(D+E)
   - ④ 경험조정 후 기시채무 DBO.기시가정별/기시사망률/퇴직률/급여변동에 따른 기말치
3) 근속년수별 변동 반영 순서: 할인율변동 → 인구통계변동 → 경험조정

**4) 기말잔액 DBO 검증 vs 합산**:

```
기말잔액 DBO기시가정별 =
  기시잔액 DBO기시가정별(+기시사용추가분/분할)
  + 이자비용(기시잔액적립분추정치)
  - 급여지급(기시채무분지급추정치)
  + 보험수리적손익(기시사용변동추정치)
  + 공시잔액수정이전 = 공시잔액수정효과(5)

= 기시잔액 DBO기시가정별 + 기시사용추가분(분할) + 이자비용 - 급여지급 + 보험수리적손익 + 공시잔액수정치

② 기말치 = X = (A+B+C+D+E+F(1))
```

#### 5. 사외적립자산 변동 (p.18)

```
사외적립자산의 보유 및 변동:
  기초잔액
  + 기대수익
  - 급여지급(사외적립자산에서 지급분)
  + 기여금(사용자 불입)
  + (기시잔액변동치) - (비용/수수료차감)
  = 기말잔액

순확정급여부채(자산) = DBO - PA
```

#### 6. 최소적립금/사외적립자산 적립 기준 (p.15~16)

**적립비율 관리**:

| Case | 조건 | 상태 |
|------|------|------|
| ① | AS < AL x MR, MR=0.95 | 적립부족(심각), 근로자대표 통보 |
| ② | AL x MR < 0.95 < AS < AL x MR | 적립부족상태, 근로자대표+금융위원회 통보 |
| ③ | AL x MR < AS < AL x B | 경상상태, 적정/보험수리적/과거근무원가 충당 |
| ④ | AL x B <= AS < AL | 정상상태, 적정잔액 |
| ⑤ | AL <= AS < AL x 1.5 | 적립초과, 적정초과잔액(사업장변동자산 제한) |
| ⑥ | AL x 1.5 <= AS | 적립과다, 초과잔액 환불 가능 (AL 이상 5이면 되돌림 가능) |

여기서:
- AS: 사외적립자산(=PA)
- AL: 확정급여채무(=DBO)
- MR: 최소적립비율 (0.95)
- B: 목표적립비율

### VI. 모델 적용 시 핵심 산식 요약

#### 이자비용 정밀 공식

```
IC = (DBO_기초 - BP/2) x r
```

- 급여지급이 연중 균등 발생한다는 가정 하, 기중 평균 DBO에 할인율 적용
- 단순 근사: IC = DBO_기초 x r (BP가 작을 때)
- 따라서: IC/DBO_기말 = r x (DBO_기초/DBO_기말) = r x 1/(1+부채증가율)

#### 임금효과 분리 공식

```
SC(g) = NC(g) x (1+r)          ... 현행 임금상승률 g 하에서의 당기근무원가
SC(0) = SC(g) / (1+g)^Dur      ... g=0일 때의 당기근무원가 (순수 근속효과)

임금효과 = [SC(g) - SC(0)] / DBO = SC/DBO x [1 - 1/(1+g)^Dur]
순수근속효과 = SC(0) / DBO = SC/DBO x [1/(1+g)^Dur]
```

#### 부채증가율 2차원 시나리오

```
부채증가율(dR, dg) = base_drift + (-Dur_r x dR) + (Dur_g x dg)

where:
  base_drift = (SC - BP + IC) / DBO    현행 가정 하 순 drift
  -Dur_r x dR                          할인율 변동 효과
  Dur_g x dg                           임금상승률 변동 효과
  Dur_g = Dur_r (한국 DB형 근사: 퇴직시점 일시금 집중)
```

#### DBO Reconciliation 검증

```
DBO_기말 = DBO_기초 + SC - BP + IC + AGL
AGL = AGL_Financial(할인율) + AGL_Demographic(인구통계) + AGL_Experience(경험조정)

검증: |예측-실제| / 실제 < 5%
```
