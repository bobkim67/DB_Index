# DB_Index — DART XBRL 퇴직연금 부채 인덱스

## Claude API Key = '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'

## 프로젝트 개요

DART XBRL TSV 데이터에서 한국 상장사 DB형 퇴직연금 부채 인덱스 산출에 필요한 변수들의 ELEMENT_ID 패턴을 탐색하고 매핑한다. 기업마다 사용하는 ELEMENT_ID가 다르므로(IFRS 표준 / DART 확장 / 기업 자체 확장), 변수별 매핑 전략을 수립하는 것이 목표.

## 핵심 파일

| 파일 | 역할 |
|------|------|
| `xbrl_pension_explore.ipynb` | **메인 탐색 노트북** (8 셀, Cell 4부터 재실행 필요) |
| `dart_llm_batch.py` | **DART 사업보고서 LLM 배치 추출** (18개 변수, Claude Haiku) |
| `llm_extract_2024.csv` | v1 추출 결과 (1,711사, ActuarialGL 합계만) |
| `llm_extract_2024_v2.csv` | v2 추출 대상 (ActuarialGL 3개 하위항목 분리) |
| `pension_cik_mapping.csv` | CIK별 변수-ELEMENT_ID 매핑 (출력) |
| `pension_dbo_type.csv` | DBO 총액/순액 유형 판별 결과 (출력) |
| `pension_pattern_report.csv` | 변수별 패턴 요약 (출력) |
| `netdbo_only_cik_urls.csv` | NetDBO-only 496 CIK DART 공시 URL 목록 |
| `XBRL가이드.pdf` | DART XBRL TSV 파일 스키마 가이드 (7페이지) |

## 데이터 경로 & 파일 구조

```
DB_Index\
├── 2024_4Q\  (sub: ~3,000개사, val: 5.6M rows, 1.6GB)  ← 주 탐색 대상
├── 2023_4Q\  (sub: ~2,800개사, val: 3.8M rows, 1.1GB)
├── 2025_4Q\  (sub: 59개사, val: 72K rows)  ← 미완성
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

### 추출 변수 (18개, v2)

- **금액 15개**: DBO, PlanAsset, NetDBO, ServiceCost, InterestCost, InterestIncome, NetInterest, BenefitPayment, ActuarialGL, ActuarialGL_Financial, ActuarialGL_Demographic, ActuarialGL_Experience, RetirementBenefitCost, ExpectedContribution, DCPlanCost
- **범위 3개** (각 Min/Max/Mid): DiscountRate, SalaryGrowth, Duration

### 발췌 키워드 & 로직 (v3)

- `PENSION_KEYWORDS_TABLE`: 퇴직연금 TABLE 매칭 (2개+ 히트 시 발췌). `임금인상률` 변형 포함
- `HIGH_SIGNAL_KEYWORDS`: 1개만 매칭돼도 발췌 (`확정급여채무`, `확정급여부채`, `사외적립자산`, `당기근무원가`)
- `ASSUMPTION_KEYWORDS`: 1개만 매칭돼도 발췌 + **바스켓 우선 담기** (`할인율`, `할인률`, `임금상승률`, `임금인상률`, `승급률` 등 10개)
- `PENSION_KEYWORDS_TEXT`: 텍스트 단락 발췌용 (예상기여금, 듀레이션 등)

**v3 발췌 개선 (v2 대비)**:
1. **ASSUMPTION_KEYWORDS 신설**: 가정/듀레이션 키워드 1개 매칭으로 발췌 허용
2. **바스켓 우선순위**: priority_tables(가정 키워드 히트) → normal_tables 순서로 담기
3. **10KB 이상 테이블 스킵**: 재무제표 본문(BS/PL/CF/자본변동표 등) 노이즈 제거. 퇴직연금 핵심 테이블은 최대 ~3KB이므로 손실 없음

**v2 문제점 (v3에서 해결)**:
- 현금흐름표(101KB), 재무상태표(50KB) 등 거대 테이블이 `이자비용`/`사외적립자산` 키워드로 매칭 → 20KB 바스켓 독점 → 가정 테이블(~500B) 누락
- 결과: OK 1,093사 중 할인율 추출 19.3%, 임금상승률 17.2%
- v3 테스트 결과: 삼성전자 5→18개 변수 풀추출, 두산건설/경창산업/앤디포스 할인율·임금상승률 정상 추출

### LLM 프롬프트 주의사항

- **할인율/듀레이션 혼동 방지**: 할인율은 "보험수리적가정 테이블의 할인율/할인률 행"에서, 듀레이션은 "가중평균만기/듀레이션 텍스트"에서 추출하도록 명시. **9% 이상이면 듀레이션 오분류 가능성 재확인** 지시
- **임금상승률 레이블 변형**: 임금상승률, 기대임금상승률, 승급률, 임금인상률 등 다양한 표기 대응 (프롬프트에 `임금인상률` 명시 추가)
- **ActuarialGL 하위항목**: 재무적가정(할인율변화효과) / 인구통계적가정 / 경험조정 3개 분리 추출

### 알려진 이슈

- **할인율==듀레이션 오염 13사** (v1 결과): JB금융지주, 대상홀딩스, 아이티센엔텍, 매일유업, 깨끗한나라, 강원랜드, 삼보모터스, 한국알콜, DH오토웨어, 동국산업, JW중외제약, SK증권, 신세계센트럴. LLM이 듀레이션을 할인율로 오분류 → v3 프롬프트에 9% 가드레일 추가
- **솔트웨어**: 보험수리적가정 테이블이 `임금인상률` 키워드 사용 → `PENSION_KEYWORDS_TABLE`에 미포함으로 발췌 누락 → 키워드 추가로 해결
- **노무라인터내셔널펀딩피티이(01082834)**: XML 1개(29MB)에 모회사(Nomura Holdings) 20-F 번역본 포함 → 모회사 US GAAP 퇴직연금 데이터(백만엔)를 자사 것으로 오인 추출. SPC/외국법인 필터 필요

## 알려진 한계 & 주의사항

1. **할인율/듀레이션 커버리지**: v1에서 15~20% → v3 발췌 개선으로 재실행 예정
2. **DBO 순액 혼입**: `dart_PostemploymentBenefitObligations`는 기업에 따라 총액 또는 순액
3. **Entity 태그**: 기업 자체 정의 → 표준화 안 됨, 매핑 불안정
4. **lab.tsv 파싱**: 따옴표 문제로 `quoting=csv.QUOTE_NONE` 필수
5. **민감도 변수**: IFRS/DART 표준 태그 부재 → 수집 불가
6. **NetDBO-only CIK (~496)**: DBO/PA 분리 불가, 순액만 가용
7. **포스코이앤씨(CIK 00100814) 확인**: DART 주석에는 DBO/PA 별도 기재되나 XBRL 미태깅 → 순액만 존재
8. **아세아제지(CIK 00138729) VALUE 오류**: 2024_4Q DECIMALS=-6이나 VALUE가 10^6 과대 저장 (DART 파일링 오류). Cell 5에서 `VALUE_OUTLIER_CIKS` dict로 하드코딩 보정. 향후 분기 데이터 추가 시 이상치 재확인 필요
9. **DECIMALS='INF'**: 일부 CIK(아세아제지 등)에서 DECIMALS가 `'INF'` 문자열 → `pd.to_numeric` 시 `inf` → int 변환 실패. `dec.where(dec.abs() < 100, 0)`으로 0 치환 처리
10. **XML 1개 기업**: 본문+주석 통합 XML에서 첨부 번역본(모회사 재무제표 등)의 퇴직연금 데이터를 자사 것으로 오인할 수 있음

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

현재 2024년만 추출 완료 ($15.27). 2020~2023 추가 추출 필요.

**수정 포인트** (최소 변경):
- `main()`에 year 인자 추가 (CLI: `python dart_llm_batch.py 2023`)
- OUT_CSV → `llm_extract_{year}.csv` 연도별 분리
- `rcept_2024` 하드코딩 → `f'rcept_{year}'` 동적 참조
- `call_llm(tables_text, year)` 결산연도 동적

**비용**: 연도당 ~$15 × 4년 = ~$60 추가

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
