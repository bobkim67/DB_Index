# DB_Index 개발 로그

## 2026-02-24 — v1 초기 구현

### 작업 내용
- `xbrl_pension_explore.ipynb` 생성 (markdown 1 + code 8 셀)
- DART XBRL TSV 데이터(2024_4Q + 2023_4Q)에서 퇴직연금 부채 인덱스용 15개 변수 탐색
- Cell 1~8 구현: 데이터 현황 → val/lab 필터 → 변수 매핑 → DBO 유형 판별 → CONTEXT 분석 → 리포트

### 기술적 결정
- **.py가 아닌 .ipynb 선택**: val.tsv 2.7GB를 한번 로드하면 Cell 4~8 반복 실행 가능. .py면 매번 재로드.
- **전체 로드 후 필터**: 메모리 충분(32GB+)하므로 chunked 처리 대신 전체 로드 → 키워드 필터 → `del` 해제
- **벡터화 CONTEXT_ID 스코어**: iterrows 루프 → `compute_ctx_score()` + `groupby().idxmax()`로 최적화

### 이슈 & 해결
1. **lab.tsv ParserError** (`EOF inside string at row 2945836`)
   - 원인: LABEL 컬럼에 이스케이프 안 된 따옴표 포함
   - 해결: `quoting=csv.QUOTE_NONE`, `on_bad_lines='warn'` 추가 (Cell 2, 3)

2. **XBRL가이드.pdf 미참조**
   - 가이드 읽기 전 코드 작성 → 가이드에서 UNIT_ID(PURE), DECIMALS 해석, cal.tsv WEIGHT 정보 확인
   - Cell 6에 cal.tsv WEIGHT 검증 추가, Cell 7에 UNIT_ID/DECIMALS 분석 추가

### v1 출력 결과 (실행 완료)
- `pension_cik_mapping.csv`: 28,534건 (2024_4Q + 2023_4Q)
- `pension_dbo_type.csv`: 1,110건 (dart_PostemploymentBenefitObligations 사용 CIK만)
- `pension_pattern_report.csv`: 15개 변수 요약

---

## 2026-02-25 — v1 결과 분석 & v2 수정

### v1 결과 분석에서 발견된 문제

#### 심각도 높음

1. **DBO 총액/순액 혼입 (33%)**
   - DBO 변수 2,428건 중 802건의 ko_label에 '순' 포함
   - 원인: `en_pattern`이 `DefinedBenefitObligation` 하나로 OCI 재측정 항목까지 매칭
   - `dart_PostemploymentBenefitObligations`가 기업에 따라 총액/순액 혼용

2. **대표 ELEMENT_ID 오류**
   - DBO, NetDBO, ActuarialGL 3개 변수의 대표가 모두 동일한 OCI 항목
   - `ifrs-full_OtherComprehensiveIncomeNetOfTaxGainsLossesOnRemeasurementsOfDefinedBenefitPlans`
   - 원인: 이 ELEMENT_ID가 가장 많은 CIK에서 사용 (DBO보다 OCI가 더 보편적)

3. **UNIT_ID 불일치 812건**
   - DBO에 PURE 7건 (민감도 비율 오매핑), USD/CNY/JPY 9건 (해외 자회사)
   - DiscountRate에 KRW 3건 (영업권 할인율 오매핑)
   - Duration 372건 NaN (UNIT_ID 미기재)

#### 심각도 중간

4. **판별불가 158건 (14.2%)**
   - 전부 has_ifrs_dbo=False, has_pa=False
   - 102건은 '확정급여부채' 레이블 → 현재 로직이 이 레이블을 '판별불가'로 분류

5. **dbo_type 미적용 1,526개 CIK**
   - dart_PostemploymentBenefitObligations만 대상 → 나머지 CIK 누락

6. **필수 9변수 완전 보유 371개 CIK (14.1%)**
   - 3개 미만 보유: 1,378개 (52.5%)
   - 대부분 DBO + ActuarialGL만 보유 (주석 XBRL 태깅 미흡)

#### 심각도 낮음

7. **SensitivityDiscount/SensitivitySalary**: 커버리지 2/1 CIK → 수집 불가
8. **비정상 레이블 51건**: '충당부채', '종업원급여부채' 등 비표준
9. **Entity-only CIK 70개**: 표준 태그 미사용

### v2 수정 내용

#### Cell 4 — VARIABLES 패턴 정밀화
- `en_pattern` 정밀화:
  - DBO: `DefinedBenefitObligation(?:AtPresentValue)?$` ($ 앵커로 OCI 차단)
  - ActuarialGL: `OtherComprehensiveIncome.*Remeasurement.*DefinedBenefit` (명시적 OCI 패턴)
  - InterestCost: `InterestExpense.*DefinedBenefit` (DefinedBenefit 필수)
- `exclude_pattern` 추가:
  - DBO: `OtherComprehensiveIncome|Remeasurement|GainLoss|Sensitivity|ChangeIn`
  - PlanAsset: `AdjustmentsFor` 제외
  - NetDBO: `CurrentServiceCost|InterestExpense|Payment` 등 PL 항목 제외
- `expected_unit` 추가: 변수별 기대 UNIT_ID (KRW/PURE)
- UNIT_ID 불일치 ELEMENT_ID → `var_candidates`에서 자동 제거
- 한글 키워드 정밀화:
  - DBO: `['확정급여채무']` (순액 관련 레이블 제거)
  - InterestCost: `['이자비용.*확정급여']` (확정급여 한정)
- 민감도 2개 변수 삭제 (SensitivityDiscount, SensitivitySalary)
- 변수 15개 → 13개

#### Cell 5 — 복합 스코어 + UNIT 필터
- UNIT_ID 사전 필터: `expected_unit`과 불일치 행 제거 (NaN은 유지)
- `compute_eid_priority()` 추가: ifrs-full(+8) > dart(+4) > entity(+0)
- 복합 스코어: `ctx_score + eid_priority + label_penalty`
- DBO 순액 penalty: `ko_label`에 '순확정' 포함 시 -10 (총액 ELEMENT_ID 우선 선택)
- `flag` 컬럼 추가: `NET_LABEL`, `ENTITY`, `UNIT:USD` 등 이상치 표시

#### Cell 6 — DBO 유형 판별 범위 확대
- 대상: `dart_PostemploymentBenefitObligations` 사용 CIK → **DBO/NetDBO 매핑된 전체 CIK**
- `normalize_label()`: 번호 접두사(`3.`, `(2)`) 공백 제거
- 분류 추가:
  - '확정급여채무' → `총액추정(레이블)` (이전에는 판별불가)
  - '종업원급여/충당' → `오분류의심` + `WRONG_LABEL` anomaly
  - DBO 없고 NetDBO만 → `NetDBO만`
- IFRS DBO/PA 값 조회 시 `compute_ctx_score` 적용 (연결/당기 우선)
- `anomaly` 컬럼: `ZERO_VAL`, `NEGATIVE_VAL`, `DBO_IS_NET`, `WRONG_LABEL`

#### Cell 8 — 리포트 업데이트
- 필수 변수 9개 → 7개 (민감도, RetirementBenefitCost, NetInterest → 선택)
- EXPECTED_UNIT → `VARIABLES['expected_unit']`에서 직접 참조 (하드코딩 제거)
- 이상치 플래그 통합 보고 (cik_mapping flag + dbo_type anomaly)
- Entity-only CIK 수 집계

### v2 기대 개선 효과

| 지표 | v1 | v2 예상 |
|------|----|---------|
| DBO 순액 혼입 | 802/2,428 (33%) | 대폭 감소 (exclude + penalty) |
| 대표 ELEMENT_ID 정확도 | DBO=OCI(오류) | DBO=DefinedBenefitObligation |
| UNIT_ID 불일치 | 812건 | ~0건 (사전 필터) |
| 판별불가 | 158/1,110 (14.2%) | 감소 ('확정급여채무'→총액추정) |
| dbo_type 대상 | 1,110 CIK | ~2,500+ CIK (전체 DBO/NetDBO) |

### 미해결 & 다음 작업

- [ ] v2 노트북 실행 후 결과 검증
- [ ] 총액 분리 가능 기업 (~42개)에서 실제 DBO/PA/Net 값 교차 검증
- [ ] 주석 XBRL 태깅 미흡 기업 대안 (DART Open API 주석 PDF 파싱?)
- [ ] 확정급여 → 확정기여 전환 기업 식별 (2023에 있고 2024에 없는 CIK)
- [ ] 실제 인덱스 산출 모듈 개발 (DB_Index → Spread_Sim 연동)

---

## 2026-02-25 continued — Cell 4.5 v2 + 키워드 정밀화 + Tier 분석

### Cell 4.5 v2 (진단 셀 버그 수정)
- CIK 00101044 진단에서 3건 버그 발견 & 수정:
  1. **NetDBO = DBO 동일값**: Dimension축(NetDefinedBenefitLiabilityAssetAxis) 미처리 → VAR_DIM_PREFERENCE dict + dim_bonus 스코어 추가
  2. **DiscountRate 7.82%**: CashFlowProjection 할인율 오매핑 → DISCOUNT_RATE_BONUS/PENALTY_PATTERNS 추가
  3. **BenefitPayment PA측 선택**: dbo_side로 변경 → DBO 측 급여지급(+12.7억) 선택
- `get_best_consolidated_value()`: Dimension 없는 행(총계) 우선 선택 추가

### Cell 2 v2 (val_filtered 키워드 정밀화)
- PENSION_KEYWORDS_EN: 22개 범용 → 9개 퇴직연금 전용으로 축소
- 제거: DiscountRate, Duration, ServiceCost, InterestExpense, InterestIncome, SalaryIncrease, WeightedAverageDuration, Sensitivity, IncreaseDecrease, Actuarial, Remeasurement, GainLoss 등
- 결과: 481K→177K행 (63% 감소), 16개 핵심 EID 전수 커버 확인
- 근거: IFRS 퇴직연금 EID는 거의 모두 DefinedBenefit/PostemploymentBenefit 포함

### Cell 4.6 (단일 CIK raw table)
- val_filtered → raw val.tsv 직접 로드로 변경 (해당 CIK 전체 ELEMENT_ID 조회)
- lab_std → raw lab.tsv 직접 로드로 변경 (LABEL_ROLE_URI 필터만)
- `퇴직연금` 마커 컬럼 추가 (●): 퇴직연금 키워드 매칭 행 표시
- 포스코이앤씨(CIK 00100814) 검증: 순확정급여부채 482,334천원 = XBRL 값 482,333,757원 일치

### Tier 분류 분석 결과
- ifrs-full DBO+PA 분리: 352 CIK (Tier 1)
- dart_ DBO+PA 분리: 69 CIK (Tier 2) — 이 중 38개는 dart_순액(1,043)에서 DBO/PA 분리 가능
- NetDBO-only: 496 CIK → DBO/PA 분리 불가 (DART URL 일괄 조회용 CSV 생성)
- OCI-only: ~1,503 CIK (퇴직연금 주석 태깅 미흡)
- 퇴직연금 XBRL 없음: 611 CIK

### 미해결 & 다음 작업
- [x] Cell 5 v2: Dimension축/DiscountRate 스코어링 전체 CIK 적용
- [x] Cell 6 v2: dart_ DBO/PA 추가 + Tier 분류 출력
- [x] Cell 2→3→4→5→6 전체 재실행 후 Tier별 결과 검증
- [x] Cell 2 키워드 축소 (22→9개) 적용 후 재실행 (481K→177K행)

---

## 2026-02-25 continued — Cell 9 교차검증 + Cell 10 CIK 변동

### Cell 9: 총액분리 가능 기업 DBO/PA/Net 교차검증

대상: IFRS DBO + PA 별도 보유 355개 CIK

주요 발견:
1. **PA 음수 29개**: XBRL에서 사외적립자산을 NetDBO 롤업 내 차감항목(음수)으로 기록하는 패턴
   - 부호 반전(abs) 적용 시 적립비율 정상 범위
2. **DBO ≤ 0인 9개**: 부호/매핑 이상 — 제외 필요
3. **NetDBO 매핑 정확도** (DBO>0 346개):
   - net_val ≈ DBO (총액=순액 동일 EID): 49개
   - net_val ≈ DBO-PA (순액 정확): 119개
   - 기타 불일치: 184개 (대부분 NetDBO가 DBO와 같은 값 가져옴)
4. **적립비율** (PA 부호보정, DBO>0 346개):
   - 중앙값 101.2%, 가중평균(DBO 규모 가중) 84.4%
   - 적립초과(PA>DBO): 177개, 적립부족: 169개
   - 분포: 75~125% 구간에 263개(76%) 집중

### Cell 10: 2023→2024 CIK 변동 분석

- 양쪽 모두: 2,109개
- 2023에만 (탈락): 95개
- 2024에만 (신규): 194개

탈락 95개 분류:
- A) 2024 미제출 (상장폐지/합병): 54개, DBO합계 9,731억원
  - DBO 100억+ 대형 7개 전부 미제출 (상폐/합병)
- B) 2024 제출 (태깅 중단): 41개, DBO합계 69억원
  - 대부분 소규모 (태깅 방식 변경이지 DB→DC 전환은 아님)
- DBO=0 탈락: 15개 (확정기여 전환 또는 소멸)

결론: **실질적 DB→DC 전환 탈락은 극소수**, 대형 탈락은 상장폐지/합병이 주원인

### 미해결 & 다음 작업
- [x] Cell 9, 10 노트북 실행 후 결과 확인
- [ ] 순액→총액 추정 로직 설계 (순액 55.8% CIK 처리 방안)
- [ ] 실제 값 추출 모듈 개발 (pension_cik_mapping → CIK별 변수값 일괄 추출)
- [ ] 인덱스 산출 모듈 (DB_Index → Spread_Sim 연동)

---

## 2026-02-26 — VALUE 이상치 탐지 & DECIMALS 보정

### 발견 경위
- Cell 9 교차검증에서 CIK 00138729의 DBO가 644,538,882억원(~64.5경)으로 출력
- 가중평균 적립비율(84.4%)이 이 1개사에 의해 지배됨

### DECIMALS 해석 분석

XBRL DECIMALS 필드와 VALUE 관계를 전수 조사:

| DECIMALS | KRW 행 수 | CIK 수 | VALUE 해석 |
|----------|----------|--------|-----------|
| 0 | 72,703 | - | VALUE = 원화 실제값 ✓ |
| -3 | 60,359 | 354 | VALUE = 원화 실제값 ✓ (DECIMALS는 정밀도 표시일 뿐) |
| -6 | 25,764 | 208 | VALUE = 원화 실제값 ✓ (대부분) |
| 양수(2~8) | 199 | - | PURE 비율 등 |

**결론**: `VALUE는 이미 실제값 저장` (CLAUDE.md 기존 기술 맞음). DECIMALS는 정밀도 메타데이터일 뿐.

### 이상치: 아세아제지(00138729) 1건

| 항목 | raw VALUE(억원) | VALUE×10^DEC(억원) | 판정 |
|------|----------------|-------------------|------|
| DBO | 644,538,882.2 | 644.5 | DART 파일링 오류 — VALUE가 10^6 과대 저장 |
| PA | 543,701,605.0 | 543.7 | 동일 |

- DECIMALS=-6, 같은 CIK의 다른 항목(민감도)은 DECIMALS=0으로 정상
- DART 공시에서도 단위가 "백만원"으로 표시 (본래 "원"이어야 맞음)
- 삼성전자(00126380, DBO 17조), 한전(00159193, DBO 5조)은 raw VALUE 정상 확인

### 보정 방법: 하드코딩

Cell 5 시작부에 `VALUE_OUTLIER_CIKS` dict 추가:
```python
VALUE_OUTLIER_CIKS = {
    '2024_4Q': {'00138729'},   # 아세아제지
}
```
- 해당 CIK의 val_filtered VALUE를 `VALUE × 10^DECIMALS`로 in-place 보정
- Cell 5 이후 모든 셀(6~10)에 자동 반영
- 향후 분기 추가 시 이상치 확인 후 dict에 추가

### 버그 수정: DECIMALS='INF' → IntCastingNaNError

아세아제지 DECIMALS 분포: `0`(3,296행), `INF`(173행), `-6`(137행), `7`(2), `6`(2)
- `pd.to_numeric('INF')` → `inf` → `.astype(int)` 에서 `IntCastingNaNError`
- 수정: `dec.where(dec.abs() < 100, 0)` 로 INF/비정상값을 0 치환 후 int 변환
- DECIMALS=0이면 `10^0=1`이라 VALUE 변동 없음 (안전)

### Cell 9 결과 보정 영향
- 아세아제지 DBO: 64.5경 → 644.5억 (정상)
- 가중평균 적립비율: 아세아제지 지배 해소 → 재계산 필요

### 미해결 & 다음 작업
- [ ] Cell 5~10 재실행하여 보정 결과 검증
- [ ] 순액→총액 추정 로직 설계 (순액 55.8% CIK 처리 방안)
- [ ] 실제 값 추출 모듈 개발 (pension_cik_mapping → CIK별 변수값 일괄 추출)
- [ ] 인덱스 산출 모듈 (DB_Index → Spread_Sim 연동)

---

## 2026-03-11 — dart_llm_batch.py v2 (ActuarialGL 분리 + 프롬프트/키워드 개선)

### 1. ActuarialGL 하위항목 3개 분리 추출

`AMOUNT_VARS`에 추가 (15개 → 18개 변수):
- `ActuarialGL_Financial` — 재무적가정(할인율) 변동
- `ActuarialGL_Demographic` — 인구통계적가정(임금상승) 변동
- `ActuarialGL_Experience` — 경험조정

LLM 프롬프트에 3개 변수 설명 추가. 기존 `ActuarialGL` 합계는 유지.
필수변수(essential) 판정에는 영향 없음 (선택 변수).
OUT_CSV를 `llm_extract_2024_v2.csv`로 변경 (v1 결과 보존).

**검증**: 유신(00144650) 테스트 → 합계 2,002,081 = 301,460 + 193 + 1,700,428 정확히 일치.

### 2. 할인율/듀레이션 혼동 방지 (프롬프트 개선)

**문제**: JB금융지주(00980122), 강원랜드(00255619)에서 듀레이션 값을 할인율로 오분류.
- JB금융: 할인율 실제 4.46~5.17% → LLM이 6.11~13.45(듀레이션)를 할인율로 추출
- 강원랜드: 할인율 실제 3.62~4.47% → LLM이 7.34~8.33(듀레이션)을 할인율로 추출
- JB금융 임금상승률(3.50~5.18%)도 누락

**원인**: 프롬프트에 "DiscountRate: 할인율 (%)"만 적혀있고 출처 안내 없음. 할인율은 테이블에, 듀레이션은 텍스트에 기재되는데 LLM이 텍스트의 범위 숫자를 먼저 잡음.

**해결**: 프롬프트에 출처와 레이블 매칭 명시.
```
- DiscountRate: 보험수리적가정 테이블의 "할인율/할인률" 행에서 추출. 듀레이션/만기와 혼동 금지.
- SalaryGrowth: 보험수리적가정 테이블의 "임금상승률/기대임금상승률/승급률" 행에서 추출.
- Duration: "가중평균만기/가중평균듀레이션/듀레이션" 텍스트 또는 테이블에서 추출. 할인율과 혼동 금지.
```
값 범위 힌트(3~6%, 5~15년 등)는 극단적 케이스 누락 우려로 제외.

### 3. 발췌 키워드 보완

**문제**: 솔트웨어(01390399)에서 보험수리적가정 테이블(할인율 3.28%, 임금인상률 5.00%) 발췌 누락.
- 원인: `PENSION_KEYWORDS_TABLE`에 `임금상승률`만 있고 `임금인상률` 없음. 테이블에 키워드 1개만 매칭되어 발췌 조건(2개+) 미달.

**해결**:
- `PENSION_KEYWORDS_TABLE`에 `임금인상률/임금 인상률/임금인상율/임금 인상율` 추가
- `HIGH_SIGNAL_KEYWORDS`에 `당기근무원가` 추가 (1개만 매칭돼도 발췌)

### 4. 발견된 이슈 (미해결)

- **노무라인터내셔널펀딩피티이(01082834)**: ZIP 내 XML 1개(29MB)에 모회사(Nomura Holdings) SEC 20-F 국문번역본 포함. 모회사 US GAAP 퇴직연금 데이터(백만엔)를 자사 것으로 오인 추출. XML 1개인 기업이 얼마나 되는지 전수 확인 필요하나 보류 중.

### 수정 파일
- `dart_llm_batch.py` (AMOUNT_VARS, 프롬프트, 키워드, OUT_CSV)
- `CLAUDE.md` (핵심 파일, 변수 목록, dart_llm_batch.py 상세, 알려진 한계)

### 미해결 & 다음 작업
- [x] 발췌 로직 v3 개선 (할인율/임금상승률 추출률 대폭 향상) → 2025-03-11 완료
- [ ] XML 1개 기업 전수 확인 (모회사 번역본 오인 방지)
- [ ] 순액→총액 추정 로직 설계
- [ ] 인덱스 산출 모듈 (DB_Index → Spread_Sim 연동)

---

## 2025-03-11 — 발췌 로직 v3 + LLM 프롬프트 개선

### 문제

v1 추출 결과 (`llm_extract_2024.csv`, 1,711사) 분석:
- OK 1,093사 중 **할인율 19.3%** (211사), **임금상승률 17.2%** (188사)만 추출
- 듀레이션은 58.3% (637사) — 텍스트 발췌에서 잡힘
- 할인율==듀레이션 동일값 오염: **13사** (JB금융지주 9.78, 대상홀딩스 9.06, 강원랜드 7.83 등)

**근본 원인**: 재무제표 본문(현금흐름표 101KB, 재무상태표 50KB 등)이 `이자비용`/`사외적립자산` 키워드로 매칭 → 20KB 바스켓 독점 → 보험수리적가정 테이블(~500B) 누락

테이블수 vs 할인율 추출률 상관관계:
- 1~4개: 0%, 5~8개: 2~5%, 9~10개: 17~22%, 11+개: 40~100%

### 해결 (dart_llm_batch.py v3)

**1. `ASSUMPTION_KEYWORDS` 신설 (10개)**
```python
ASSUMPTION_KEYWORDS = [
    '할인율', '할인률',
    '임금상승률', '임금상승율', '임금인상률', '임금인상율', '승급률', '승급율',
    '가중평균만기', '듀레이션',
]
```
- 1개만 매칭돼도 발췌 허용 (기존: 일반 2개+ 필요)
- 공백 변형(`임금 상승률`)은 `text_nospace`에서 매칭되므로 별도 추가 불필요

**2. 바스켓 우선순위 분리**
- `priority_tables` (가정 키워드 히트) → `normal_tables` 순서로 담기
- 가정 테이블(~500B)이 먼저 바스켓에 확보됨

**3. 10KB 이상 테이블 스킵**
- 퇴직연금 핵심 테이블 최대 크기: ~3KB (변동내역)
- 10KB 이상: 전부 재무제표 본문(BS/PL/CF/자본변동표)
- `len(table_html) > 10000: continue` 한 줄 추가

**4. LLM 프롬프트 개선**
- DiscountRate: "9% 이상이면 듀레이션 오분류 가능성 재확인" 가드레일 추가
- SalaryGrowth: `임금인상률` 레이블 명시 추가

### 테스트 결과

| 기업 | v1 할인율 | v3 할인율 | v1 임금상승률 | v3 임금상승률 | v1 변수 | v3 변수 | v1 tbl | v3 tbl |
|------|----------|----------|-------------|-------------|--------|--------|--------|--------|
| 삼성전자 | null | 4.0~5.1% | null | 2.0~5.9% | 5개 | **18개** | 8 | 12 |
| 두산건설 | null | 3.3% | null | 4.0% | - | - | 8 | 11 |
| 경창산업 | null | 3.66~4.12% | null | 2.51~3.96% | - | - | 4 | 8 |
| 앤디포스 | null | 3.75% | null | 5.0% | 1개 | **9개** | 2 | 17 |

### 오히트 분석

v3에서 새로 잡히는 노이즈 테이블:
- 증자 할인율 (`기준주가에 대한 할인율 10.0%`) — LLM이 정확히 무시
- 영업권 DCF 할인율 (`영구성장률/할인율 12.72%`) — LLM이 정확히 무시
- 합쳐도 ~2~4KB, 20KB 바스켓에 영향 미미

### 수정 파일
- `dart_llm_batch.py` (ASSUMPTION_KEYWORDS, extract_pension_tables v3, LLM 프롬프트)
- `CLAUDE.md` (발췌 로직 v3, 부채 증가율 Projection 계획)

### 다음 단계
- [ ] v3 로직으로 895사 재실행 (OK+할인율null 882 + 오염 13)
- [ ] 비용: DART 895회 + LLM 895회 ≈ ~$6, ~15분
- [ ] 5개년 확장 (2020~2023, 연도당 ~$15)
- [ ] pension_projection.py 작성 (drift 추정 + 시나리오 테이블)
