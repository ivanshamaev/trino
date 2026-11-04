# Stage 3 — Правило `DetermineJoinAlgorithm`, eligibility и гварды

PR3 фазы 1 ([risks/04 §4.2 п.3](../../smj-research/risks/04-implementation-plan-with-risk-gates.md);
правила выбора — [15 §15.2-15.7](../../smj-research/15-smj-selection-policy.md)).
Зависит от stage 1 (поле) и stage 0 (SortingProperty для скидок; для forced-MVP не блокирует).

## Цель

Session/config-пара + правило, которое выбирает MERGE **только** по жёстким рамкам политики 15,
добавляет `IS NOT NULL` для INNER и неактивно при OFF (0 диффов планов).

## Состав работ

| Файл | Изменение | Якорь |
|---|---|---|
| `SystemSessionProperties` | `prefer_merge_join` (enum OFF/PREFER/COST_BASED/FORCE; MVP: OFF/PREFER/FORCE) — прецедент нейминга `prefer_partial_aggregation` | :131 |
| `OptimizerConfig` | `optimizer.prefer-merge-join` (default OFF) | прецедент :76/:517 (`optimizer.prefer-partial-aggregation`) |
| новое `iterative/rule/DetermineJoinAlgorithm.java` | MUST-гейты 15 §15.2: INNER/LEFT; equi непусты; все ключи `isOrderable()`, без DOUBLE/REAL (до C1-guards; N5); `distributionType==PARTITIONED`; `retryPolicy==NONE` (I1); `distributed_sort==true` (OptimizerConfig:64); `prefer_merge_join != OFF`. W-триггеры 15 §15.4 (PREFER-режим): W1 безусловен (colocated ∧ sortedL ∧ sortedR); W2 `Bp > α·U`; W3 big-big; W4 `(sortedL ∨ sortedR) ∧ Bp > β·U`. Регистрация в `PlanOptimizers` **после** `DetermineJoinDistributionType` (:886), до `AddExchanges`. **Упорядоченность безопасна и в том же батче**: паттерн DDT — `joinNode.getDistributionType().isEmpty()` (матчит только «нерешённые» join'ы и никогда не переписывает установленное распределение) ⇒ DDT не флипнет PARTITIONED после нашей пометки; рекомендуемый вариант — отдельным optimizer-entry сразу после батча DDT (детерминизм для ревью) | 15 §15.6 (псевдокод); PlanOptimizers:886; DDT-паттерн (верифицировано аудитом) |
| — // — (продолжение) | для INNER добавляет `k IS NOT NULL` на обе стороны (семантически эквивалентно; NULL-хвост отрезается до shuffle + Iceberg-прунинг по null_count) | C9 (risks/06 §6.4 R-C9.2); авто-добавления в Trino нет (проверено, §5.13.1) |
| — // — (продолжение) | DF-гейтинг: для non-INNER не собирать DF (несобираемые подчистит `RemoveUnsupportedDynamicFilters`) | I3, risks/08 §8.2 |
| `AdaptiveReorderPartitionedJoin` | skip при `algorithm=MERGE` | I1 |
| `UseNonPartitionedJoinLookupSource` | skip при `algorithm=MERGE` (правило выполняется после AddLocalExchanges и перестраивает build-side local exchange по hash-экономике) | I14; PlanOptimizers:1004/1012 |
| `TestDetermineJoinAlgorithm` | матрица W/N-переходов 15 §15.10: {colocated × sortedL/R × Bp/U × L/U} → ожидаемый алгоритм; OFF → 0 диффов | по образцу `TestDetermineJoinDistributionType` |

## Тесты / Acceptance

- план-тесты: все MUST-нарушения → hash; W1 → MERGE на любых размерах; N1/N2/N4 → hash;
  INNER-план содержит `IS NOT NULL` обеих сторон, LEFT — нет; DF собирается только для INNER;
- гвард-тесты: adaptive-flip и `UseNonPartitionedJoinLookupSource` не меняют SMJ-план (golden);
- OFF: 0 диффов планов в CI.

## Откат

`prefer_merge_join=OFF` (default) — правило выходит сразу после первой проверки гейтов.

## Согласованность (проверено против корпуса)

- [x] **15 §15.2-15.7/§15.10**: MUST/W/N/пороги/псевдокод/CI-инвариант — состав работ
  реализует дословно; пороги α/β/γ/δ — константы этапа 6 (COST_BASED), в PREFER — те же дефолты;
- [x] **risks/04 §4.2 п.3**: eligibility-список совпадает (включая distributed_sort — фикс
  восьмого прохода §5.11.1);
- [x] **risks/01 §1.2/§1.4/§1.6** (C2/C4/C6): isOrderable-гейт, полный (не префиксный) матчинг
  scheme, cast → missed-opportunity (не eligibility-блокер);
- [x] **risks/02 §2.1 (I1)**: retryPolicy-гейт; **§2.13 (I14)**: гвард
  UseNonPartitionedJoinLookupSource;
- [x] **risks/06 §6.4**: `IS NOT NULL` только для INNER (LEFT/FULL не фильтруются —
  null-padded строки);
- [x] **risks/08 §8.2**: DF INNER-only;
- [x] **03 §3.3** (рецепт property) и **§3.10** (карта: правило после
  DetermineJoinDistributions/до AddExchanges; гварды adaptive) — совпадают;
- [x] **09 §9.4**: правило выбора и его место — то же.
