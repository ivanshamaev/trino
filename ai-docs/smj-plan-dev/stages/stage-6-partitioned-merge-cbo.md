# Stage 6 — Partitioned merging exchange + CBO

Фаза 3 плана [risks/04 §4.4](../../smj-research/risks/04-implementation-plan-with-risk-gates.md).
Зависит от stage 4; калибровочные данные — из stage 5 и perf-трека (07 §7.6).

## Цель

Сортировка распараллелена по сплитам на скан-стадиях (без single-merge-бутылочного горлышка на
больших partition count) + режим COST_BASED по политике [15](../../smj-research/15-smj-selection-policy.md).

## Состав работ

| Файл | Изменение | Якорь |
|---|---|---|
| `ExchangeNode.java` | ослабление инварианта: разрешить REPARTITION + ordering (сегодня 4 checkArgument: remote⇒SINGLE, local⇒PASSTHROUGH, GATHER, один вход); новая фабрика `mergingPartitionedExchange` (REPARTITION + FIXED_HASH + orderingScheme) | ExchangeNode:104-111, :220-231; 03 §3.10 |
| `AddExchanges.visitJoin` MERGE-ветка | третий путь: partitioned exchange + **partial SortNode на скан-стадиях** + partitioned merging (сортировка распараллелена сплитами) | 09 §9.3; P1 |
| `StreamPropertyDerivations` | ordered-свойства нового exchange | 03 §3.10 |
| `PlanFragmenter.visitExchange` | перенос orderingScheme для REPARTITION — **подтверждено аудитом**: `RemoteSourceNode` строится с `exchange.getOrderingScheme()` безусловно, для любого типа exchange (правок не требуется; нужны только ослабление checks ExchangeNode + StreamPropertyDerivations) | PlanFragmenter:538-548 |
| `CostCalculatorWithEstimatedExchanges` | static `calculateSortedMergeJoinCostWithoutOutput` (по образцу существующего `calculateJoinCostWithoutOutput`, статически импортируемого `DetermineJoinDistributionType:230`) **с memory-компонентой** (`LocalCostEstimate.of(cpu, maxMemory, network)`) + `CostCalculatorUsingExchanges.visitJoin` | риски/05 §5.9.1; CostCalculatorUsingExchanges.visitJoin:180 |
| `DetermineJoinAlgorithm` | режим COST_BASED: выбор по cost-сравнению с δ-допуском (15 §15.6), пороги α/β/γ/δ → OptimizerConfig (`optimizer.join-merge-*`, 15 §15.7) | 15 §15.6-15.7 |
| `JoinOperatorInfo` | runtime-телеметрия algorithm | план §4.4 |
| Документация | ETL-пресет (spill on + компрессия, query.max-memory ↑, partition count 200-400, spill-тома локальные) + warning при конфиг-аномалиях | P6; 13 §13.x; 15 §15.11 |

## Acceptance (гейты фазы 3)

- stage-count(SMJ-план) == stage-count(hash-план) на эталонных запросах (I7-тест для новых путей);
- CBO на TPC-DS SF-100: SMJ выбирается **только** в big-big join'ах; ручные пороги
  (build > 1.5-2× памяти ИЛИ входы отсортированы) — диффы планов reviewed (P5);
- стаб-профиль: на 200-400 партициях wall-clock ≤ CPU-total/ядра × 1.5 (нет сериализации);
- OFF/COST_BASED-выключен → 0 диффов.

## Откат

COST_BASED → PREFER (конфиг); partitioned-merging-путь за флагом (fallback на stage 4-планировку).

## Согласованность (проверено против корпуса)

- [x] **risks/04 §4.4**: состав/гейты/откат зеркалированы; cost-якорь
  (`CostCalculatorWithEstimatedExchanges`, фикс pass 3) — тот же;
- [x] **15 §15.6-15.7**: COST_BASED-режим и пороги α/β/γ/δ (0.7/0.3/1.0/10%) — включены в
  правило; калибровка JMH/макро (07 §7.6) — в зависимостях этапа;
- [x] **risks/03 §3.5/§3.6 (P5/P6)**: LLC-порог и ETL-пресет — в составе/acceptance;
- [x] **risks/02 §2.7 (I7)**: стадий не добавляется — тест в acceptance;
- [x] **09 §9.7 п.2/п.5**: partitioned merging exchange и CBO как отдельные deliverables —
  совпадает (перенесены из MVP-нарезки в фазу 3 осознанно, отражено в §4.2 плана 04);
- [x] **11 §11.2 (S3-S5)**: shuffle-пути получают partitioned-merging — планы соответствуют
  матрице сценариев.
