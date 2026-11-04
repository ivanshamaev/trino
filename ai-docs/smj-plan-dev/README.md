# SMJ Dev Plan: план реализации Sorted Merge Join в Trino (Java)

Единый план разработки. Детализация по этапам — в [stages/](stages/).
Основа: риск-гейтованный план [risks/04](../smj-research/risks/04-implementation-plan-with-risk-gates.md)
(фазы 0–4), политика выбора [15](../smj-research/15-smj-selection-policy.md) (MUST/W/N-правила),
синтез [10](../smj-research/10-synthesis-and-recommendations.md), карта правок
[03 §3.10](../smj-research/03-smj-integration-points.md), дизайн оператора
[05 §5.1-5.6](../smj-research/05-brainstorm-java-developer.md). Все якоря кода верифицированы
десятью проходами ([risks/05](../smj-research/risks/05-verification-report.md) §5.1-5.13).

## 1. Цель и рамки

**Цель**: equi-JOIN с bounded memory на входах, не помещающихся в RAM (класс 15B×17B),
через sorted merge join как дополнительную стратегию `JoinNode` — с нулевой регрессией при OFF.

**Не-цели (MVP)**: RIGHT/FULL (stage 5), CBO-режим (stage 6), FTE (`retryPolicy != NONE`),
SemiJoin/ReorderJoins (stage 7), Velox.

## 2. Зафиксированные архитектурные решения (не пересматриваются в рамках плана)

| # | Решение | Обоснование / якорь |
|---|---|---|
| A1 | Маркер алгоритма — поле `JoinNode.algorithm: Optional<JoinAlgorithm>` (внутренний enum по прецеденту `DistributionType`, JoinNode:52). **Не** новый план-узел, **не** значение DistributionType | консенсус 5 ролей (10 §10.2); 8 семантических точек матчинга DistributionType не трогаем; openai/06 ADR1 пришёл к тому же |
| A2 | Поле — в единственном публичном `@JsonCreator`-конструкторе (JoinNode:78) **без дефолт-перегрузок**; приватный полный (:105) — только делегирование. Компилятор заставит пробросить все 45 мест `new JoinNode(` | I13; прецеденты `spillable`/`maySkipOutputDuplicates` |
| A3 | Оператор — два пайплайна + мост: build = push-`Operator` `MergeJoinBufferOperator` (sink), probe = `WorkProcessorOperator` `MergeJoinOperator` через `WorkProcessorOperatorAdapter` (паттерн `spilling.LookupJoinOperatorFactory:50`); мост `MergeJoinSource` через `JoinBridgeManager<MergeJoinBridge>` | 05 §5.1-5.2; probe-spill упирается в `WorkProcessorOperator.java:21` TODO → вся тяжёлая память на build |
| A4 | Канонический `OrderingScheme` обеих сторон: equi-ключи **ASC, NULLS_LAST** | = SQL-дефолт Trino (`OrderingTranslator:25-29`); C3 |
| A5 | НейминG: пакет `io.trino.operator.join.merge`; классы `MergeJoinOperator`/`MergeJoinBufferOperator`/`MergeJoinSource` | §4.9 плана 04; коллизии с `io.trino.operator.MergeOperator` нет |
| A6 | Правило `DetermineJoinAlgorithm` — отдельным iterative-правилом **после** `DetermineJoinDistributionType` (PlanOptimizers:886), **до** `AddExchanges` | §4.2 плана; 09 §9.4 |
| A7 | Выбор — только по жёстким рамкам [15](../smj-research/15-smj-selection-policy.md): 7 MUST-гейтов, W1–W4 (S1-GOLD безусловен), N1–N5-запреты; режимы OFF→PREFER→COST_BASED→FORCE | «не платить CPU там, где RAM хватает» |
| A8 | Guards: adaptive (I1), `retryPolicy==NONE`, `AddLocalExchanges` single-stream (P11), `UseNonPartitionedJoinLookupSource` skip-MERGE (I14, PlanOptimizers:1004/1012), `distributed_sort=true` (OptimizerConfig:64), DF — только INNER | §4.2/§4.9 плана |
| A9 | Корректность в горячем цикле: NaN-guard (C1: `compare(NaN,NaN)=0`, `equal=false`) и NULL-fast-path (C9: NULL-группа никогда не буферизуется; INNER — O(1)-skip хвостов, LEFT — стриминг с padding) | risks/01 §1.1, risks/06 |
| A10 | MVP «forced» (PREFER/FORCE), INNER/LEFT; спилл групп и RIGHT/FULL — stage 5; CBO — stage 6 | план 04 §4.2-4.4 |

## 3. Карта этапов

| Stage | Название | Фаза плана 04 | Зависимости | Ключевые риски | PR-граница |
|---|---|---|---|---|---|
| [0](stages/stage-0-iceberg-sortedness.md) | Iceberg sortedness (enabler) | 0 | — | C5, C7, C8 | отдельный PR, самостоятельно ценен |
| [1](stages/stage-1-plan-model.md) | План-модель: `JoinNode.algorithm` | 1 (PR1) | — | I13, I9, I10 | PR1 |
| [2](stages/stage-2-merge-join-operator.md) | Оператор merge join | 1 (PR2) | 1 (частично параллелен) | C1, C9, C3, I5, I8, I12 | PR2 |
| [3](stages/stage-3-planner-rule-and-guards.md) | Правило + eligibility + гварды | 1 (PR3) | 1 | C2, C4, C6, I1, I3, I14 | PR3 |
| [4](stages/stage-4-exchanges-mvp-complete.md) | MERGE-ветка обменов + e2e (MVP done) | 1 (PR4) | 2, 3 | P11, I7, I2 | PR4 |
| [5](stages/stage-5-spill-right-full-multidriver.md) | Спилл групп + RIGHT/FULL + multi-driver | 2 | 4 | I4, I5, I6, I11, P1-P4, P8 | PR5-7 |
| [6](stages/stage-6-partitioned-merge-cbo.md) | Partitioned merging + CBO | 3 | 4 (статы — из 5) | P5, P6, P7, I7 | PR8-10 |
| [7](stages/stage-7-full-integration.md) | Полная интеграция (по спросу) | 4 | 6 | I1, P9, P12 | отдельные мини-планы |

Параллелизация: **stage 0 и stage 1 стартуют одновременно** (независимы); stage 2 можно начинать
параллельно со stage 1 (юнит-тесты кормят оператор отсортированными страницами напрямую);
perf-трек JMH (07 §7.6) стартует до stage 6.

## 4. Сквозные инварианты (действуют на каждом этапе)

1. **OFF = ноль изменений**: `prefer_merge_join` default OFF; plan-compare CI — планы байт-в-байт.
2. **Oracle-тестирование против hash join** на каждом этапе (fuzz ≥ 10⁶ строк: NaN/NULL/дубли/
   zipf/пустые стороны; сравнение мультимножествами — §1.8).
3. **Корректность не торгуется**: NaN/NULL-guards в merge-цикле с первого коммита оператора.
4. Repo-конвенции (AGENTS.md): после Java-правок `mvnd airstyle:format` (одиночный файл —
   `mvnd -pl <module> airstyle:format -Dincludes=**/FileName.java`); commit-сообщения —
   по политике check-commit-messages; PR-описание — по `.github/pull_request_template.md`;
   session+config в паре; release notes.
5. Каждый этап несёт свой kill-switch и обратимый откат (см. stages).

## 5. Опорные точки интеграции (единые для всех этапов)

| Точка | Якорь (верифицирован) |
|---|---|
| Выбор оператора | `LocalExecutionPlanner.visitJoin:2539`; все join-типы идут через `createLookupJoin` (:2558, сам метод :2879) — MERGE-ветка перед ним |
| Вставка правила | `PlanOptimizers:886` (батч `DetermineJoinDistributionType`, до `AddExchanges`) |
| MERGE-ветка обменов | `AddExchanges.visitJoin:945`; LOCAL merging — паттерн `AddLocalExchanges.visitSort` → `ExchangeNode.mergingExchange(LOCAL, orderingScheme)` (GATHER + FIXED_PASSTHROUGH, ExchangeNode:220-231); потребитель — `LocalMergeSourceOperatorFactory` (`operator/exchange/`, :38) |
| Гвард пост-AddLocalExchanges | `UseNonPartitionedJoinLookupSource` (PlanOptimizers:1004/1012) |
| Iceberg sortedness | `IcebergMetadata.getTableProperties:1003` + `// todo support sorting properties` (~:1085); `getSupportedSortFields:1740`, `table.sortOrder():1725`, `TrinoSortField` |
| Config/Session | `OptimizerConfig` (`optimizer.prefer-merge-join`, прецедент `prefer-partial-aggregation`:76/:517); `SystemSessionProperties` (`prefer_merge_join`, прецедент :131) |
| Статистика/наблюдаемость | `JoinOperatorType` (класс со static-фабриками → `mergeJoin(...)`), `JoinStatisticsCounter`, `JoinOperatorInfo` |
| Тест-инфраструктура | `TestHashJoinOperator` (SMALL_MEMORY_POOL_BYTES:127), `TestDetermineJoinDistributionType`, `TestPatternRecognitionNodeSerialization`, `sql/query/` (QueryAssertions), `testing/trino-tests` (BaseCostBasedPlanTest), `testing/trino-product-tests` |

## 6. Готовность и верификация этого плана

- Definition of Ready — план 04 §4.9 (все микро-решения сняты: канон Order, нейминг, property,
  guard-набор).
- Каждый stage-файл содержит секцию «Согласованность» — кросс-проверку против корпусов
  `ai-docs/smj-research*` и `ai-docs/01-12`; автоматический чек: ссылки, секции, ID рисков,
  якоря кода (скрипт согласованности, прогоняется при каждом изменении stage-файлов).
- Definition of Done проекта — план 04 §4.8.

## 7. Индекс этапов

- [stage-0-iceberg-sortedness.md](stages/stage-0-iceberg-sortedness.md) — Iceberg сообщает SortingProperty (консервативно, file-level sortOrderId, identity-префикс)
- [stage-1-plan-model.md](stages/stage-1-plan-model.md) — `JoinNode.algorithm` + Jackson + инварианты + golden-тесты
- [stage-2-merge-join-operator.md](stages/stage-2-merge-join-operator.md) — пакет `join.merge`: bridge, конечный автомат, NaN/NULL-guards, MatchBuffer
- [stage-3-planner-rule-and-guards.md](stages/stage-3-planner-rule-and-guards.md) — `DetermineJoinAlgorithm`, eligibility (15 §15.2), `IS NOT NULL` для INNER, DF INNER-only, гварды
- [stage-4-exchanges-mvp-complete.md](stages/stage-4-exchanges-mvp-complete.md) — `AddExchanges` MERGE-ветка, LOCAL merging под сторонами, EXPLAIN, e2e → MVP
- [stage-5-spill-right-full-multidriver.md](stages/stage-5-spill-right-full-multidriver.md) — SpillingMatchBuffer, RIGHT/FULL zero-retention, multi-driver, каскад-merge
- [stage-6-partitioned-merge-cbo.md](stages/stage-6-partitioned-merge-cbo.md) — partitioned merging exchange, cost-модель, COST_BASED, ETL-пресет
- [stage-7-full-integration.md](stages/stage-7-full-integration.md) — ReorderJoins, SemiJoin, FTE spooling, range-split-skip (P12) и др.
