# SMJ Dev TODO — чек-лист реализации Sorted Merge Join

Живой чек-лист. Источник: [smj-plan-dev/README.md](smj-plan-dev/README.md) + stages/.
Отметка: [x] — сделано (коммит), [~] — в работе, [ ] — впереди.
Каждый пункт завершается: `./mvnd airstyle:format` (изменённые файлы) + компиляция + тесты + коммит.

## Stage 1 — План-модель: `JoinNode.algorithm` (PR1, фаза 1)

- [x] 1.1 `JoinNode`: внутренний `public enum JoinAlgorithm { MERGE }` (прецедент `DistributionType`)
- [x] 1.2 поле `Optional<JoinAlgorithm> algorithm` + параметр в **@JsonCreator**-конструкторе
      (без дефолт-перегрузок; приватный полный конструктор — делегирует) + геттер + `withAlgorithm()`
- [x] 1.3 инвариант: `MERGE ⇒ distributionType=PARTITIONED ∧ type ∈ {INNER,LEFT} ∧ criteria непусты`
      (+ семантика flipChildren: INNER сохраняет маркер, LEFT→RIGHT сбрасывает)
- [x] 1.4 проброс поля во всех местах `new JoinNode(` — 60 вставок в 42 файлах (main+test,
      скрипт по 11-му аргументу; lib/trino-matching — другой класс, не тронут)
- [x] 1.5 replaceChildren/with* пробрасывают поле
- [x] 1.6 JSON: `@JsonProperty("algorithm")` на геттере (рефлексия-тест); полный round-trip —
      e2e Stage 4
- [x] 1.7 тесты: `TestJoinNodeAlgorithm` — 9/9 зелёные (инварианты, flip, копии)
- [x] 1.8 компиляция trino-main (main+test) зелёная
- [x] 1.9 airstyle/license формат; коммит

## Stage 2 — Оператор `io.trino.operator.join.merge` (PR2, фаза 1)

- [ ] 2.1 `MergeJoinBridge implements JoinBridge` (whenBuildFinishes/destroy; outer-итератор —
      заглушка до stage 5)
- [ ] 2.2 `MergeJoinBufferOperator(Factory)` — build-sink: копит pages (вход отсортирован),
      публикует мост через `JoinBridgeManager<MergeJoinBridge>`
- [ ] 2.3 `MergeJoinOperator(Factory)` — probe: `WorkProcessorOperator` + `WorkProcessorOperatorAdapter.Factory`
- [ ] 2.4 merge-автомат (05 §5.4): advanceL/R, group match (compare==0), drain; **NaN-guard**
      (C1: NaN-ключи не матчатся); **NULL-fast-path** (C9: NULL-группа не буферизуется;
      INNER — O(1)-skip хвостов; LEFT — стриминг с padding)
- [ ] 2.5 MatchBuffer (05 §5.6): буфер текущей build-группы; addresses+pages для residual-фильтра
      (`JoinFilterFunctionFactory` контракт, SyntheticAddress)
- [ ] 2.6 residual filter (LEFT: all-filtered → padded-вывод) + outputSingleMatch
      (реюз вычисления maySkipOutputDuplicates — LEP:2911)
- [ ] 2.7 компаратор ключей: canonical ASC NULLS_LAST (OrderingTranslator-совместимый);
      serialization-free по каналам
- [ ] 2.8 статистика: `JoinOperatorType.mergeJoin(...)` (фабрика) + `JoinStatisticsCounter` +
      operatorType-строка WebUI
- [ ] 2.9 `TestMergeJoinOperator` (по образцу TestHashJoinOperator): INNER/LEFT × {дубли, NULL
      0..100%, NaN, пустые, RLE/dictionary, multi-page, residual вкл/отсекающий, outputSingleMatch};
      oracle против hash-эквивалента; NULL-хвост при SMALL_MEMORY_POOL_BYTES — память плоская
- [ ] 2.10 finish() идемпотентен; close() не течёт
- [ ] 2.11 airstyle + коммит «Add merge join operator»

## Stage 3 — Правило + eligibility + гварды (PR3, фаза 1)

- [ ] 3.1 session `prefer_merge_join` (OFF/PREFER/FORCE; MVP) в SystemSessionProperties
      + config `optimizer.prefer-merge-join` в OptimizerConfig (default OFF)
- [ ] 3.2 `iterative/rule/DetermineJoinAlgorithm`: MUST-гейты (INNER/LEFT; equi непусты;
      isOrderable все ключи; без DOUBLE/REAL; distributionType=PARTITIONED; retryPolicy==NONE;
      distributed_sort; session != OFF) + W-триггеры (W1 colocated∧sorted — безусловен; W2 Bp>α·U;
      W3 big-big; W4 одна сторона sorted ∧ Bp>β·U) — по 15 §15.6 (MVP: PREFER упрощённо =
      любые MUST + forced-режим для тестов)
- [ ] 3.3 INNER → добавить `k IS NOT NULL` на обе стороны (C9; до shuffle)
- [ ] 3.4 гварды: `UseNonPartitionedJoinLookupSource` skip при MERGE (I14);
      adaptive-правила skip при MERGE
- [ ] 3.5 DF-гейтинг: non-INNER — не собираем DF (RemoveUnsupportedDynamicFilters подчистит)
- [ ] 3.6 `TestDetermineJoinAlgorithm`: MUST/W-матрица, IS-NOT-NULL-инъекция, OFF=0 диффов
- [ ] 3.7 airstyle + коммит «Add DetermineJoinAlgorithm rule and prefer_merge_join property»

## Stage 4 — Обмены + LEP-ветка + e2e: MVP завершён (PR4, фаза 1)

- [ ] 4.1 `AddExchanges.visitJoin` MERGE-ветка: REPARTITION по equi-ключам обеих сторон +
      SortNode (canonical) внутри join-стадии (свойства выводим напрямую — без повторного plan)
- [ ] 4.2 `AddLocalExchanges`: SMJ-стороны → частичная сортировка (fixedParallelism) +
      LOCAL merging exchange (паттерн visitSort) — скан остаётся параллельным (P11)
- [ ] 4.3 `LocalExecutionPlanner.visitJoin`/`createMergeJoin`: сборка build-пайплайна
      (MergeJoinBufferOperator + DF-обёртка для INNER) и probe-пайплайна (MergeJoinOperator)
- [ ] 4.4 `PropertyDerivations.visitJoin`: вывод сортированности выхода (SMJ упорядочен)
- [ ] 4.5 EXPLAIN/PlanPrinter: algorithm + co-location (только при MERGE)
- [ ] 4.6 e2e (QueryAssertions): forced SMJ INNER/LEFT == hash результаты; ревокация;
      stage-count == hash-плану (I7)
- [ ] 4.7 acceptance фазы 1 (орakl-фаззинг ≥10⁶, OFF байт-в-байт, TPC ≤2×) — прогон доступного
      подмножества в CI-режиме
- [ ] 4.8 airstyle + коммит «Wire merge join into planner and local execution (MVP)»

## Stage 0 — Iceberg sortedness (enabler, параллельно; для W1/скидок)

- [ ] 0.1 `IcebergMetadata.getTableProperties`: SortingProperty при {все файлы снапшота с
      file-level sortOrderId == текущему; поля порядка identity; валидный префикс}
- [ ] 0.2 kill-switch `iceberg.report-sortedness` (session, default true)
- [ ] 0.3 тесты: fuzz sortedness vs манифесты; потребители (visitTopN PARTIAL→Limit,
      visitWindow pre-sorted) на Iceberg-таблицах
- [ ] 0.4 airstyle + коммит «Report Iceberg sortedness via table properties»

## Stage 5-7 — спилл групп, RIGHT/FULL, multi-driver, CBO, интеграция (после MVP)

- [ ] Stage 5 (фаза 2): SpillingMatchBuffer; RIGHT/FULL zero-retention; multi-driver; каскад ≤64;
      async tail-spill; spillThroughput
- [ ] Stage 6 (фаза 3): partitioned merging exchange; cost+COST_BASED (α/β/γ/δ); ETL-пресет
- [ ] Stage 7 (фаза 4): FTE spooling; ReorderJoins; SemiJoin; range-partitioning; MergePath; P12

## Сквозное

- [ ] S1 dev-цикл: JDK установлен, mvnw работает, инкрементальные компиляции trino-main/iceberg
- [ ] S2 после каждого этапа: sync заметок в ai-docs (stage-файл: пометка «реализовано» +
      фактические якоря) и dev_todo_list
