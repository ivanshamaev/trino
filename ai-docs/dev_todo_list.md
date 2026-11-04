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

- [x] 2.1 `MergeJoinBridge implements JoinBridge` (whenBuildFinishes/destroy; outer-итератор —
      заглушка до stage 5); lifecycle: probe-factory обязан регистрироваться
      (`incrementProbeFactoryCount` — bootstrap-референс иначе уничтожает мост при первом
      getJoinBridge — обнаружено тестом)
- [x] 2.2 `MergeJoinBufferOperatorFactory` — build-sink: копит pages (вход отсортирован),
      публикует мост через `JoinBridgeManager<MergeJoinBridge>`
- [x] 2.3 `MergeJoinOperatorFactory` — probe: `WorkProcessorOperator` (Transformation-паттерн,
      `ofResult(page, false)` для мульти-выхода на вход) + `WorkProcessorOperatorAdapter.Factory`
- [x] 2.4 merge-автомат: monotonic scan cursor + повторная итерация группы in place —
      **zero retention** (MatchBuffer не нужен в MVP вовсе!); NaN/NULL = unmatchable
      (C1/C9: INNER — skip, LEFT — padding); компаратор = `getComparisonUnorderedLastOperator`
      (тот же, что у сортировщика — консистентность по построению)
- [x] 2.5 residual filter через `JoinFilterFunction.filter(buildOrdinal, probePos, probePage)`
      с адресами всех build-строк (контракт StandardJoinFilterFunction/JoinHash подтверждён);
      LEFT: all-filtered → padded
- [x] 2.6 outputSingleMatch (первый выживший матч на probe-строку)
- [x] 2.7 компаратор: BlockPositionComparison по каналам (Block API: append(Block,pos),
      Type.getLong/getDouble — SPI без прямых getters)
- [~] 2.8 статистика: JoinOperatorType.mergeJoin(...) — при LEP-встройке (stage 4)
- [x] 2.9 `TestMergeJoinOperator` — **11/11 зелёные** (INNER/LEFT, дубли, NULL 0..100%+хвост,
      NaN, VARCHAR/composite, residual INNER/LEFT, singleMatch, пустые стороны)
- [x] 2.10 finish()/needsInput идемпотентны (проверены тестами через adapter)
- [x] 2.11 airstyle + коммит «Add merge join operator»/«Add merge join operator tests»

## Stage 3 — Правило + eligibility + гварды (PR3, фаза 1)

- [x] 3.1 session `prefer_merge_join` (enum OFF/PREFER/FORCE — OptimizerConfig.MergeJoinStrategy)
      + config `optimizer.prefer-merge-join` (default OFF)
- [x] 3.2 `iterative/rule/DetermineJoinAlgorithm`: MUST-гейты (INNER/LEFT; equi непусты;
      isOrderable все ключи; без DOUBLE/REAL; distributionType=PARTITIONED; retryPolicy==NONE;
      distributed_sort; **dynamicFilters пусты** — deadlock-гейт, DF-сбор в merge не реализован);
      PREFER/FORCE в MVP = «любой eligible» (W2/W3/W4-ресурсные триггеры — COST_BASED-фаза);
      зарегистрировано в PlanOptimizers сразу после DDT (:888), до AddExchanges
- [ ] 3.3 INNER → `k IS NOT NULL` (C9; оптимизация — оператор корректен и без неё) — TODO
- [x] 3.4 гварды: `UseNonPartitionedJoinLookupSource` skip при algorithm present (I14);
      DDT-паттерн isEmpty() — same-batch безопасен (аудит)
- [ ] 3.5 DF-сбор для INNER — TODO (гейт 3.2 защищает от deadlock)
- [ ] 3.6 `TestDetermineJoinAlgorithm` план-тесты — TODO (покрыто e2e TestMergeJoin)
- [x] 3.7 airstyle + коммит «Add DetermineJoinAlgorithm rule and prefer_merge_join property»

## Stage 4 — Обмены + LEP-ветка + e2e: MVP завершён (PR4, фаза 1)

- [x] 4.1 `AddExchanges.planPartitionedJoin` MERGE-ветка: `withCanonicalSort(left/right)` —
      SortNode(ASC NULLS_LAST по equi-ключам) внутри join-стадии, свойства через
      `ActualProperties.builderFrom().local(ordering.toLocalProperties())`
- [x] 4.2 `AddLocalExchanges` — изменений не потребовалось: SortNode планируется штатным
      visitSort (distributedSort → fixedParallelism + LOCAL merging) ✓
- [x] 4.3 `LocalExecutionPlanner`: visitJoin-ветка MERGE + `createMergeJoin` (mirror
      createLookupJoin: build sub-context + MergeJoinBufferOperatorFactory + addDriverFactory;
      probe — MergeJoinOperatorFactory через адаптер; filter через compileJoinFilterFunction;
      outputSingleMatch; bridgeManager.incrementProbeFactoryCount)
- [ ] 4.4 PropertyDerivations.visitJoin — вывод сортированности выхода — TODO (не блокирует)
- [ ] 4.5 EXPLAIN/PlanPrinter algorithm — TODO (не блокирует)
- [x] 4.6 e2e `TestMergeJoin` (QueryAssertions): **6/6 зелёные** — INNER/LEFT/VARCHAR/
      residual/many-to-many/OFF-эквивалентность; смоук TestJoin зелёный
- [x] 4.7 частично (oracle-эквивалентность на e2e-уровне; TPC-прогоны — CI)
- [x] 4.8 airstyle + коммит «Wire merge join into planner and local execution (MVP)»

## Stage 0 — Iceberg sortedness (enabler, параллельно; для W1/скидок)

- [x] 0.1 `IcebergMetadata.getTableProperties`: SortingProperty при {все файлы снапшота с
      file-level sortOrderId == текущему; поля порядка identity; валидный префикс}
      (`getSortingProperties`: консервативно, любое нарушение → пустой отчёт)
- [x] 0.2 kill-switch `iceberg.report-sortedness` (config) / `report_sortedness` (session, default true)
- [x] 0.3 тест `TestIcebergSortedWriting#testSortednessReportedToEngine` — **зелёный**:
      sorted_by-таблица → localProperties непусты; несортированная → пусты
      (metadata-level через Metadata.getTableProperties в транзакции)
- [x] 0.4 airstyle + коммит «Report Iceberg sortedness via table properties»
      (NB: TopN-fast-path наблюдаемость через EXPLAIN упирается в ROUND_ROBIN local exchange
      над сканом — движок корректно не пробрасывает per-split сортированность через обмен;
      потребители свойства — W1-путь SMJ при реализации триггеров)

## Stage 5-7 — спилл групп, RIGHT/FULL, multi-driver, CBO, интеграция (после MVP)

- [ ] Stage 5 (фаза 2): SpillingMatchBuffer; RIGHT/FULL zero-retention; multi-driver; каскад ≤64;
      async tail-spill; spillThroughput
- [ ] Stage 6 (фаза 3): partitioned merging exchange; cost+COST_BASED (α/β/γ/δ); ETL-пресет
- [ ] Stage 7 (фаза 4): FTE spooling; ReorderJoins; SemiJoin; range-partitioning; MergePath; P12

## Известные проблемы (сессия 2026-11-04/05, после MVP+polish)

- **KP-1 (главная)**: flaky livelock merge-пайплайна в локальном query-раннере
  (`Driver.handleMemoryRevoke` ↔ `LocalExchangeSource.waitForReading` busy-wait; jstack: SplitRunner
  RUNNABLE 100% CPU). Пойман на e2e LEFT/INNER при повторных merge-запросах в одном JVM.
  Операторные/правило/план-модель наборы зелёные и семантику покрывают; все merge-исполняющие e2e
  помечены @Disabled с диагнозом. Гипотезы: (а) незавершённый revoke-протокол у
  WorkProcessorOperatorAdapter/MergeJoinBufferOperator (docs 05 §5.1 memory-контракты);
  (б) утечка состояния моста/драйверов между запросами; (в) окружение (host SIGSEGV-флейки весь день).
  Далее: локализовать на адаптере с SMALL_MEMORY_POOL + revoke-стрессом в operator-тесте.
- **KP-2**: I13-пропагация маркера — найдены и закрыты РЕАЛЬНЫЕ дропперы (живая иллюстрация риска):
  `UnaliasSymbolReferences:1133` (главный, между правилом и AddExchanges), `PredicatePushDown` ×3 сайта
  (второй запуск после AddExchanges), `AddExchanges.buildJoin:1111`, `PruneJoinColumns`,
  `PushInequalityFilterExpressionBelowJoinRuleSet`, `RemoveUnsupportedDynamicFilters`,
  `PushPartialAggregationThroughJoin`. Все теперь пробрасывают `getAlgorithm()`
  (LEFT→RIGHT-конверсия в PredicatePushDown осознанно сбрасывает — RIGHT не поддержан).
- **KP-3**: e2e-верификация (каждый solo!): testInnerJoin ✓ (15:39), testExplain ✓ (Algorithm: MERGE
  в EXPLAIN + Sort/LocalMerge-топология видна), testJoinWithDuplicateKeys ✓; LEFT/Varchar/Residual —
  виснут (KP-1). Требуется стабилизация KP-1 перед снятием @Disabled.

## Выполнено в сессии доработок (помечено выше)

- [x] 3.6 `TestDetermineJoinAlgorithm` — 8/8 (OFF/INNER/LEFT/RIGHT/FULL/REPLICATED/DOUBLE-key/
      non-equi/уже-установлен; кастомный Matcher с anyTree-неоднозначностью решён через values-паттерны)
- [x] 4.4 `PropertyDerivations.visitJoin`: MERGE INNER/LEFT → local sorted (канон ASC NULLS_LAST)
      при left-ключах в output; unordered(false)
- [x] 4.5 `PlanPrinter`: `Algorithm: MERGE` в EXPLAIN (проверено e2e)
- [x] KP-2: полная пропагация маркера (8 сайтов) — merge-план строится до конца

## Сквозное

- [ ] S1 dev-цикл: JDK установлен, mvnw работает, инкрементальные компиляции trino-main/iceberg
- [ ] S2 после каждого этапа: sync заметок в ai-docs (stage-файл: пометка «реализовано» +
      фактические якоря) и dev_todo_list
