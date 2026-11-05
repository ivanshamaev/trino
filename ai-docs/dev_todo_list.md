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
- [x] 3.3 INNER → `k IS NOT NULL` — ✅ реализовано (38691c6a14a): FilterNode(IS NOT NULL)
      на обеих сторонах в DetermineJoinAlgorithm (C9: NULL-хвост отрезается до shuffle)
- [x] 3.4 гварды: `UseNonPartitionedJoinLookupSource` skip при algorithm present (I14);
      DDT-паттерн isEmpty() — same-batch безопасен (аудит)
- [x] 3.5 DF-сбор для INNER — ✅ реализовано (99353263188): DynamicFilterSourceOperator
      над build-sink в createMergeJoin (INNER-only, как hash join); DF-гейт из правила убран
- [x] 3.6 `TestDetermineJoinAlgorithm` — 8/8 (OFF/INNER/LEFT/RIGHT/FULL/REPLICATED/DOUBLE-key/
      non-equi/уже-установлен)
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
- [x] 4.4 PropertyDerivations.visitJoin — вывод сортированности выхода — ✅
- [x] 4.5 EXPLAIN/PlanPrinter algorithm — ✅ (`Algorithm: MERGE` в EXPLAIN)
- [x] 4.6 e2e `TestMergeJoin` (QueryAssertions): **7/7 зелёные** — INNER/LEFT/RIGHT/FULL/VARCHAR/
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

- [x] Stage 5 (частично): **RIGHT/FULL поддержаны** (5ea9a3a58e7): zero-retention drain
      (buildRowMatched[] + drainPage); null-padding O(1); операторные тесты FULL/RIGHT
      (вкл. NULL-ключи и residual filter) зелёные
- [x] Stage 5 (инфраструктура): **spill-threshold + memory tracking** в
      MergeJoinBufferOperator (totalBytesReceived + spillThresholdBytes + factory-параметр);
      полный SpillingMatchBuffer (спилл-раны + ре-итерация групп) — Phase 2 follow-up;
      multi-driver — требует redesign моста (Phase 2); каскад ≤64 / async tail-spill /
      spillThroughput — Phase 2 оптимизации. Блокером НЕ являются: оператор уже обеспечивает
      bounded memory через zero-retention drain + canonical sort (revocable память сортировок).
- [x] Stage 6 (инфраструктура): **COST_BASED режим** (f39e649cd54) + **ExchangeNode
      invariant relaxation** (515dded0a3e) — REPARTITION+ordering разрешён, фабрика
      `mergingPartitionedExchange` создана; полное подключение в AddExchanges MERGE-ветку
      (partial sort на скан-стадиях + partitioned merging exchange вместо SortNode в join-
      стадии) — Phase 3 follow-up при подключении W1-скидок (Stage 0 sortedness).
- [ ] Stage 7 (фаза 4, по спросу): FTE spooling; ReorderJoins; SemiJoin; range-partitioning;
      MergePath; P12 — отдельные мини-планы по мере спроса (план 04 §4.5)

## Известные проблемы (сессия 2026-11-04/05, после MVP+polish)

- **KP-1 (главная)**: ✅ **ЗАКРЫТА** (2026-11-05): корневая причина — `AddLocalExchanges.visitJoin`
  планировал merge-join пайплайны с default parallelism → N дублированных probe-пайплайнов делили
  один мост, ROUND_ROBIN резал отсортированный поток → livelock + потерянные строки. Фикс:
  MERGE-ветка в visitJoin планирует обе стороны `singleStream()` (befa3f855).
  Дополнительно закрыта утечка (lifecycle probe-фабрики). Все e2e 7/7 зелёные.
- **KP-2**: I13-пропагация маркера — найдены и закрыты РЕАЛЬНЫЕ дропперы (живая иллюстрация риска):
  `UnaliasSymbolReferences:1133` (главный, между правилом и AddExchanges), `PredicatePushDown` ×3 сайта
  (второй запуск после AddExchanges), `AddExchanges.buildJoin:1111`, `PruneJoinColumns`,
  `PushInequalityFilterExpressionBelowJoinRuleSet`, `RemoveUnsupportedDynamicFilters`,
  `PushPartialAggregationThroughJoin`. Все теперь пробрасывают `getAlgorithm()`
  (LEFT→RIGHT-конверсия в PredicatePushDown осознанно сбрасывает — RIGHT не поддержан).
- **KP-3**: ✅ **ЗАКРЫТА**: все e2e TestMergeJoin 7/7 зелёные (INNER/LEFT/VARCHAR/residual/
  many-to-many/EXPLAIN/OFF-эквивалентность).

## Выполнено в сессии доработок (помечено выше)

- [x] 3.6 `TestDetermineJoinAlgorithm` — 8/8 (OFF/INNER/LEFT/RIGHT/FULL/REPLICATED/DOUBLE-key/
      non-equi/уже-установлен; кастомный Matcher с anyTree-неоднозначностью решён через values-паттерны)
- [x] 4.4 `PropertyDerivations.visitJoin`: MERGE INNER/LEFT → local sorted (канон ASC NULLS_LAST)
      при left-ключах в output; unordered(false)
- [x] 4.5 `PlanPrinter`: `Algorithm: MERGE` в EXPLAIN (проверено e2e)
- [x] KP-2: полная пропагация маркера (8 сайтов) — merge-план строится до конца

## Сквозное

- [x] S1 dev-цикл: JDK установлен, mvnw работает, инкрементальные компиляции trino-main/iceberg
- [x] S2 после каждого этапа: sync заметок в ai-docs (stage-файл: пометка «реализовано» +
      фактические якоря) и dev_todo_list
