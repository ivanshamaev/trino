# 04. План реализации SMJ с учётом рисков (risk-gated plan)

План уточняет roadmap из [10-synthesis](../10-synthesis-and-recommendations.md) риск-гейтами:
каждая фаза содержит **список закрываемых рисков**, **acceptance-критерии, выведенные из рисков**,
и **условия отката**. Идентификаторы рисков — из [README](README.md) (C* — корректность,
I* — интеграция, P* — производительность).

## 4.0. Сквозные принципы (из рисков)

1. **Инвариант равенства/порядка — приоритет №1**: NaN-guard и NULL-guard встроены в merge-цикл
   с первого коммита (C1, C3), даже если MVP исключает DOUBLE-ключи.
2. **Ноль регресса при OFF**: план байт-в-байт (plan-compare CI) на каждой фазе (I10, C7).
3. **Oracle-тестирование против hash join** — обязательная часть каждой фазы (C1-C8).
4. **Нефорк оператора**: один merge-оператор, spill-ветвление внутри. Урок Trino #12618
   («Hard-fork join operator and remove spilling from it», release 389; форк живёт в репо как
   пакеты `operator/join/{spilling,nonspilling}`, session `force_spilling_join`; фактический
   выигрыш форка ~1-1.5%): форк допустим только при стабильном выигрыше по профилировщику —
   новый оператор с форка не начинаем.

## 4.1. Фаза 0 — Iceberg sortedness (enabler, независимо ценно)

**Состав**: `IcebergMetadata.getTableProperties` → SortingProperty (только если все файлы снапшота
с текущим `sortOrderId`, только identity-поля, single-spec); гарантия «сплит = отсортированный
диапазон» (порядок чтения сплитов одного файла).

**Закрывает риски**: C5 (пер-файловая сортировка осознана и закодирована), C7 (консервативный
критерий), частично P7 (документация).

**Acceptance (из рисков)**:
- fuzz: «отчёт sortedness vs фактический порядок файлов» на матрице {file-level `sortOrderId` ∈ {0, current, mixed}, identity/truncate/bucket-поля, deletes on/off, многосплитовые файлы} — 0 ложных отчетов (уточнение C8: смотрим **sortOrderId каждого файла в манифесте**, не табличный; сообщаем валидный identity-**префикс** порядка — сортировка по большему множеству имплицирует префикс);
- планер начинает использовать sortedness проверяемыми способами: (а) `AddExchanges.visitTopN` PARTIAL → Limit для pre-sorted входа; (б) `PropertyDerivations.visitWindow` pre-sorted окна без пересортировки; (в) опциональная deliverable — новое простое iterative-правило элиминации `SortNode` над сканом с удовлетворяющей `SortingProperty` (~50 строк; правило `RemoveRedundantSorts` в Trino **не существует** — проверено grep'ом, пишем с нуля);
- mixed-версионный product-test не требуется (SPI не меняется).

**Откат**: флаг `iceberg.report-sortedness=false` (kill-switch).

## 4.2. Фаза 1 — MVP (forced SMJ: INNER/LEFT, bucket-execution fast-path + SortNode)

**Состав** (PR-нарезка из 09/§9.7, с правками верификации):
1. `JoinNode.algorithm` (Optional) + сериализация + инварианты; **поле — в единственном
   публичном (@JsonCreator) конструкторе, без дефолт-перегрузок** (прецеденты: `spillable`,
   `maySkipOutputDuplicates`; приватный полный конструктор JoinNode:105 используется только
   делегированием — все 45 внешних мест идут через публичный) — 45 мест `new JoinNode(`
   обязаны пробросить явно, компилятор ловит каждое
   (риск I13); golden-тест на трансформирующие правила (UnaliasSymbolReferences/PredicatePushDown/
   `flipChildren` — JoinNode:170); golden-JSON тест по прецеденту
   `TestPatternRecognitionNodeSerialization` (planner/plan тесты);
2. оператор `io.trino.operator.join.merge` (bridge-модель: `MergeJoinBufferOperator` sink +
   `MergeJoinOperator`; NaN/NULL-guards; **NULL-группа никогда не буферизуется — fast-path:
   INNER O(1)-skip NULL-хвостов обеих сторон, LEFT — стриминг с padding** — риск C9, risks/06);
   residual filter через `JoinFilterFunction` с SyntheticAddress);
   сбор DF — обёрткой build-пайплайна `createDynamicFilterSourceOperatorFactory` над sink, как в
   `createLookupJoin` (LocalExecutionPlanner:2959; сам `DynamicFilterSourceOperator` — отдельный
   оператор над sink, интерфейс build-оператору не нужен);
   статистика: `JoinOperatorType` — класс со static-фабриками → добавить фабрику `mergeJoin(...)`
   + operatorType-строку для WebUI (не enum — поправка верификации D3);
3. session/config `prefer_merge_join` + правило `DetermineJoinAlgorithm` (PREFER/FORCE,
   eligibility: INNER/LEFT, equi-критерии непусты, **все ключи isOrderable, без DOUBLE/REAL** —
   до guards-тестов, DF-совместимость, `retryPolicy==NONE`, distribution=PARTITIONED,
   **`distributed_sort=true`** — default (OptimizerConfig:64); при `false` visitSort загоняет
   весь входной пайплайн в singleStream — возврат P11);
   **жёсткие рамки выбора — [15, §15.2-15.6](../15-smj-selection-policy.md)** (MUST-гейты,
   W-триггеры/N-запреты, псевдокод, пороги α/β/γ/δ); для INNER правило добавляет `k IS NOT NULL`
   на обе стороны (C9: отсечение NULL-хвоста до shuffle + прунинг Iceberg-файлов по null_count);
   DF — **только INNER** (сбор той же обёрткой над build-sink; для LEFT/FULL probe-фильтрация
   невалидна — I3, risks/08 §8.2);
4. `AddExchanges.visitJoin` MERGE-ветка (SortNode после exchange; co-located ветка с per-bucket
   merge сплитов — fast-path из C5); **под каждой SMJ-стороной: частичная сортировка по сплитам +
   LOCAL merging exchange** (паттерн `AddLocalExchanges.visitSort`: N параллельных сортировщиков →
   `ExchangeNode.mergingExchange(LOCAL, orderingScheme)` — GATHER + FIXED_PASSTHROUGH_DISTRIBUTION,
   ExchangeNode.java:220 — → 1 merge) — иначе singleStream сериализует весь
   probe-pipeline до однопоточного скана (риск P11); `AddLocalExchanges` гварды; гвард в
   `UseNonPartitionedJoinLookupSource` — правило выполняется **после** AddLocalExchanges
   (PlanOptimizers:1004/1012) и перестраивает build-side local exchange по hash-экономике,
   для MERGE исключается (риск I14); гварды adaptive;
5. e2e-тесты.

**Закрывает риски**: C1 (guards или exclusion), C2, C3, C4 (eligibility-матчинг), C6 (документация),
**C9 (NULL fast-path + IS NOT NULL для INNER)**, I1 (gate), I2 (тест phased), I3 (DF-тест,
INNER-only), I8 (адреса фильтра), I9, I10, I12 (fail-during-merge),
**I13 (канонический конструктор + golden-тесты)**, **I14 (гвард в
UseNonPartitionedJoinLookupSource)**, **P11 (LOCAL merging exchange под сторонами)**,
P3-частично (probe без копирования), P7 (аннотации EXPLAIN).

**Acceptance (из рисков)** — «железные» гейты перед фазой 2:
- **Правильность**: oracle-сравнение с hash join на fuzz-корпусе ≥ 10⁶ строк с генерацией:
  NaN/-0.0/+0.0/Infinity (после включения DOUBLE), NULL-ключи (доли 0/1/50/100%), duplicate-ключи
  (zipf, одна гига-группа), пустые стороны, RLE/dictionary-страницы, multi-page группы, residual
  filter (включая фильтр-все-отсекающий), outputSingleMatch вкл/выкл (реальный переключатель — `JoinNode.isMaySkipOutputDuplicates()`
   от правила `OptimizeDuplicateInsensitiveJoins`, вычисление в `LocalExecutionPlanner:2911`),
   все SortOrder-комбинации;
  сравнение — мультимножествами (нестабильность сортировки, §1.8).
- **Инварианты движка**: план при OFF байт-в-байт; `finish()` повторно-идемпотентен; ревокация в
  середине merge (тест с `SMALL_MEMORY_POOL_BYTES` — TestHashJoinOperator:127); отмена запроса
  не течёт spill-файлами (fail-during-merge);
  mixed-version сериализация (infra: `testing/trino-product-tests`).
- **Базовая производительность**: TPC-H/TPC-DS с FORCE — результаты идентичны; время в пределах
  2× от hash на SF-100 (SMJ не обязан выигрывать на малых данных, но не должен деградировать >2×).

**Откат**: session-флаг OFF; операторы не инстанцируются.

## 4.3. Фаза 2 — Спилл групп + RIGHT/FULL + multi-driver + производительность

**Состав**: SpillingMatchBuffer (пороги → block-nested-loop re-iterate); RIGHT/FULL outer-driver
(реюз outer-operator lifecycle из `JoinBridgeManager`: `outerOperatorCreated/Closed` /
`getOuterPositionsFuture()` — готовая механика full-outer hash join); **FULL OUTER — однопроходный
стриминг: null-padding O(1), zero-retention (пик памяти == INNER), drain build-хвоста после
probe, residual `anyMatchSurvived`-флаг, NULL-хвосты — C9-fast-path** (risks/07);
per-partition merge-драйверы (partitionCount=k×taskCount, реестр мостов по instance-index);
перенос каскад-merge (fan-in ≤ 64) в core; асинхронный tail-spill в OrderByOperator (I4);
асинхронизация спиллов внутри задачи (I11); memory-ownership словарных выходов (I6) или
словарный вывод за флагом с телеметрией.

**Закрывает**: I4, I5-контракт re-iterate, I6, I11, P1 (частично), P2, P3, P4, P8
   (runtime-метрика `spillThroughput` + warning при < 500 МБ/с).

**Acceptance**:
- **Масштаб**: синтетика 15B×17B (экстраполяция 1B→10B) без OOM; память bounded: max memory/задачу
  ≤ configured cap при гигантских группах; spill-bytes/строку ≤ 2 (1 write + 1 read) при fusion;
  re-iterate-группа 10M×10M при 64 МБ лимите — bytes-read ≤ L + R×(L/C) (P4-гейт).
- **Изоляция**: 4 конкурентные SMJ-задачи на воркере с малым пулом — нет деградации > 30%
  (короткие раны допустимы, каскад съедает).
- **Правильность**: RIGHT/FULL oracle; фильтр-адресация на multi-page группах.
- **FULL OUTER zero-retention** (risks/07): пик памяти FULL == INNER на тех же данных;
  гига-NULL-хвост + FULL без OOM (C9); probe-хвост 10⁹ без матчей — память плоская.

**Откат**: spill-режим за флагом; RIGHT/FULL fallback на hash (правило не выбирает SMJ для
RIGHT/FULL, если оператор не готов).

## 4.4. Фаза 3 — Partitioned merging exchange + CBO

**Состав**: ослабление инварианта ExchangeNode (REPARTITION+ordering); partialSort на скан-стадиях;
StreamPropertyDerivations; `calculateSortedMergeJoinCostWithoutOutput` — новый static-метод по
образцу `CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput` (сегодня
статически импортируется `DetermineJoinDistributionType:230`) — с memory-компонентой;
режим COST_BASED — правила и пороги из [15, §15.6-15.7](../15-smj-selection-policy.md)
(α=0.7 memory-pressure, β=0.3 small-build, γ=1.0 big-big, δ=10% CPU-допуск; калибровка — perf-трек);
ETL-пресет в документации; runtime-телеметрия algorithm в JoinOperatorInfo.

**Закрывает**: P1 (сортировка распараллелена сплитами), P5 (cost-порог), P6 (пресет+warning),
I7 (тест stage-count).

**Acceptance**:
- stage-count(SMJ-план) == stage-count(hash-план) на эталонных запросах;
- CBO на TPC-DS SF-100: SMJ выбирается только в big-big join'ах; ручные пороги
  (build > 1.5-2× памяти ИЛИ входы отсортированы) — диффы планов reviewed;
- **стаб-профиль**: на 200-400 партициях wall-clock ≤ CPU-total/ядра × 1.5 (нет serialization-бутылочного горлышка).

**Откат**: COST_BASED → PREFER (конфиг), exchange-путь за флагом.

## 4.5. Фаза 4 — Полная интеграция (по спросу)

ReorderJoins SMJ-aware; SemiJoin; FTE ordered spooling (I1-полное закрытие); range-partitioning
по Iceberg min/max (P7-полное); MergePath-параллелизм merge (P9); order-preserving casts (C6-смягчение);
**cursor-driven range-split-skip для S1-GOLD (P12, risks/08 §8.3 — только против DF-бейзлайна;
после REPARTITION порядок по hash(k) — не работает)**.
Каждый пункт — отдельный мини-план с теми же гейтами.

## 4.6. Маппинг «риск → тест/гейт → фаза»

| Риск | Тест/гейт | Фаза |
|---|---|---|
| C1 NaN | fuzz NaN-ключи + unit guards | 1 (exclusion → guards+DOUBLE той же фазой) |
| C2 MAP | eligibility-тест | 1 |
| C3 NULL-порядки | unit компаратора + fuzz NULL | 1 |
| C4 префикс | правило-тесты оба направления | 1 |
| C5 per-file sortedness | фаза-0 fuzz + fast-path merge сплитов | 0/1 |
| C6 cast | документация + правило-тест (missed, не broken) | 1 |
| C7 ложный отчёт | фаза-0 fuzz + kill-switch | 0 |
| I1 FTE | гвард-тест (SMJ не выбирается при TASK) | 1; полное — 4 |
| I2 phased | e2e equivalence phased/all-at-once | 1 |
| I3 DF | product-test DF+SMJ | 1 |
| I4 tail-spill | async-spill тест малый пул | 2 |
| I5 раны одноразовые | контракт SpillingMatchBuffer + тест re-iterate | 2 |
| I6 словари | memory-тест + телеметрия | 2 |
| I7 stage-count | тест stage-count(SMJ) == stage-count(hash) | 1/3 |
| I8 адресация фильтра | MatchBuffer → addresses+pages; unit residual-filter | 1 (multi-page — 2) |
| I9 mixed | product-test смешанный кластер | 1 |
| I10 наблюдаемость | фабрика mergeJoin + EXPLAIN golden (печать только при наличии) | 1 |
| I11 конкуренция в задаче | сериализация спиллов; тест 4 конкурентных SMJ-задач | 2 |
| I12 утечки spill-файлов | fail-during-merge + отмена запроса; LocalSpillManager | 1/2 |
| **I13 45 мест пересборки JoinNode** | golden-тест потери поля через правила; канонический конструктор | 1 |
| **I14 UseNonPartitionedJoinLookupSource** | гвард правила (MERGE исключается) + golden-тест неизменности SMJ-плана | 1 |
| **P11 singleStream сериализация пайплайна** | LOCAL merging exchange под SMJ-сторонами; тест параллелизма скана | 1 |
| **C8 file-level sortOrderId** | фаза-0 fuzz по манифестам | 0 |
| **C9 NULL-хвост** | fast-path компаратора + `IS NOT NULL` (INNER); NULL-fuzz {0..100}% с малым пулом | 1 (INNER/LEFT) / 2 (FULL) |
| P1 параллелизм | стаб-профиль 200-400 партиций | 2/3 |
| P2 merge k | бенчмарк k∈{2..512} | 2 |
| P3 копирование | метрика copied-vs-referenced | 2 |
| P4 re-iterate | bytes-read ≤ L + R·L/C | 2 |
| P5/P6 LLC/пресет | CBO-диффы + warning | 3 |
| P7 ожидания | EXPLAIN-аннотации + доки | 1/3 |
| P8 сетевые spill-тома | метрика `spillThroughput` + warning; документация | 2 |
| P9 однопоточный merge | MergePath-декомпозиция; метрика queue-time | 4 |
| P10 shuffle-порядок | бенчмарк почти-сгруппированных входов (quicksort 3-way) | 2/3 |
| **P12 range-split-skip** | фаза-4 spike vs DF-бейзлайн (только S1-GOLD) | 4 |
| S-N1 co-location same-catalog | документация сценариев (11/14) | постоянно |

## 4.7. Порядок работ и параллелизация

- Фаза 0 и «PR1 план-модель» фазы 1 независимы — стартуют одновременно.
- Perf-трек (JMH из 07/§7.6) стартует до фазы 2 (данные нужны для порогов CBO и radix-решения).
- Каждая фаза — отдельный mergeable PR-серия (нарезка 09/§9.7), ревью-чеклист: airstyle,
  Optional/nullability, session+config пара, release notes, docs.

## 4.8. Критерии общей готовности (definition of done проекта)

1. Все C-риски закрыты тестами (oracle-фуззинг зелёный в CI на каждом PR).
2. 15B×17B-синтетика проходит с bounded memory (фаза 2 acceptance).
3. CBO-режим не регрессирует TPC-DS (фаза 3 acceptance).
4. OFF-режим: ноль diffs планов в CI.
5. Документация: ETL-пресет, сценарии применимости ([14-best-fit-scenarios.md](../14-best-fit-scenarios.md)),
   ограничения (C1/C2/C6/P7).

## 4.9. Definition of Ready для старта реализации MVP

Статус: **готов к старту** (проверено проходами верификации 5–9, risks/05 §5.8-5.12;
блокеров нет).

**Опорные дизайн-документы** (маршрут первого чтения инженера):
- [03, §3.10](../03-smj-integration-points.md) — карта «файл → правка» (plan-модель, правило,
  обмены, гварды, рантайм, Iceberg); §3.3 — рецепт session property; §3.9 — тест-инфраструктура
  (`TestHashJoinOperator` → `TestMergeJoinOperator`, PlanBuilder, BaseCostBasedPlanTest,
  QueryAssertions);
- [05, §5.4](../05-brainstorm-java-developer.md) — конечный автомат merge; §5.6 — MatchBuffer;
  §5.1 — модель оператора: build = push-`Operator` (как `HashBuilderOperator`), probe =
  `WorkProcessorOperator` через `WorkProcessorOperatorAdapter` (паттерн подтверждён:
  `spilling.LookupJoinOperatorFactory` — `implements JoinOperatorFactory,
  WorkProcessorOperatorFactory`, :50; probe-side spill упирается в `// TODO: support spill`
  — WorkProcessorOperator.java:21, поэтому в MVP вся тяжёлая память на build-стороне);
- [09, §9.5-9.7](../09-brainstorm-trino-developer.md) — скелет `createMergeJoin` по образцу
  `createLookupJoin` (LocalExecutionPlanner:2879; выбор оператора — `visitJoin:2539`, единая
  точка: все INNER/LEFT/RIGHT/FULL идут через `createLookupJoin`, :2558);
- [12](../12-sorting-algorithms-landscape.md) — выбор алгоритма сортировки ранов;
- [15](../15-smj-selection-policy.md) — **жёсткие рамки выбора SMJ** (MUST-гейты §15.2,
  W-триггеры/N-запреты §15.4-15.5, псевдокод §15.6, пороги §15.7) — контракт для правила
  `DetermineJoinAlgorithm` и его план-тестов.

**Закреплённые микро-решения** (сняты «например» из ранних документов):
1. Канонический `OrderingScheme` обеих сторон: equi-ключи **ASC, NULLS_LAST** — совпадает с
   SQL-дефолтом Trino (`OrderingTranslator.sortItemToSortOrder`: UNDEFINED → NULLS_LAST,
   OrderingTranslator.java:25-29) → минимум сюрпризов у пользователей и в sort-elimination.
2. Канонический нейминг: пакет `io.trino.operator.join.merge`; классы `MergeJoinOperator`,
   `MergeJoinBufferOperator` (sink), мост `MergeJoinSource` через
   `JoinBridgeManager<MergeJoinBridge>` (консенсус 09/10/план; в 05 были рабочие имена
   `SortedMerge*` — приведено к канону; коллизии с `io.trino.operator.MergeOperator` нет).
   Сам `JoinAlgorithm` — внутренний `public enum` в JoinNode по прецеденту
   `DistributionType {PARTITIONED, REPLICATED}` (JoinNode:52).
3. Session property `prefer_merge_join` (прецедент нейминга: `prefer_partial_aggregation`,
   SystemSessionProperties:131); config-пара в **OptimizerConfig**
   (`optimizer.prefer-merge-join`, по прецеденту `optimizer.prefer-partial-aggregation` —
   OptimizerConfig:76/517), default OFF; полный рецепт — [03, §3.3](../03-smj-integration-points.md).
4. Guard-набор MVP: `retryPolicy==NONE` (I1), adaptive (I1), `AddLocalExchanges` (P11),
   `UseNonPartitionedJoinLookupSource` (I14), `distributed_sort=true` (P11; default —
   OptimizerConfig:64, session `distributed_sort` — SystemSessionProperties:125),
   DF — только INNER (I3/C9: `IS NOT NULL` для INNER, для LEFT/FULL не собираем).

**Остаточные открытые вопросы** (решаются внутри фазы 1, старт не блокируют): точный список
счётчиков статистик оператора; формат строки PlanPrinter/EXPLAIN; пороги SpillingMatchBuffer
(фаза 2); состав fuzz-генератора oracle-корпуса.
