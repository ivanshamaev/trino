# 9. Brainstorm: Trino-коммиттер — план-модель, exchange, нарезка PR

> Роль: коммиттер Trino. Позиция по всем ключевым решениям с точными ссылками на код.

## 9.1. Plan model

**Рекомендация: никакого нового значения `DistributionType` и никакого отдельного узла. Новое поле
`Optional<JoinAlgorithm> algorithm` в `JoinNode` (единственное значение — `MERGE`).**

Почему не `DistributionType.SORTED_MERGE`: enum отвечает «как входы распределены по нодам», на нём
SWITCH-ится семантика в 8 местах (`AddExchanges.visitJoin:954-967`, `DetermineJoinDistributionType:121-182`,
`ReorderJoins`, `CostCalculator*`, `PhasedExecutionSchedule:385-393`, `PlanPrinter:672`). SMJ — не третий
способ распределения, а **локальный алгоритм поверх PARTITIONED**: репартиционирование по equi-ключам
то же, что у hash join. Каждому из восьми свитчей пришлось бы выучить третье значение, хотя меняется
только исполнение внутри stage. Плюс enum сериализуется по имени — при rolling upgrade старый воркер
упадёт на `SORTED_MERGE` на десериализации, а `Optional.empty()`-поле обратно совместимо.

Почему не `MergeJoinNode` (как PrestoDB): отдельный узел дорог — PlanVisitor, AddExchanges,
AddLocalExchanges, PropertyDerivations, StreamPropertyDerivations, PlanFragmenter, PlanPrinter,
PlanSanityChecker/ValidateDependenciesChecker, stats-правила; главное — **SMJ выбирается после
ReorderJoins и DetermineJoinDistributionType, которые работают с JoinNode/MultiJoinNode**. Отдельный
узел либо выпадает из общих правил (PredicatePushDown...), либо дублирует их.

Практические детали:
- Конструктор: новый параметр в `@JsonCreator` (прецеденты роста — `spillable`, `maySkipOutputDuplicates`);
  copy-метод `withAlgorithm` с `validateSymbols=false` (по образцу `withDistributionType`).
  Инвариант: `algorithm.isPresent() ⇒ distributionType == PARTITIONED && !criteria.isEmpty()`.
  REPLICATED vs RIGHT/FULL не трогаем — SMJ всегда PARTITIONED, для FULL/RIGHT легален по построению
  (операторно FULL придёт позже).
- `flipChildren`: алгоритм переживает флип (критерии флипаются, отсортированность сохраняется); решение
  о SMJ принимается после DetermineJoinDistributionType, где флипы зафиксированы.
- `SemiJoinNode`: не нужно (свой узел и оператор; выгоды маргинальны). Откладываем.
- **Dynamic filtering**: концептуально конфликтует (DF требует build-домен до старта probe). MVP:
  правило не матчит join с непустым `getDynamicFilters()` (чистить принудительно — плохой silent
  behavior); в bridge-модели DF на build-sink вообще сохраняется — тогда ограничение снимается.

## 9.2. Список правок для MVP (force-by-session)

**Plan-side:**
1. `plan/JoinNode.java` — enum + поле + геттер + `withAlgorithm` + инварианты.
2. `SystemSessionProperties.java` — `prefer_merge_join` (boolean, false) по образцу DISTRIBUTED_SORT;
   config-дубликат в `OptimizerConfig` (Trino требует для каждого session-свойства).
3. Новое правило `iterative/rule/DetermineJoinAlgorithm.java`: pattern `join()` с
   `distributionType == PARTITIONED`, algorithm пуст, критерии непусты, тип INNER (LEFT бонусом),
   флаг включён → `withAlgorithm(MERGE)`.
4. `PlanOptimizers.java` — правило в тот же IterativeOptimizer, где DetermineJoinDistributionType
   (обязательно до AddExchanges).
5. `AddExchanges.visitJoin`, ветка `planPartitionedJoin` (970-1026): при `algorithm == MERGE` обе
   стороны вместо `partitionedExchange(...)` — `partial SortNode(keys ASC)` + partitioned exchange
   с ordering (см. 9.3). Co-located ветку (985-994) для SMJ в MVP отключаем (всегда явный exchange
   с ordering); co-location вернём в Iceberg-PR.
6. `AddLocalExchanges.visitJoin` (900-931): для merge-join обе стороны `singleStream()` (локальный
   hash-exchange разрежет отсортированный поток); spill-ветку (907-917) пропускаем.
7. `PropertyDerivations.visitJoin` — в MVP не менять (после exchange локальные свойства сносятся;
   дефолт корректен). Бонус на потом: INNER SMJ сохраняет сортировку на выходе.
8. `StreamPropertyDerivations.visitJoin` — не трогаем (наследует probe-свойства как hash).
9. Гварды: `AdaptiveReorderPartitionedJoin` — skip при `algorithm.isPresent()` (иначе молча переставит
   стороны и сломает ordering); `PhasedExecutionSchedule:385` — PARTITIONED-ветка работает как есть.
10. `SchedulingOrderVisitor` — не трогаем (у стримящегося merge обе стороны — remote-source одного
    фрагмента, build-first безвреден); `ValidateDependenciesChecker` — проверить, что ничего не разваливается.
11. `PlanPrinter` (~672) + explain-инфраструктура — вывод algorithm.

**Execution-side:** `LocalExecutionPlanner.visitJoin` (2539-2560) — ветка
`if (node.getAlgorithm() == MERGE) return createMergeJoin(...)` до `createLookupJoin`;
`createMergeJoin` по скелету `createLookupJoin` (2880+): probe в основном контексте, build в
`createSubContext` + `context.addDriverFactory(false, ...)`, мост — существующий `JoinBridgeManager`.

## 9.3. Partitioned merging exchange — ядро дизайна, дешевле, чем кажется

**Инварианты ExchangeNode** (104-111): сейчас orderingScheme требует `GATHER + SINGLE_DISTRIBUTION +
1 input`. Меняем: orderingScheme допустим с `type == GATHER` (как было) **или** `type == REPARTITION`
с фиксированным hash-partitioning; сохраняем `outputLayout.containsAll(ordering.orderBy())`.
Новая фабрика `mergingPartitionedExchange(id, scope, child, partitioningColumns, orderingScheme)`:
`REPARTITION` + `FIXED_HASH_DISTRIBUTION(keys)` + ordering. Семантика: **внутри каждого партициона
выход отсортирован**; гарантируется тем, что producer перед PartitionedOutput уже отсортирован
(partial SortNode), а `PagePartitioner` пишет строки в партиционный буфер в порядке входа.

**Пайплайн**: child → `SortNode(partial=true, keys)` → `mergingPartitionedExchange(REMOTE)`. Это в
точности обобщение `AddExchanges.visitSort` (597-625), где roundRobinExchange — вырожденный случай
partitioning. Вывод: **число stage'ей не растёт** — те же два exchange на join, что и в hash-варианте,
просто с ordering; query_max_stage_count не страдает.

- `PlanFragmenter.visitExchange` (502-548): **ноль правок** — REPARTITION-ветка ставит distribution
  как раньше, orderingScheme уже пробрасывается в RemoteSourceNode (545) безусловно.
- `LocalExecutionPlanner.visitRemoteSource → createMergeSource` (899-939): почти ноль — MergeOperatorFactory
  при `orderingScheme.isPresent()` уже есть. Ограничение `setDriverInstanceCount(1)` **сохранить
  осознанно**: одному партициону нужен один отсортированный поток → один merge-драйвер на таск;
  параллелизм — по партиционам. Задокументировать в правиле и тестах. Проверку `retryPolicy == NONE`
  оставить (FTE-путь для merge-источников отдельно не включаем; adaptive-планер отфильтрован гвардом).
- `MergeOperator` — уже SourceOperator, мержащий N входов; для REPARTITION сплиты приходят через
  `ExchangeSplitSource` (таск фиксированной стадии получает свои партиционы по bucketToPartition,
  сплит на upstream-таск уже отфильтрован партиционными буферами) — **scheduler-машина не меняется**.

## 9.4. Правило выбора

**Отдельное правило `DetermineJoinAlgorithm`, не расширение DetermineJoinDistributionType**: (а) SRP —
у первого тонко отлаженный cost-механизм с fallback'ами; (б) SMJ имеет смысл только когда distribution
уже PARTITIONED — правило математически *следует* за первым, так и объявим в PlanOptimizers;
(в) отдельное правило = отдельный файл тестов и kill-switch.

Матчинг sortedness: в MVP (force) не нужен (всегда вставляем сортировку). В CBO-фазе:
`context.getLookup().resolve(child)` + `PropertyDerivations.derivePropertiesRecursively(child, lookup, ...)`
(ровно так CostCalculatorWithEstimatedExchanges получает ActualProperties до AddExchanges), затем
`LocalProperties.match(ImmutableList.of(new SortingProperty<>(leftKey, ASC)), actualProps.getLocalProperties())`
для префикса equi-ключей. Кэшировать самим не надо — Lookup-дерево с мемоизацией уже не даёт взрыва.

CBO-функция — статик рядом с `calculateJoinCostWithoutOutput`:
```java
static LocalCostEstimate calculateSortedMergeJoinCostWithoutOutput(
    PlanNode left, PlanNode right, StatsProvider stats,
    boolean leftSorted, boolean rightSorted, int taskCount)
```
Слагаемые: network (repartition обеих), probe+build scan (линейно, без хэширования), сортировка
`n·log₂(n/C)` для несортированных сторон, и — важно для честности — **память**: hash держит build
резидентно, merge — O(макс. ран). Включая memory в LocalCostEstimate, SMJ выигрывает у больших build'ов
без ручных порогов. Fallback `getSizeBasedJoin` не трогаем — unknown-статы ⇒ SMJ автоматически не выбирается.

## 9.5. Оператор

**Контракт по PrestoDB, адаптированный под Trino: два пайплайна + мост.**
- Правая: `MergeJoinBufferOperator` (sink, свой DriverFactory через createSubContext, не-input) —
  принимает отсортированные страницы, буферизует deque по «ранам» ключей, exposes `MergeJoinSource`
  через `JoinBridgeManager<MergeJoinBridge>` (рефкаунтинг жизненного цикла бесплатно).
- Левая: пакет `io.trino.operator.join.merge`: `MergeJoinOperator` + мосты. Память bounded размером
  наибольшего key-рана; backpressure естественный (`needsInput=false` у sink, когда ран не потреблён).
- Итерация: классический merge — обе стороны до key_k; группы равных ключей → cross внутри пары рангов;
  не-equi filter на паре (как `isJoinPositionEligible` в DefaultPageJoiner); LEFT-outer — emit
  nulls-build для непроматченного probe-ранга; NULL-ключи не матчатся никогда (guard как в JoinProbe).
- RIGHT/FULL — отдельный driver по образцу LookupOuterOperator (хвост ранов справа) — второй этап, не MVP.
- **Не WorkProcessorOperator**: обычный Operator с кооперативным getOutput/needsInput, как
  LookupJoinOperator. WorkProcessor-вариант понадобится только для спилла ранов (тогда снимем TODO
  в WorkProcessorOperator.java:21); MVP не блокируется: bounded memory по ранам — главный selling point.
- Статистика: JoinStatisticsCounter + новый `OperatorType` для WebUI; в `JoinOperatorType` добавить
  значение MERGE_JOIN с маппингом.
- EXPLAIN: `Merge inner join with distribution PARTITIONED` рядом с distribution; проверить
  EXPLAIN (TYPE DISTRIBUTED)-тесты.

## 9.6. Iceberg

`IcebergMetadata.getTableProperties` (1003-1088): заменить `ImmutableList.of()` на SortingProperty-префикс
из `getSupportedSortFields` (1740-1764 — уже фильтрует только identity-трансформы, ровно то, что умеет
сравнивать движок). Нюанс корректности: `sortOrderId` у файла может отличаться от табличного (legacy
`sortOrderId=0`). В уже мемоизированном цикле `tableScan.planFiles()` (1028-1047) собирать
`contentFile.sortOrderId()`; SortingProperty отдавать **только если все файлы снапшота несут текущий
`sortOrder.orderId()`**. Дёшево, консервативно, без новых флагов.

Партиционированные таблицы: семантика localProperties — «свойство в каждом сплите/потоке» → (а) сплит
монолитен по порядку: Iceberg-сплиты — по data-файлу, большие файлы режутся на offset-диапазоны — нужно
гарантировать последовательное чтение диапазонов одного файла без межфайловых склеек (проверить
IcebergSplitSource) и что deletes порядок не ломают (только вырезают строки — ок). Главное
ревью-рисковое место → Iceberg-PR отдельно и с продукт-тестами.

**Bucketed+sorted — идеальный кейс**: applyPartitioning + PartitioningHandleReassigner дают co-located
join без exchange, SortingProperty — merge без сортировки (ноль exchange, ноль хэш-таблиц). Но при
**разном числе бакетов** co-location маппит несколько бакетов в один партицион — конкатенация
отсортированных бакетов не отсортирована ⇒ пересортировка обеих сторон; co-location-only путь для SMJ
закрываем. Закодировать явно в AddExchanges-ветке.

## 9.7. Нарезка PR-ов

1. **Plan model**: JoinNode.algorithm, PlanPrinter, JSON-сериализация, sanity-инварианты.
   Тесты: JSON-сериализация JoinNode, unit инвариантов. Ничего не включает.
2. **Partitioned merging exchange**: инварианты ExchangeNode + фабрика + AddExchanges строит
   merge-обмен (безотносительно join; тест через visitSort-подобный кейс). Тесты: unit ExchangeNode,
   plan-тесты AddExchanges, QueryAssertions smoke на ORDER BY через partitioned merge.
3. **Оператор**: merge-пакет + ветка LocalExecutionPlanner.visitJoin + operatorType/статистика.
   Тесты: TestMergeJoinOperator по образцу TestHashJoinOperator (отсортированные страницы, все типы
   join MVP, дубликаты ключей, NULL-ключи, пустые раны).
4. **Session property + форс-правило**: prefer_merge_join, DetermineJoinAlgorithm, гварды
   AddLocalExchanges/adaptive. Тесты: TestDetermineJoinAlgorithm (PlanBuilder), план-тесты AddExchanges
   (обе стороны = Sort + merging exchange), QueryAssertions end-to-end с флагом.
5. **CBO**: calculateSortedMergeJoinCostWithoutOutput + кандидат. Тесты: BaseCostBasedPlanTest-фикстуры.
6. **Iceberg**: localProperties + split-ordering гарантии + product-tests bucketed+sorted.

## 9.8. Committer concerns

- **Стиль**: `mvnd airstyle:format` после каждого Java-редакта; nullability — `requireNonNull` на каждое
  поле, `Optional` вместо nullable, `@Immutable` на план-узле и мостах; copy-методы без повторной валидации.
- **Naming**: `MergeJoinOperator` рядом с `MergeOperator` неудачно; пакет `io.trino.operator.join.merge`
  развязывает коллизию. Поле `algorithm` (Velox тоже так называет).
- Martin спросит «зачем, если есть spill?» — в описании PR нужны бенчмарки/мотивация: bounded memory
  без spill-IO, бесплатная сортировка с Iceberg SORT BY, отсутствие роста stage-каунта.
- **Session property**: обязательный config-дубликат, запись в docs (`docs/src/main/sphinx/admin/…`) +
  release notes, никаких deprecations в этом PR.
- **Статистики**: честная memory-компонента в cost — иначе CBO никогда не выберет SMJ и фича умрёт
  как «всегда выключенная».
