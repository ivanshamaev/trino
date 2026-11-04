# 3. Карта точек интеграции SMJ в Trino

Код: `core/trino-main/src/main/java/io/trino/` + `plugin/trino-iceberg`. Документ — полная карта
изменений по файлам для реализации SMJ + модель sortedness.

## 3.1. JoinNode и DistributionType: все точки

`sql/planner/plan/JoinNode.java`: `enum DistributionType { PARTITIONED, REPLICATED }` (52-56),
`Optional<DistributionType> distributionType` (66) — выставляется постфактум правилами
(`DetermineJoinDistributionType`/`ReorderJoins` матчатся по `getDistributionType().isEmpty()`);
`AddExchanges.visitJoin` требует заполненное значение. Инвариант (153-160): REPLICATED запрещён для
RIGHT/FULL.

**Все места семантического pattern-matching по `DistributionType` (main-код):**

| Файл | Строки | Что делает |
|---|---|---|
| `sql/planner/optimizations/AddExchanges.java` | 954-967, 1025, 1055, 1073-1091 | REPLICATED → planReplicatedJoin, иначе planPartitionedJoin; buildJoin |
| `sql/planner/iterative/rule/DetermineJoinDistributionType.java` | 42-43, 121-182, 205 | назначение PARTITIONED/REPLICATED |
| `sql/planner/iterative/rule/ReorderJoins.java` | 92-93, 574-606 | перечисление кандидатов (+flip) |
| `sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java` | 59, 121 | матчится только PARTITIONED |
| `cost/CostCalculatorUsingExchanges.java` | 180-188 | replicated vs partitioned cost |
| `cost/CostCalculatorWithEstimatedExchanges.java` | 127-135 | то же для estimated exchanges |
| `execution/scheduler/policy/PhasedExecutionSchedule.java` | 385-393 | REPLICATED → build-фаза сначала |
| `sql/planner/planprinter/PlanPrinter.java` | 672 | печать distribution |

Ещё ~20 файлов пересобирают JoinNode через конструктор (копирование значения): `UnaliasSymbolReferences`,
`PredicatePushDown`, `IndexJoinOptimizer`, `LimitPushDown`, `PruneJoinColumns`, `ExtractSpatialJoins`,
`PushInequalityFilterExpressionBelowJoinRuleSet`, `PushAggregationThroughOuterJoin`,
`PushProjectionThroughJoin`, `PushDownDereferenceThroughJoin`, `PushPartialAggregationThroughJoin`,
`RemoveUnsupportedDynamicFilters`, `ExpressionRewriteRuleSet`, `PlanCopier`, ...

**Вывод (единодушный brainstorm'а):** добавлять `DistributionType.SORTED_MERGE` опасно — все
`else`-ветки истолкуют новое значение как PARTITIONED, инварианты RIGHT/FULL пришлось бы дублировать.
Правильный путь — **новое поле `Optional<JoinAlgorithm> algorithm`** (значение `MERGE`); SMJ —
локальный алгоритм поверх PARTITIONED (см. 09/10).

## 3.2. DetermineJoinDistributionType — механика выбора

- Паттерн: `join().matching(distributionType isEmpty)`; session `join_distribution_type`
  (BROADCAST/PARTITIONED/AUTOMATIC), лимит `join_max_broadcast_table_size`.
- AUTOMATIC → cost-based: кандидаты для исходного и flip-варианта (`addJoinsWithDifferentDistributions`,
  161-169 — **точка добавления SMJ-кандидата**), стоимость через статический
  `CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput(left, right, stats, replicated, taskCount)`
  (196-224; exchange cost + «HACK» adjustReplicatedJoinLocalExchangeCost + input cost
  `cpu = probe+build*multiplier, memory = build*multiplier`), минимум через `CostComparator`.
- **Unknown-статы ломают сравнение целиком** (`CostComparator.checkArgument(!hasUnknownComponents)`,
  fallback `getSizeBasedJoin` 105-107): эвристики по размерам (порог различия 8×).
- `mustPartition`: RIGHT/FULL; `mustReplicate`: нет equi-условий или right scalar.
- `ReorderJoins`: перечисление порядков через `MultiJoinNode`/`JoinGraph` (до 63 источников);
  `getPossibleJoinNodes` (589-623) строит кандидатов с обоими распределениями.

## 3.3. Как добавить session property (рецепт)

Образец — `DISTRIBUTED_SORT`:
1. Константа в `SystemSessionProperties.java` (:125);
2. default в `OptimizerConfig` (`@Config("...")`);
3. `booleanProperty(...)`/`enumProperty(...)` в конструкторе (:578-582 / 266-271);
4. статический геттер `isXxx(Session)` (:1535).

Для SMJ: `prefer_merge_join`/`sorted_merge_join_strategy` (boolean или enum OFF/COST_BASED/PREFER/FORCE),
default OFF; читатели — правило выбора + `LocalExecutionPlanner`.

## 3.4. AddExchanges: visitJoin и distributed ORDER BY

- `visitJoin` (945-968): REPLICATED → broadcast правой; PARTITIONED → `planPartitionedJoin` (970-1026):
  если левая сторона уже партиционирована по leftSymbols — right с транслированным preferred partitioning
  (несовместимые bucket-схемы чинятся позже в `PlanFragmenter.PartitioningHandleReassigner`);
  иначе обе — `partitionedExchange(REMOTE, symbols)`. `PreferredProperties.partitionedWithLocal(columns,
  localProperties)` уже умеет выражать «партиционировано + локально упорядочено» (образец —
  `visitWindow` 373-401: `GroupingProperty` + `orderingScheme.toLocalProperties()`).
- `visitSort` (597-625): distributed sort = `roundRobinExchange(REMOTE)` → `SortNode(partial=true)` →
  `mergingExchange(REMOTE, orderingScheme)`; физически: финальный merge — single-node `MergeOperator`
  (координаторская стадия для ORDER BY).

**Для SMJ** нужен: hash-REPARTITION по join key + сортировка каждой стороны + (в полной версии)
partitioned merging exchange. MVP: обычный `partitionedExchange` + `SortNode` (полный, со спиллом)
в join-стадии — работает уже сегодня.

## 3.5. Sortedness: как движок узнаёт, что поток отсортирован

- **SPI**: `io.trino.spi.connector.LocalProperty` (sealed): `ConstantProperty`, `GroupingProperty`,
  **`SortingProperty<E>`** (column + `io.trino.spi.connector.SortOrder` ASC/DESC × NULLS_FIRST/LAST).
  В `ConnectorTableProperties` — поле `List<LocalProperty<ColumnHandle>> localProperties`
  (поля `sortOrder` нет; сортировка сообщается через SortingProperty).
- **`OrderingScheme.toLocalProperties()`** — OrderingScheme → список SortingProperty.
- **`LocalProperties.match(actuals, desired)`** — проверка удовлетворения свойств (образец
  потребления — `AddExchanges.visitTopN` PARTIAL, 577-590).
- **`PropertyDerivations`** (вывод ActualProperties по узлам):
  - `visitSort/visitTopN/visitTopNRanking` — сортировка;
  - `visitWindow` — если вход pre-partitioned+pre-sorted — свойства пробрасываются наверх;
  - `visitExchange` — GATHER с ordering даёт single + SortingProperty (692); REMOTE без ordering сносит;
  - **`visitTableScan` (876-908) — единственный путь sortedness из коннектора**:
    `metadata.getTableProperties(...).getLocalProperties()` → трансляция через assignments;
  - `visitJoin` (554-598) — выход наследует свойства probe-стороны (INNER/LEFT), но
    `.unordered(spillPossible)`; RIGHT/FULL стирают. Для SMJ сюда нужна ветка «выход упорядочен
    по join key» (бонус: вышележащие окна/sort elimination);
  - `spillPossible` ставит `unordered(true)` — при SMJ это надо аккуратно учитывать.
- **`StreamPropertyDerivations`** — `StreamProperties.ordered()` для exchange/remote source с
  orderingScheme (364-401); `AddLocalExchanges.visitExchange` (820-832) — `withOrderSensitivity()`.

**Чего не хватает:** (1) никто не строит partitioned merging exchange; (2) коннекторы не сообщают
сортировку; (3) `visitJoin` не умеет «упорядоченный выход».

## 3.6. ConnectorTableProperties и Iceberg

- `IcebergMetadata.getTableProperties` (1003-1088) возвращает `discretePredicates` и
  `ImmutableList.of()` для localProperties с комментарием **`// todo support sorting properties`** (1086).
- При этом Iceberg SortOrder в метаданных уже используется write-путём:
  `newWritableTableHandle` → `getSupportedSortFields(schema, table.sortOrder(), orderId)` (1740-1764,
  фильтрует не-identity трансформы), `TrinoSortField` (маппинг direction/null-order →
  `io.trino.spi.connector.SortOrder`), `IcebergPageSink` пишет с сортировкой и возвращает
  `sortOrderId` в `CommitTaskData`.
- **Рецепт для чтения**: в `getTableProperties` построить `List<LocalProperty<ColumnHandle>>` из
  `getSupportedSortFields(...)` — но **консервативно**: сообщать SortingProperty только если все файлы
  снапшота (после partition pruning, в уже мемоизированном цикле `planFiles()`) имеют одинаковый
  `sortOrderId` (legacy-файлы могут иметь sortOrderId=0) и все sort-поля — identity-трансформы.
  Семантика localProperties = «в каждом сплите/потоке» — нужно гарантировать монолитность порядка
  в сплите (диапазоны одного файла читаются последовательно; deletes порядок не ломают).
- **Bucketed+sorted Iceberg — идеальный кейс SMJ**: `applyPartitioning` + `PartitioningHandleReassigner`
  дают co-located join (без exchange), SortingProperty — merge без сортировки. При **разном числе
  бакетов** co-location конкатенирует бакеты — порядок ломается, нужна пересортировка.

## 3.7. Стоимость (cost model)

- `cost/JoinStatsRule.java` — только кардинальность выхода (equi-селективности через NDV,
  join complement для outer) — **не зависит от алгоритма, менять не нужно**.
- `cost/CostCalculatorUsingExchanges.visitJoin` (180-188) — финальная стоимость join
  (costForLookupJoin: cpu = probe+build+local, memory = max) — сюда ветка SMJ.
- `CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput` (196-224) — для выбора
  стратегии; для SMJ — перегрузка `calculateSortedMergeJoinCostWithoutOutput(left, right, stats,
  leftSorted, rightSorted, taskCount)`:
  - network: repartition обеих сторон (как PARTITIONED hash);
  - cpu: scan probe+build (линейно, без хэширования) + `sortCost = n·log₂(n/runway)` для несортированных сторон;
  - **memory: O(макс. группа ключей) вместо build-размера** — включая memory в LocalCostEstimate,
    SMJ выигрывает у больших build'ов без ручных порогов.
- Правило интеграции: SMJ-кандидат не должен ломать cost-based выбор при unknown-статах —
  консервативно исключать кандидата при unknown.

## 3.8. Выбор физического оператора

`LocalExecutionPlanner.visitJoin` (2539-2560) — единственная точка: cross → nested loop; иначе
`createLookupJoin`. SMJ — ветка `if (node.getAlgorithm() == MERGE) return createMergeJoin(...)`
перед `createLookupJoin`; `createMergeJoin` — по скелету `createLookupJoin` (probe в основном
контексте, build в sub-context + `addDriverFactory(false,...)`, мост, compileJoinFilterFunction).

## 3.9. Тестовая инфраструктура

- **Operator-level**: `core/trino-main/src/test/java/io/trino/operator/join/TestHashJoinOperator.java`
  (RowPagesBuilder, `OperatorAssertion.assertOperatorEquals`, `JoinTestUtils` — setupBuildSide,
  instantiateBuildDrivers, runDriverInThread; PARTITION_COUNT=4; spill-сценарии с SMALL_MEMORY_POOL_BYTES).
- **Rule-level**: `.../iterative/rule/TestDetermineJoinDistributionType.java` (PlanBuilder из
  `iterative.rule.test`), `TestReorderJoins`, `TestJoinEnumerator`.
- **CBO**: `testing/trino-tests/.../BaseCostBasedPlanTest.java` (session с JOIN_DISTRIBUTION_TYPE=AUTOMATIC).
- **Query-level**: `core/trino-main/src/test/java/io/trino/sql/query/` (QueryAssertions, TestJoin,
  TestFullJoin); Iceberg sortedness — юнит-тесты `getTableProperties` в plugin/trino-iceberg.

## 3.10. Сводная карта изменений (файл → что менять)

**Plan-модель:** `JoinNode.java` (поле algorithm + инварианты + withAlgorithm + Jackson),
`OptimizerConfig.java` (default флага), `SystemSessionProperties.java` (свойство).

**Выбор стратегии:** новое правило `iterative/rule/DetermineJoinAlgorithm.java` (после
DetermineJoinDistributions, до AddExchanges), регистрация в `PlanOptimizers.java`; (опционально)
`ReorderJoins.java`; cost — `CostCalculatorWithEstimatedExchanges` (новая функция) +
`CostCalculatorUsingExchanges.visitJoin`.

**Распределение данных:** `ExchangeNode.java` (ослабить checks 104-111 + фабрика
`mergingPartitionedExchange`: REPARTITION + FIXED_HASH + orderingScheme); `AddExchanges.visitJoin`
(третий путь: partitioned exchange + partial SortNode + partitioned merging); `AddLocalExchanges.visitJoin`
(900-931: single/ordered потоки для SMJ-сторон, skip spill-ветки); `PropertyDerivations.visitJoin`
(выход SMJ упорядочен; учёт spillPossible); `StreamPropertyDerivations` (ordered для нового exchange);
`PlanFragmenter.visitExchange` (уже переносит orderingScheme — проверить для REPARTITION);
гварды: `AdaptiveReorderPartitionedJoin` (skip при algorithm), `PhasedExecutionSchedule` (не replicated — ок),
`SchedulingOrderVisitor` (build-first безвреден), `ValidateDependenciesChecker`,
`UseNonPartitionedJoinLookupSource` (перестраивает build-side local exchange **после**
AddLocalExchanges — PlanOptimizers:1004/1012; skip при algorithm=MERGE — риск I14, risks/02 §2.13).

**Рантайм:** `LocalExecutionPlanner.visitJoin` — ветка SMJ; новый пакет операторов (см. 05/09);
`JoinOperatorType`/WebUI-статистика; `PlanPrinter` (печать algorithm).

**Iceberg:** `IcebergMetadata.getTableProperties` (SortingProperty из getSupportedSortFields,
консервативно по sortOrderId); гарантии порядка в сплитах; product-tests bucketed+sorted.

**Ключевые факты для дизайна:** (а) merging exchange — только GATHER/single → нужен partitioned-merge;
(б) sortedness уже моделируется и выводится, матчинг есть; (в) unknown-статы ломают cost-сравнение;
(г) Iceberg сортировку не отдаёт (TODO), но все структуры (`table.sortOrder()`, `getSupportedSortFields`,
`TrinoSortField`) уже есть.
