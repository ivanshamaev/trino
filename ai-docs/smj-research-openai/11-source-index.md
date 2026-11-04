# Источники и карта кода

Локальный срез: commit `e112ffdf7304333b50bfdff50dda4229c897896d`, Trino `485-SNAPSHOT`. Дата исследования: 2026-11-04. Локальные ссылки адресуют checkout, символы указаны для навигации. Внешние ссылки ведут на первичные источники; Spark/ClickHouse tags зафиксированы для воспроизводимости. Поиск не включал запрещённые исследования.

## Проверяемые находки Trino

| Находка | Файл и ориентир |
|---|---|
| Native ordinary JOIN dispatch | [LocalExecutionPlanner](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java): `visitJoin`, `createLookupJoin`, `createNestedLoopJoin` |
| Build/probe и выбор spilling implementation | Тот же файл: `useSpillingJoinOperator`, `spillEnabled`, `buildOuter`, `partitionCount > 1` |
| Join semantics/distribution, отсутствие algorithm field | [JoinNode](../../core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java): fields, `flipChildren`, copy helpers |
| Distribution candidates и size fallback | [DetermineJoinDistributionType](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineJoinDistributionType.java): `getCostBasedJoin`, `getSizeBasedJoin`, `mustPartition` |
| Порядок optimizer passes | [PlanOptimizers](../../core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java): `AddLocalExchanges`, `OptimizeLocalExchangeConsumers`, `PushPartialAggregations` |
| Remote distribution, sort gather | [AddExchanges](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java): `visitJoin`, `visitSort` |
| Асимметричный local hash topology | [AddLocalExchanges](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java): `visitJoin`, `visitSort` |
| Поздняя замена build repartition на gather | [UseNonPartitionedJoinLookupSource](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java): `apply`, `toGatheringExchange` |
| Adaptive flip с exchange rewrites | [AdaptiveReorderPartitionedJoin](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java) |
| Build-first dependencies и unblock | [PhasedExecutionSchedule](../../core/trino-main/src/main/java/io/trino/execution/scheduler/policy/PhasedExecutionSchedule.java): `processJoin`, `unblockStagesWithFullOutputBuffer` |
| TASK отключает existing distributed sort | [SystemSessionProperties](../../core/trino-main/src/main/java/io/trino/SystemSessionProperties.java): `isDistributedSortEnabled` |
| Ограничение FTE splitting scale writers | [HashDistributionSplitAssigner](../../core/trino-main/src/main/java/io/trino/execution/scheduler/faulttolerant/HashDistributionSplitAssigner.java): `create`, `canSplit` |
| Unspill восстанавливает всю build partition | [spilling.HashBuilderOperator](../../core/trino-main/src/main/java/io/trino/operator/join/spilling/HashBuilderOperator.java): `unspillLookupSourceIfRequested`, `finishLookupSourceUnspilling` |
| Current hash matching и equality | [DefaultPagesHash](../../core/trino-main/src/main/java/io/trino/operator/join/DefaultPagesHash.java): `positionEqualsRowIgnoreNulls`; [JoinCompiler](../../core/trino-main/src/main/java/io/trino/sql/gen/JoinCompiler.java): `typeEqualsBothNotNull` |
| Hash sort-links не равны SMJ | [SortedPositionLinks](../../core/trino-main/src/main/java/io/trino/operator/join/SortedPositionLinks.java), [SortExpressionExtractor](../../core/trino-main/src/main/java/io/trino/sql/planner/SortExpressionExtractor.java) |
| Все runs участвуют в финальном merge | [OrderByOperator](../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java): `mergeSpilledAndMemoryPages`; [GenericSpiller](../../core/trino-main/src/main/java/io/trino/spiller/GenericSpiller.java): `getSpills` |
| K-way merge primitive | [MergeSortedPages](../../core/trino-main/src/main/java/io/trino/util/MergeSortedPages.java), [WorkProcessorUtils](../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorUtils.java): `mergeSorted` |
| Повторное чтение spill запрещено | [FileSingleStreamSpiller](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java): `getSpilledPages`, `writable.getAndSet(false)` |
| Index cap и retained memory | [PagesIndex](../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java): `addPage`, `calculateEstimatedSize` |
| NaN / signed zero ordering vs equality | [DoubleType](../../core/trino-spi/src/main/java/io/trino/spi/type/DoubleType.java), [RealType](../../core/trino-spi/src/main/java/io/trino/spi/type/RealType.java): equality/identical/comparison |
| One-input lifecycle | [Operator](../../core/trino-main/src/main/java/io/trino/operator/Operator.java), [Driver](../../core/trino-main/src/main/java/io/trino/operator/Driver.java) |
| WorkProcessor adapter не готовый spill shell | [WorkProcessorOperator](../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorOperator.java), [WorkProcessorOperatorAdapter](../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorOperatorAdapter.java) |
| Новый node требует dependency validator support | [ValidateDependenciesChecker](../../core/trino-main/src/main/java/io/trino/sql/planner/sanity/ValidateDependenciesChecker.java): default `visitPlan` |
| Dynamic filter producer/consumer lifecycle | [RemoveUnsupportedDynamicFilters](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/RemoveUnsupportedDynamicFilters.java), [AddDynamicFilterSource](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AddDynamicFilterSource.java) |

Дополнительные исходники и точные контракты указаны в пяти [ролевых записках](09-five-agent-brainstorm.md). Для чтения не требуется переходить в старые исследования.

## Документация текущего checkout

- [Spill to disk](../../docs/src/main/sphinx/admin/spill.md): legacy status, поддерживаемые операции, partition-size limitation.
- [Fault-tolerant execution](../../docs/src/main/sphinx/admin/fault-tolerant-execution.md): task retries, memory estimates, exchange storage.
- [Adaptive plan optimizations](../../docs/src/main/sphinx/optimizer/adaptive-plan-optimizations.md): текущая область адаптации.
- [Cost-based optimizations](../../docs/src/main/sphinx/optimizer/cost-based-optimizations.md): join order/distribution.
- [Development guide](../../.github/DEVELOPMENT.md): build, style, configuration и benchmark workflow.

## Внешние первичные источники

| Источник | Зафиксированная область анализа |
|---|---|
| [Spark SortMergeJoinExec](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinExec.scala) | Requirements, partition pairing, scanners и FULL group state |
| [Spark evaluator](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinEvaluatorFactory.scala) | Join-type dispatch и predicates |
| [Spark replay array](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArray.scala) | Spillable group storage и repeated iterator |
| [Spark SortExec](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/SortExec.scala) | External sorter, prefix/radix eligibility, metrics |
| [Spark EnsureRequirements](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala) | Shuffle requirements и local sort insertion |
| [Spark 4.0.1 tuning](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html) | AQE, join strategies, skew |
| [ClickHouse full/partial merge article](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3) | Алгоритмический обзор; не доказательство всех свойств конкретного tag |
| [ClickHouse FullSortingMergeJoin](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/FullSortingMergeJoin.h) | Y-shaped pipeline, eligibility |
| [ClickHouse merge transform](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Processors/Transforms/MergeJoinTransform.cpp), [header](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Processors/Transforms/MergeJoinTransform.h) | ALL group state, output continuation |
| [ClickHouse partial merge](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/MergeJoin.cpp), [header](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/MergeJoin.h) | Min/max ranges, cache, memory/disk paths |
| [ClickHouse SortedBlocksWriter](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/SortedBlocksWriter.cpp) | Bounded premerge passes |
| [Vertica 24.2 hash vs merge](https://docs.vertica.com/24.2.x/en/data-analysis/query-optimization/join-queries/hash-joins-versus-merge-joins/) | Projections и sort-based choice; internals не раскрыты |
| [DuckDB range joins](https://duckdb.org/2022/05/27/iejoin) | Piecewise merge и IEJoin, отличие от equijoin |
| [Saving Private Hash Join](https://duckdb.org/library/saving-private-hash-join-vldb/), [VLDB paper](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf) | Larger-than-memory external hash как альтернативное направление |
| [MPSM, VLDB 2012](https://arxiv.org/abs/1207.0145) | Parallel partition-local main-memory sort-merge |

Внешние исходники загружались для чтения во временный каталог вне репозитория; они не скопированы в документацию целиком. Ошибочные предполагаемые пути `Processors/Transforms/MergeJoinAlgorithm.cpp` и `Processors/Transforms/MergingSortedTransform.cpp` в ClickHouse дали 404; через дерево tag найдены реальные файлы. Выводы об actual join опираются на `MergeJoinTransform.cpp`, а bounded premerge — на `SortedBlocksWriter.cpp`.

## Дополнительный audit unsorted JOIN 15 млрд × 17 млрд

| Проверенный факт | Исходник |
|---|---|
| Partition columns должны входить в join-key set, constants учитываются отдельно | [Partitioning](../../core/trino-main/src/main/java/io/trino/sql/planner/Partitioning.java): `isPartitionedOn`, `isPartitionedOnExactly` |
| Reuse одной/двух сторон либо shuffle обоих inputs | [AddExchanges](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java): `planPartitionedJoin` |
| Connector bucketing не обязательно используется scan-ом | [DetermineTableScanNodePartitioning](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineTableScanNodePartitioning.java) |
| Hive способен публиковать sorting properties при соответствующем режиме | [HiveMetadata](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/HiveMetadata.java): `getTableProperties` |
| Iceberg table properties пока не публикуют sorting properties | [IcebergMetadata](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java): `getTableProperties`, TODO |
| Local hash повторно смешивается относительно stage distribution | [LocalPartitionGenerator](../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalPartitionGenerator.java): `processRawHash`; [LocalExchange](../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalExchange.java): `createPartitionFunction` |
| Tuned quicksort + insertion, pivot selection, address swapping | [PagesIndexOrdering](../../core/trino-main/src/main/java/io/trino/operator/PagesIndexOrdering.java) |
| Bounded fan-in с smallest-file scheduling уже существует | [SortingFileWriter](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/SortingFileWriter.java): `combineFiles`, `mergeFiles`, `writeSorted` |
| Writer buffer не является полным строгим peak bound | [SortBuffer](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/SortBuffer.java): `canAdd`, `flushTo` |
| Iceberg использует Hive sorting writer | [IcebergSortingFileWriter](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSortingFileWriter.java) |
| Temporary ORC format и iterator merge | [TempFileWriter](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/TempFileWriter.java), [TempFileReader](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/TempFileReader.java), [MergingPageIterator](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/MergingPageIterator.java) |

Внешние источники дополнительного сравнения: [Spark Java UnsafeInMemorySorter](https://github.com/apache/spark/blob/v4.0.1/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeInMemorySorter.java), [Spark Java RadixSort](https://github.com/apache/spark/blob/v4.0.1/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/RadixSort.java), [Spark TimSort wrapper](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/util/collection/Sorter.scala), [PostgreSQL REL_17_0 tuplesort](https://github.com/postgres/postgres/blob/REL_17_0/src/backend/utils/sort/tuplesort.c), [DuckDB sort 2021](https://duckdb.org/2021/08/27/external-sorting), [DuckDB sort redesign 2025](https://duckdb.org/2025/09/24/sorting-again), [исследование merge structures](https://arxiv.org/abs/2207.12713). Последние C/C++ engines используются как источники алгоритмических идей, не как предлагаемый runtime Trino.

Численные допущения хранятся в [JSON](calculations/capacity-assumptions.json), результаты 16 сценариев — в [CSV](calculations/capacity-model.csv). Формулы и исключения объяснены в [14](14-case-15b-17b-unsorted.md).

## Пределы доказательств

По исходникам можно установить control flow, отсутствие конкретного API и memory-sensitive structures. Нельзя по ним одним получить throughput, время на 10¹⁰ строк или надёжную production threshold. Фразы «рекомендуется», «потребуется» и «предлагается» в исследовании обозначают инженерный вывод; измеримые критерии проверки находятся в [08](08-validation-and-benchmarks.md).
