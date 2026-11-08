# Область третьего аудита

Ревизия `0393e45fcf9`, ветка `smj-plan`. База diff: `e112ffdf730`. Проверяется реализация после исправлений v02, а не исходный HEAD второго аудита.

Изменено 106 software/test/docs/fixture files: 64 production Java, 35 test Java, 4 public docs и 3 plan fixtures. Полный перечень — [scope.json](evidence/scope.json).

## Глубина разбора

- Полностью разобраны merge transformation, build buffer, bridge и replay cache, append spiller, disk bitmap, indexed native spill и bounded SortingSpiller. Проверены вызываемые memory contexts, Driver/WorkProcessor adapter, JoinBridgeManager, SpillFile и агрегация operator info.
- В planner прослежены выбор алгоритма, canonical sorts, partitioning/ordering exchanges, single-stream enforcement, local operator creation и последующие rewrites. Особое внимание — реальным фазам использования PushProjectionThroughJoin и PushPartialAggregationThroughExchange.
- В Iceberg проверены nullable file IDs, identity prefix, snapshot/predicate cache, projection translation и совместимость file/split order с engine properties.
- Остальные изменения logical planning/constructor sites просмотрены как изменения diff с контекстом вызова; это преимущественно создание пустого маркера до выбора алгоритма либо сохранение маркера при структурно безопасном rewrite. Перечень файлов не означает построчное чтение всех неизменённых частей каждого большого класса.
- Прочитаны исходный dev plan/stages, материалы v02 и пользовательская документация. Прежние acceptance и новые public claims сопоставлены с фактически проверяемым scope.

Production fixes не применялись. Audit probes находятся в отдельном detached checkout. `hs_err_pid*.log` не читались.

## Production Java paths

- `core/trino-main/src/main/java/io/trino/SystemSessionProperties.java`
- `core/trino-main/src/main/java/io/trino/cost/MergeJoinCostModel.java`
- `core/trino-main/src/main/java/io/trino/operator/JoinOperatorType.java`
- `core/trino-main/src/main/java/io/trino/operator/OperatorInfo.java`
- `core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java`
- `core/trino-main/src/main/java/io/trino/operator/PagesIndex.java`
- `core/trino-main/src/main/java/io/trino/operator/join/NonCopyingPageList.java`
- `core/trino-main/src/main/java/io/trino/operator/join/StandardJoinFilterFunction.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/AppendingSpiller.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/DiskBackedMatchBitmap.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBufferOperatorFactory.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java`
- `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorInfo.java`
- `core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java`
- `core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java`
- `core/trino-main/src/main/java/io/trino/spiller/SingleStreamSpiller.java`
- `core/trino-main/src/main/java/io/trino/spiller/SortingSpiller.java`
- `core/trino-main/src/main/java/io/trino/spiller/SpillerStats.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/OptimizerConfig.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/PlanCopier.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/RelationPlanner.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineJoinAlgorithm.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/EliminateCrossJoins.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExpressionRewriteRuleSet.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExtractSpatialJoins.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ImplementTableFunctionSource.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PruneJoinColumns.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushAggregationThroughOuterJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushDownDereferenceThroughJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushInequalityFilterExpressionBelowJoinRuleSet.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushProjectionThroughJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/RemoveUnsupportedDynamicFilters.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ReorderJoins.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithoutProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithoutProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithoutProjection.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedInPredicateToJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedJoinToJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformFilteringSemiJoinToInnerJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformUncorrelatedSubqueryToJoin.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/IndexJoinOptimizer.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/LimitPushDown.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PredicatePushDown.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PropertyDerivations.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/StreamPropertyDerivations.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/UnaliasSymbolReferences.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/plan/SortNode.java`
- `core/trino-main/src/main/java/io/trino/sql/planner/planprinter/PlanPrinter.java`
- `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergConfig.java`
- `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java`
- `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSessionProperties.java`

## Дополнительные точки проверки

- `core/trino-main/src/main/java/io/trino/operator/Driver.java` — scheduling, empty pages, revoke/finish protocol.
- `core/trino-main/src/main/java/io/trino/operator/WorkProcessorOperatorAdapter.java` — continuation и lifecycle.
- `core/trino-main/src/main/java/io/trino/operator/join/JoinBridgeManager.java` — destroy ожидает build future и завершения probe references.
- `core/trino-main/src/main/java/io/trino/spiller/SpillFile.java` — append, synchronization и seekable reader.
- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughExchange.java` — различие ordered-exchange guards двух patterns; отдельный distributed probe не подтвердил тихую потерю ordering.
- `core/trino-main/src/main/java/io/trino/operator/aggregation/builder/InMemoryHashAggregationBuilder.java` — first-seen group IDs и отличие обычного output от hash-sorted spill output.
- `core/trino-spi/src/main/java/io/trino/spi/type/` — сравнение scalar keys, NULL, floating-point exclusions.
