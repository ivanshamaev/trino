# Проверенные исходники и границы аудита

> Историческое состояние до исправлений: `773afdee3bd`. Текущие изменения и результаты проверок: [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md), [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md).

Базовая ревизия: `e112ffdf7304333b50bfdff50dda4229c897896d`. Аудируемая ревизия: `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`.
Сравнение охватывает всю ветку SMJ, включая изменения до первого аудита и последующие исправления.

Глубокий разбор выполнен для оператора и bridge, buffer lifecycle, AppendingSpiller,
FileSingleStreamSpiller, DiskBackedMatchBitmap, SortingSpiller, OrderBy/PagesIndex,
DetermineJoinAlgorithm/MergeJoinCostModel, AddExchanges/AddLocalExchanges,
LocalExecutionPlanner, PropertyDerivations/StreamPropertyDerivations, Iceberg metadata.
Для механических изменений конструкторов JoinNode проверены diff, передача/сброс
algorithm и места запуска соответствующих optimizers. Это не формальное доказательство
всех возможных последовательностей правил.

Дополнительно прочитаны связанные контракты Operator/WorkProcessorOperator,
JoinBridgeManager, StandardJoinFilterFunction, OperatorInfo/OperatorStats,
TaskInfo/TaskResource, SpillFile/GenericSpiller/SingleStreamSpillerFactory,
ConnectorTableProperties и пути обработки scan splits. Проверены тесты и критерии
приёмки этапов 0–7, исходный TODO, отчёт и документация v01, архитектурные материалы
smj-research и smj-research-openai. Не каждый исследовательский файл проверен
построчно: нормативными считаются Java-код, API-контракты и явно указанные acceptance gates.

## Изменённые production Java-файлы (61)

- [SystemSessionProperties.java](../../core/trino-main/src/main/java/io/trino/SystemSessionProperties.java) — `core/trino-main/src/main/java/io/trino/SystemSessionProperties.java`
- [MergeJoinCostModel.java](../../core/trino-main/src/main/java/io/trino/cost/MergeJoinCostModel.java) — `core/trino-main/src/main/java/io/trino/cost/MergeJoinCostModel.java`
- [JoinOperatorType.java](../../core/trino-main/src/main/java/io/trino/operator/JoinOperatorType.java) — `core/trino-main/src/main/java/io/trino/operator/JoinOperatorType.java`
- [OrderByOperator.java](../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java) — `core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java`
- [PagesIndex.java](../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java) — `core/trino-main/src/main/java/io/trino/operator/PagesIndex.java`
- [NonCopyingPageList.java](../../core/trino-main/src/main/java/io/trino/operator/join/NonCopyingPageList.java) — `core/trino-main/src/main/java/io/trino/operator/join/NonCopyingPageList.java`
- [StandardJoinFilterFunction.java](../../core/trino-main/src/main/java/io/trino/operator/join/StandardJoinFilterFunction.java) — `core/trino-main/src/main/java/io/trino/operator/join/StandardJoinFilterFunction.java`
- [AppendingSpiller.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/AppendingSpiller.java) — `core/trino-main/src/main/java/io/trino/operator/join/merge/AppendingSpiller.java`
- [DiskBackedMatchBitmap.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/DiskBackedMatchBitmap.java) — `core/trino-main/src/main/java/io/trino/operator/join/merge/DiskBackedMatchBitmap.java`
- [MergeJoinBridge.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java) — `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java`
- [MergeJoinBufferOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBufferOperatorFactory.java) — `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBufferOperatorFactory.java`
- [MergeJoinOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java) — `core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java`
- [FileSingleStreamSpiller.java](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java) — `core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java`
- [SingleStreamSpiller.java](../../core/trino-main/src/main/java/io/trino/spiller/SingleStreamSpiller.java) — `core/trino-main/src/main/java/io/trino/spiller/SingleStreamSpiller.java`
- [SortingSpiller.java](../../core/trino-main/src/main/java/io/trino/spiller/SortingSpiller.java) — `core/trino-main/src/main/java/io/trino/spiller/SortingSpiller.java`
- [SpillerStats.java](../../core/trino-main/src/main/java/io/trino/spiller/SpillerStats.java) — `core/trino-main/src/main/java/io/trino/spiller/SpillerStats.java`
- [LocalExecutionPlanner.java](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java) — `core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`
- [OptimizerConfig.java](../../core/trino-main/src/main/java/io/trino/sql/planner/OptimizerConfig.java) — `core/trino-main/src/main/java/io/trino/sql/planner/OptimizerConfig.java`
- [PlanCopier.java](../../core/trino-main/src/main/java/io/trino/sql/planner/PlanCopier.java) — `core/trino-main/src/main/java/io/trino/sql/planner/PlanCopier.java`
- [PlanOptimizers.java](../../core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java) — `core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java`
- [RelationPlanner.java](../../core/trino-main/src/main/java/io/trino/sql/planner/RelationPlanner.java) — `core/trino-main/src/main/java/io/trino/sql/planner/RelationPlanner.java`
- [AdaptiveReorderPartitionedJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java`
- [DetermineJoinAlgorithm.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineJoinAlgorithm.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineJoinAlgorithm.java`
- [EliminateCrossJoins.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/EliminateCrossJoins.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/EliminateCrossJoins.java`
- [ExpressionRewriteRuleSet.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExpressionRewriteRuleSet.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExpressionRewriteRuleSet.java`
- [ExtractSpatialJoins.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExtractSpatialJoins.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExtractSpatialJoins.java`
- [ImplementTableFunctionSource.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ImplementTableFunctionSource.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ImplementTableFunctionSource.java`
- [PruneJoinColumns.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PruneJoinColumns.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PruneJoinColumns.java`
- [PushAggregationThroughOuterJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushAggregationThroughOuterJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushAggregationThroughOuterJoin.java`
- [PushDownDereferenceThroughJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushDownDereferenceThroughJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushDownDereferenceThroughJoin.java`
- [PushInequalityFilterExpressionBelowJoinRuleSet.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushInequalityFilterExpressionBelowJoinRuleSet.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushInequalityFilterExpressionBelowJoinRuleSet.java`
- [PushPartialAggregationThroughJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughJoin.java`
- [PushProjectionThroughJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushProjectionThroughJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushProjectionThroughJoin.java`
- [RemoveUnsupportedDynamicFilters.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/RemoveUnsupportedDynamicFilters.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/RemoveUnsupportedDynamicFilters.java`
- [ReorderJoins.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ReorderJoins.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ReorderJoins.java`
- [TransformCorrelatedDistinctAggregationWithProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithProjection.java`
- [TransformCorrelatedDistinctAggregationWithoutProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithoutProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedDistinctAggregationWithoutProjection.java`
- [TransformCorrelatedGlobalAggregationWithProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithProjection.java`
- [TransformCorrelatedGlobalAggregationWithoutProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithoutProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGlobalAggregationWithoutProjection.java`
- [TransformCorrelatedGroupedAggregationWithProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithProjection.java`
- [TransformCorrelatedGroupedAggregationWithoutProjection.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithoutProjection.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedGroupedAggregationWithoutProjection.java`
- [TransformCorrelatedInPredicateToJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedInPredicateToJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedInPredicateToJoin.java`
- [TransformCorrelatedJoinToJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedJoinToJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformCorrelatedJoinToJoin.java`
- [TransformFilteringSemiJoinToInnerJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformFilteringSemiJoinToInnerJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformFilteringSemiJoinToInnerJoin.java`
- [TransformUncorrelatedSubqueryToJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformUncorrelatedSubqueryToJoin.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/TransformUncorrelatedSubqueryToJoin.java`
- [UseNonPartitionedJoinLookupSource.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java) — `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java`
- [AddExchanges.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java`
- [AddLocalExchanges.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java`
- [IndexJoinOptimizer.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/IndexJoinOptimizer.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/IndexJoinOptimizer.java`
- [LimitPushDown.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/LimitPushDown.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/LimitPushDown.java`
- [PredicatePushDown.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PredicatePushDown.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PredicatePushDown.java`
- [PropertyDerivations.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PropertyDerivations.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PropertyDerivations.java`
- [StreamPropertyDerivations.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/StreamPropertyDerivations.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/StreamPropertyDerivations.java`
- [UnaliasSymbolReferences.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/UnaliasSymbolReferences.java) — `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/UnaliasSymbolReferences.java`
- [ExchangeNode.java](../../core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java) — `core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java`
- [JoinNode.java](../../core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java) — `core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java`
- [SortNode.java](../../core/trino-main/src/main/java/io/trino/sql/planner/plan/SortNode.java) — `core/trino-main/src/main/java/io/trino/sql/planner/plan/SortNode.java`
- [PlanPrinter.java](../../core/trino-main/src/main/java/io/trino/sql/planner/planprinter/PlanPrinter.java) — `core/trino-main/src/main/java/io/trino/sql/planner/planprinter/PlanPrinter.java`
- [IcebergConfig.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergConfig.java) — `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergConfig.java`
- [IcebergMetadata.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java) — `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java`
- [IcebergSessionProperties.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSessionProperties.java) — `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSessionProperties.java`

## Что не является проверкой исполнения

- 103 TPC-DS query parts из v01 — проверка плана по metadata fixtures; выполнения SF-1000 нет.
- Результаты 10 млн / 2³¹ строк и macro v01 здесь рассмотрены как ранее сохранённые свидетельства,
  а не повторно полученные измерения. В v02 эти тяжёлые сценарии не запускались.
- В исходном функциональном аудите не выполнялись полный reactor build, полный набор connector/product tests,
  error-prone compiler, hardware profiling, многоворкерный нагрузочный прогон и failure injection TASK retries.
  Последующие модульные pre-PR проверки отражены в [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md).
- Файлы hs_err_pid*.log не читались. Производственный Java-код по итогам аудита не изменён.

Машиночитаемый перечень также находится в [scope.json](evidence/scope.json).
