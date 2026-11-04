# 6. Brainstorm: Java architect — границы слоёв, фазы, совместимость, риски

> Роль: Java architect. Задача — архитектурные границы, feature flags, поэтапная поставка, совместимость, риски.

## 6.1. Границы изменений по слоям

### SPI — менять почти ничего не нужно
`ConnectorTableProperties.localProperties` уже умеет выражать сортировку (`SortingProperty`), и
семантика ровно та: свойство держится **внутри одного стрима/сплита** — соответствует per-partition
отсортированности. Единственная сторона-работа — Iceberg: закрыть TODO в
`IcebergMetadata.getTableProperties` (~1086) с осторожностями: сообщать `SortingProperty` **только
если все файлы текущего снапшота** (после partition pruning) имеют одинаковый `sortOrderId`;
из transform'ов — только `identity`; row-level deletes порядок выживших строк не ломают.

### Plan model: отдельное поле `JoinAlgorithm`, НЕ новое значение `DistributionType` (центральное решение)
`DistributionType` отвечает «как стороны распределены» (PARTITIONED/REPLICATED); SMJ — ортогональная
ось: sorted merge join — PARTITIONED-джойн. Втискивание `SORTED_MERGE` в enum заставит 8 семантических
точек матчинга отвечать на нелепый вопрос «SORTED_MERGE — это PARTITIONED или REPLICATED?»;
инвариант «REPLICATED запрещён для RIGHT/FULL» пришлось бы дублировать; ~20 копирующих мест будут
молча копировать полусмысленное значение.

Рекомендация: `JoinNode` получает `Optional<JoinAlgorithm> algorithm` (enum: `HASH`, `SORTED_MERGE`),
пустое по умолчанию. Тогда: `PhasedExecutionSchedule`, инварианты RIGHT/FULL, co-location через
`PartitioningHandleReassigner` не меняются; `AddExchanges.visitJoin` идёт по PARTITIONED-ветке
(новое — только preference на ordering по join-ключам); `LocalExecutionPlanner.visitJoin`
диспетчеризует по algorithm (пусто → HASH, точка нулевого регресса); PlanPrinter печатает algorithm
только когда задан — золотые файлы не меняются; сериализация аддитивна; **нет новых PlanNode-типов**
(спасает rolling upgrade и query_max_stage_count).

### Optimizer rules: новое правило, а не расширение существующих
- `DetermineJoinDistributionType` уже перегружен (cost-based + size-fallback + must*); третья ось —
  усложнение горячего пути всех запросов ради выключенной фичи.
- **`DetermineJoinAlgorithm`** отдельным iterative-правилом после `DetermineJoinDistributions`
  (до `AddExchanges`), pattern `join().with(algorithm empty)`. Eligibility: непустые equi-критерии;
  join type (MVP INNER/LEFT); localProperties обеих сторон покрывают join-ключи одинаковым
  OrderingScheme (или режим FORCE → вставка SortNode).
- `ReorderJoins` в MVP/CBO не трогаем (перебор остаётся hash-cost-based; SMJ выбирается пост-фактум).
  Осознанный компромисс — полная интеграция в JoinGraph это комбинаторный взрыв, откладываем.
- Вставка SortNode'ов — в самом правиле (FORCE/prefer + вход не отсортирован → оборачиваем обе
  стороны по каноническому OrderingScheme); `AddExchanges` затем режет их стандартно.
- Cost model: ветка в `CostCalculatorUsingExchanges.calculateJoinCostWithoutOutput`:
  SMJ = `sortCost(left) + sortCost(right) + mergeScan`; при отсортированных входах sortCost ≈ scan;
  memory ~ O(макс. группа) вместо build-размера.

### Exchange model: partitioned merging exchange — нужно, но НЕ в MVP
MVP обходится: `SortNode` над partitioned exchange исполняется `OrderByOperator` (внешняя сортировка
со спиллом) per-partition — работает сегодня. Partitioned merging exchange (ослабить
`checkArgument(type == GATHER)` до `GATHER || REPARTITION`) — оптимизация фазы 3; трогает: инвариант
`ExchangeNode`, `StreamPropertyDerivations` (ordering внутри партиции: partitioning on keys ∧ sorted
on keys), `LocalExecutionPlanner.visitRemoteSource` (механика MergeOperator та же, не-single-task),
и главное — FTE/spooling exchange. Локальный уровень дыры нет: `LocalMergeSourceOperator` уже есть.

### Runtime: bridge-модель как в hash join, не «всё-в-одном»
Топология Trino — одновходовые операторы + мост; симметричный стриминг-мердж (Velox-style, оба входа
текут) требует dual-input исполнения, которого нет.
- build-pipeline: `SortedMergeJoinBuilderOperator` — копит, сортирует (канонический scheme), спилл
  через существующую внешнюю сортировку, отдаёт SortedLookupSourceSupplier через мост;
- probe-pipeline: стриминговый merge-оператор; residual filter внутри match-групп; LEFT — outer-emit
  в потоке; RIGHT/FULL (фаза 2) — отдельный outer-driver (аналог LookupOuterOperator);
- память bounded: build — revocable + spill; probe — стриминг; граница = макс. группа дублей
  (cap `sorted-merge-join.max-group-size`; в MVP fail-fast с внятной ошибкой лучше OOM).
- Параллелизм внутри таски: MVP — `driverInstanceCount = 1` на сторону; фаза 2 снимает через
  local merging exchange / k-way merge внутри оператора.

## 6.2. Feature flags

- **Config**: `optimizer.sorted-merge-join.strategy = OFF | COST_BASED | PREFER | FORCE`, default OFF;
  тюнинг-параметры (`max-group-size`, `min-sorted-input-bytes`) — hidden на время MVP.
- **Session**: `sorted_merge_join_strategy` (наследует config).
- Семантика: OFF — правило no-op (план байт-в-байт как сегодня); PREFER — SMJ для eligible
  (обе стороны уже отсортированы); FORCE — + вставка SortNode'ов, игнор cost (тесты/валидация);
  COST_BASED — по CostComparator (фаза 3).
- Три линии обороны нулевого регресса: (1) rule no-op; (2) `Optional<JoinAlgorithm>` пуст →
  `createLookupJoin`; (3) новые операторы не инстанцируются → нет изменений в аккаунтинге/UI/сериализации.

## 6.3. Поэтапный план

**Фаза 0 — Sortedness из Iceberg** (enabler, самостоятельно ценно): SortingProperty с консервативным
критерием. Готовность: PropertyDerivations выводит сортировку через Filter/Project/Sort;
`RemoveRedundantSorts` начинает убирать лишние SortNode (отдельная победа; поправка верификации
risks/05 D1: такого правила в Trino нет — элиминацию пишем с нуля, а реальные потребители фазы 0 —
`visitTopN` PARTIAL→Limit и `visitWindow` pre-sorted). Риск: консервативность
— лучше недосообщить, чем соврать.

**Фаза 1 — MVP (forced SMJ)**: поле JoinAlgorithm; правило DetermineJoinAlgorithm (PREFER/FORCE,
INNER/LEFT); операторы bridge-модели (спилл наследуется от внешней сортировки); driverInstanceCount=1;
DF сохраняется (DynamicFilterSourceOperator встаёт над build-sink как над HashBuilderOperator);
FTE — жёсткий gate (правило отключает себя при RetryPolicy.TASK). Готовность:
TestSortedMergeJoinOperator зелёный; TPC-H/DS с FORCE = результаты идентичны hash; bounded memory
под skew.

**Фаза 2 — Спилл + FULL outer + снятие ограничений**: spill oversized match-групп; RIGHT/FULL через
outer-driver; multi-driver таски (LOCAL merging exchange ниже join или merge внутри build-sink);
не-bucketed входы через SortNode'ы в PREFER. Готовность: десятки млрд строк без OOM, ревокации проходят.

**Фаза 3 — CBO + partitioned merging exchange**: cost-ветка; режим COST_BASED; смягчение инварианта
ExchangeNode; StreamPropertyDerivations для ordered-repartition; телеметрия
(`JoinOperatorInfo.algorithm`). Готовность: cost-режим не выбирает SMJ там, где hash быстрее.

**Фаза 4 — полная интеграция**: ReorderJoins (SMJ-aware стоимость в JoinGraph); SemiJoin; FTE/spooling
(ordered per-partition reads); возможный symmetric-streaming на WorkProcessor'ах (упрётся в TODO
спилла WorkProcessorOperator). Отложить: всё это не блокирует ценность «SMJ на отсортированных Iceberg».

## 6.4. Совместимость

- **Сериализация PlanFragment**: аддитивное Optional-поле с @JsonCreator-дефолтом; Jackson старых
  воркеров игнорирует неизвестное → на mixed-кластере план деградирует до hash join (валидного!), не
  падает. Ещё аргумент против нового план-узла.
- **EXPLAIN/PlanPrinter**: algorithm печатается только при наличии; обновить форматтер и JSON EXPLAIN (additive).
- **WebUI**: `SortedMergeJoinOperatorInfo` (match-гистограмма, размер макс. группы, spill-статистика)
  по образцу JoinOperatorInfo.
- **Spilling**: build — revocable + ревокации как у HashBuilderOperator (проще: внешняя сортировка
  уже умеет spill); требование фиксированного параллелизма наследуется.
- **FTE**: gate в MVP; в FTE спулинг exchange не гарантирует порядок чтения per-partition при retry;
  AdaptivePlanner может потерять sortedness — отдельное решение (ordered spooling), фаза 4.
- **Dynamic filtering**: bridge-модель сохраняет DF как есть (домены из build-потока до завершения
  sink'а, probe ждёт). Проблема возникнет только в symmetric-streaming варианте — ещё аргумент за
  поле в plan model.

## 6.5. Тестирование

- **Operator-level** (TestSortedMergeJoinOperator по образцу TestHashJoinOperator): все join types;
  NULL-ключи; группы дублей (включая pathological); пустые стороны; multi-page; dictionary/RLE-блоки;
  **oracle-сравнение с hash join** на случайных данных; spill-сценарии (принудительная revocation).
- **Rule-level**: eligibility по localProperties (scan→filter→project→exchange→sort); НЕ-eligibility
  при несовпадении ordering; no-op при OFF; PlanCompare (план без фичи == baseline).
- **Query-level**: QueryAssertions + Iceberg sorted/bucketed (table property `sorted_by`); FORCE на
  TPC-H/DS с diffs=0.
- **Cost-based**: расширение BaseCostBasedPlanTest — детерминированный выбор hash/SMJ по статам.
- **Product-tests**: спилл, revocation, WebUI, mixed-version.
- **Инварианты**: (1) выключенная фича меняет 0 планов; (2) SMJ-план исполняется только с валидной
  sortedness (sanity-check в PlanSanityChecker); (3) memory bounded при любом входе (fallback — spill,
  не OOM); (4) результаты идентичны hash для всех типов.

## 6.6. Варианты, рекомендация, риски

**V1 «Минимально-инвазивный»**: поле + отдельное правило + bridge-операторы + SortNode'ы, без
exchange-изменений. + быстро, нулевой регресс, переиспользование; − build материализуется,
однодрайверность в MVP, лишние стадии под SortNode.

**V2 «Exchange-first»**: сначала partitioned merging exchange, join — тонкий оператор над двумя
ordered-стримами. + красивейший end-state; − самый большой blast radius (инварианты, spooling/FTE,
StreamPropertyDerivations), дольше до первой ценности.

**V3 «Port PrestoDB»**: MergeJoinOperator+Sink + lifespans/grouped execution. + доказанный prior art;
− Trino не имеет order_based_execution/lifespans — параллельная вселенная исполнения.

**Рекомендация: V1 → эволюция в V2.** Поле JoinAlgorithm делает оба совместимыми; exchange-изменение
фазы 3 — аддитивная оптимизация под тем же план-контрактом. Prior art — источник тест-кейсов и
операторной логики, не каркас.

**Минимальный контракт: «одно Optional-поле, одно новое правило, два новых оператора, ноль новых
план-узлов, ноль SPI-изменений».**

| # | Риск | Митигация |
|---|---|---|
| 1 | Коннектор соврал про sortedness → неверные результаты | консервативный отчёт (единый sortOrderId, identity), sanity-check, fuzz-тесты, kill-switch |
| 2 | OOM на группе дублей | cap на group size + spill / fail-fast с понятной ошибкой |
| 3 | Кривой cost model → массовый регресс при COST_BASED | OFF по умолчанию, телеметрия, порог min-sorted-input-bytes |
| 4 | Rolling upgrade / сериализация | аддитивное Optional-поле, никаких новых PlanNode, mixed-version тест |
| 5 | FTE/spooling без ordering-гарантий | жёсткий gate по RetryPolicy до фазы 4 |
| 6 | ReorderJoins разрушает eligible-конфигурации | SMJ-выбор пост-reorder; интеграция — фаза 4 |
| 7 | Потеря dynamic filtering → скрытый perf-регресс | DF через build-sink сохраняется; product-test DF+SMJ |
| 8 | Multi-driver таски / LOCAL merge сложнее, чем кажется | MVP: driverInstanceCount=1; фаза 2: local merging exchange |
| 9 | Skew-партиции | наследует partitioned join; внешняя сортировка переживает skew лучше hash |
| 10 | Per-file sort orders / deletes / эволюция схемы в Iceberg | strict eligibility: только единый порядок на снапшот |

**Открытые вопросы роли**: (1) сообщать ли sortedness при equality-deletes; (2) канонический
OrderingScheme (ASC NULLS LAST) + до-сортировка при несовпадении или отказ от SMJ; (3) учёт
revocable-vs-user memory в CostComparator; (4) semi-join на SMJ (SemiJoinNode имеет свой
DistributionType — отдельная точка); (5) приемлем ли driverInstanceCount=1 для прод-пилота.
