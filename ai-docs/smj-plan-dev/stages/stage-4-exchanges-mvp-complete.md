# Stage 4 — MERGE-ветка обменов + e2e: MVP завершён

PR4 фазы 1 ([risks/04 §4.2 п.4-5](../../smj-research/risks/04-implementation-plan-with-risk-gates.md)).
Зависит от stage 2 (оператор) и stage 3 (маркер ставится правилом). Закрывает MVP.

## Цель

Физическая планировка SMJ: сортировка сторон внутри join-стадии + параллельный скан через
LOCAL merging exchange + DF-обёртка build + e2e-приёмка «железных» гейтов фазы 1.

## Состав работ

| Файл | Изменение | Якорь |
|---|---|---|
| `AddExchanges.visitJoin` | MERGE-ветка: SortNode после partitioned-exchange **внутри join-стадии** (стадии не растут — I7); co-located-ветка (S1-GOLD): без REPARTITION, per-bucket merge сплитов (fast-path C5) — **использует sortedness при наличии (stage 0); при bucketed-но-не-sorted деградирует до локальной сортировки бакетов (S2)**, что и есть лучший несортированный кейс | visitJoin:945; 09 §9.3; 11 §11.3 |
| `AddLocalExchanges` | под каждой SMJ-стороной: частичная сортировка по сплитам + **LOCAL merging exchange** по паттерну `visitSort` (N параллельных сортировщиков → `ExchangeNode.mergingExchange(LOCAL, orderingScheme)` — GATHER + FIXED_PASSTHROUGH_DISTRIBUTION, ExchangeNode:220-231 → 1 merge-драйвер `LocalMergeSourceOperatorFactory`) — иначе singleStream сериализует весь probe-pipeline до однопоточного скана; гварды: SMJ-стороны — single-stream **от merge-источника**, не от скана | P11 (risks/05 §5.3); LocalMergeSourceOperator (`operator/exchange/`, :38); `createMergeSource` требует retryPolicy==NONE + driverInstanceCount(1) (LEP:909-937) |
| `LocalExecutionPlanner` (ветка MERGE в `createMergeJoin`) | сборка двух пайплайнов: build = MergeJoinBufferOperator (+ DF-обёртка `createDynamicFilterSourceOperatorFactory` над sink, паттерн :2959 — только INNER), probe = MergeJoinOperator; SortNode над сторонами планируется обычным путём (revocable-память сортировок спиллится) | stage 2; risks/08 §8.2 |
| `PropertyDerivations.visitJoin` | вывод сортированности выхода SMJ (для вышележащих Window/TopN) — additive, OFF не меняет | 03 §3.10 |
| EXPLAIN/PlanPrinter | алгоритм + факт co-location в выводе (только при MERGE) | I10, P7 (ложные ожидания) |
| e2e-тесты | QueryAssertions: forced-SMJ запросы INNER/LEFT; equivalence с hash; bucketed+sorted каталог (product-tests) | sql/query/; testing/trino-product-tests |

## Acceptance (гейты фазы 1 из risks/04 §4.2 — «железные»)

- **Правильность**: oracle-фаззинг ≥ 10⁶ строк (полная матрица из stage 2, расширенная
  план-уровнем: оба входа через exchange);
- **Инварианты движка**: OFF — планы байт-в-байт; `finish()` идемпотентен; ревокация в середине
  merge (`SMALL_MEMORY_POOL_BYTES`); отмена не течёт spill-файлами; mixed-version сериализация;
- **Производительность**: TPC-H/TPC-DS с FORCE — результаты идентичны hash; время ≤ 2× от hash
  на SF-100 (SMJ не обязан выигрывать на малых данных);
- stage-count(SMJ-план) == stage-count(hash-план) (I7-тест).

## Откат

`prefer_merge_join=OFF` — операторы не инстанцируются (маркер не ставится).

## Согласованность (проверено против корпуса)

- [x] **risks/04 §4.2 п.4-5 + acceptance**: состав и гейты зеркалированы; LOCAL merging
  (P11-митигация) — в составе; гвард UseNonPartitionedJoinLookupSource уже в stage 3;
- [x] **09 §9.3/§9.7 п.4**: «partitioned exchange + SortNode; LOCAL merging под join» —
  совпадает; e2e с флагом — тот же список;
- [x] **11 §11.2/§11.3 (S1-S8)**: co-located ветка только при одинаковом handle
  (bucket по ⊆ equi-ключей, same N, same catalog — S-N1); иначе REPARTITION+sort —
  соответствие матрице 15 §15.11;
- [x] **risks/05 §5.11.2**: вся merge-механика (REMOTE createMergeSource:909-937;
  LOCAL createLocalMerge→LocalMergeSourceOperatorFactory) размечена — состав ссылается на неё;
- [x] **risks/02 §2.7 (I7)**: SortNode внутри join-стадии — стадии не растут; тест включён;
- [x] **risks/08 §8.2**: DF-обёртка над build-sink, INNER-only — в LEP-сборке;
- [x] **12 §12.x**: сортировка сторон — существующий OrderByOperator (quicksort-раны +
  MergeSortedPages:42), новых алгоритмов на этом этапе не вводится.
