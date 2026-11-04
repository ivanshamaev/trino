# 2. Существующие механизмы сортировки и слияния в Trino

Код: `core/trino-main/src/main/java/io/trino/` (`operator/`, `util/MergeSortedPages.java`,
`sql/gen/OrderingCompiler.java`, `sql/planner/optimizations/AddExchanges.java`,
`sql/planner/LocalExecutionPlanner.java`). Этот документ — инвентаризация строительных блоков для SMJ.

## 2.1. Внешняя сортировка: OrderByOperator

`operator/OrderByOperator.java` — классический external sort «sort → spill → k-way merge»:
- **Накопление**: `addInput` → `pageIndex.addPage(page.compact())`; память индекса — **revocable**
  (может быть отозвана системой).
- **Финиш**: конвертация revocable→user memory (при неудаче — принудительный синхронный
  `spillToDisk()` + `finishMemoryRevoke`); `pageIndex.sort(pagesIndexOrdering)` — сортировка
  индекса (только перестановка адресов, блоки не копируются); `getSortedPages()` — итератор
  отсортированных страниц.
- **Спилл**: `startMemoryRevoke()` → сортирует текущий индекс и асинхронно льёт **уже отсортированные**
  страницы (`spiller.spill(pageIndex.getSortedPages())`); в `finishMemoryRevoke()` индекс очищается.
  Каждый спилл-файл = отсортированный run; финал — `mergeSpilledAndMemoryPages` (все spill-раны +
  последний in-memory ран) через `MergeSortedPages.mergeSortedPages(...)`.
- **Ключевой паттерн**: «спилл отсортированного потока + k-way merge при чтении» — готовый рецепт для SMJ.

## 2.2. PagesIndex — единственный «сортируемый» контейнер

`operator/PagesIndex.java`: `ObjectArrayList<Block>[] channels` + `LongArrayList valueAddresses`
(адрес = `(sliceIndex << 32) | position`, `SyntheticAddress`). API для SMJ:
- `sort(PagesIndexOrdering)` и **`sort(ordering, startPosition, endPosition)`** — сортировка диапазона
  (так WindowOperator сортирует внутри партиции);
- `createChannelComparator(leftChannel, rightChannel)` — **компаратор двух разных каналов** (прототип «левый vs правый» для merge);
- `getSortedPages([start, end])` — итератор, собирающий страницы через PageBuilder; физически блоки не переупорядочиваются;
- `compact()/getEstimatedSize()/clear()` — memory accounting.
Важно: `PagesIndex` же используется hash join'ом как build-сторона (`createLookupSourceSupplier`).

`PagesIndexOrdering` (`operator/PagesIndexOrdering.java`) — fork fastutil quicksort
(Bentley–McIlroy): insertion < 16, median-of-3, pseudo-median-of-9. **Не стабилен** — для join-семантики
неважно (порядок равных ключей недетерминирован), но тесты должны сравнивать с учётом этого.

## 2.3. OrderingCompiler — кодогенерация компараторов

`sql/gen/OrderingCompiler.java`, два кэша (по 1000 записей):
- `compilePagesIndexOrdering(types, sortChannels, sortOrders)` → `PagesIndexComparator`
  (сравнение двух позиций одного индекса);
- **`compilePageWithPositionComparator(types, sortChannels, sortOrders)` → `PageWithPositionComparator`**:
  ```java
  int compareTo(Page left, int leftPosition, Page right, int rightPosition);
  ```
  — **сравнение двух разных Page+position без индекса** — ровно то, что нужно для слияния двух
  потоков в SMJ. Генерация: инлайн bound MethodHandle'ов от
  `TypeOperators.getOrderingOperator(type, sortOrder, FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)`,
  early-exit по первому ненулевому; классы через `defineHiddenClass`. Fallback —
  `SimplePageWithPositionComparator` (интерпретируемый).

Нюанс для SMJ: компилятор принимает единый `sortChannels` для обеих сторон. Для разных layouts
(левая/правая страница имеют разные каналы) нужна лёгкая обёртка с двумя наборами каналов —
готового «двухканального» компаратора нет, но есть прототип (`PagesIndex.createChannelComparator`).

## 2.4. K-way merge: MergeSortedPages + WorkProcessor.mergeSorted

- **`util/MergeSortedPages.mergeSortedPages(List<WorkProcessor<Page>>, PageWithPositionComparator,
  outputTypes, outputChannels, pageBreakPredicate, ..., memoryContext, yieldSignal)`** — центральный
  блок: разворачивает потоки в `PageWithPosition`, k-way merge, сборка выходных страниц через
  PageBuilder с break-предикатом и yield.
- **`WorkProcessor.mergeSorted(processors, comparator)`** (`operator/WorkProcessorUtils.java:110`) —
  k-way merge на `java.util.PriorityQueue`, полностью lazy, обрабатывает blocked/yielded состояния.
  Для SMJ при k=2 (двусторонний merge) PQ избыточен — прямой двухпутёвый цикл дешевле (нет boxing/аллокаций узлов).
- `MergeHashSort` — merge по **хэшу** с предикатом «один hash — одна страница» (для hash-агрегаций);
  для SMJ не годится, но паттерн «кастомный компаратор + кастомный page-break» — образец.

## 2.5. Распределённая сортировка (планировщик) — прототип для SMJ

`AddExchanges.visitSort` (597-625) при `distributed_sort_enabled`:
```
child → roundRobinExchange(REMOTE) → SortNode(partial=true) → mergingExchange(REMOTE, orderingScheme)
```
- `ExchangeNode.mergingExchange(...)` (ExchangeNode.java:220-231): `Type.GATHER` +
  `SINGLE_DISTRIBUTION` (REMOTE) или `FIXED_PASSTHROUGH_DISTRIBUTION` (LOCAL) + `Optional<OrderingScheme>`.
  **Критическое ограничение (конструктор 104-111): merging exchange обязан быть GATHER с одним входом
  и single-распределением** — «распределённый merging REPARTITION» (каждая hash-партиция получает
  свои k отсортированных потоков и мержит локально) сегодня **невыразим** — главная структурная дыра для SMJ.
- `AddLocalExchanges.visitSort`: при distributed sort вставляется **LOCAL merging exchange**
  (`FIXED_PASSTHROUGH_DISTRIBUTION`) — N параллельных partial-сортировщиков → один `LocalMergeSourceOperator`.
- Физический уровень: `PlanFragmenter.visitExchange` переносит `orderingScheme` в `RemoteSourceNode`;
  `LocalExecutionPlanner.visitRemoteSource` (899-939): при `orderingScheme.isPresent()` →
  `createMergeSource` → **`MergeOperatorFactory`** (`operator/MergeOperator.java` — SourceOperator,
  мержит N `DirectExchangeClient.pages()` через `mergeSortedPages`; ограничения:
  `setDriverInstanceCount(1)`, `retryPolicy == NONE`).
- Итого: **схема «N сортировщиков → passthrough local exchange → 1 сливающий оператор» уже существует
  целиком** (локально) и «N удалённых сортировщиков → single-node merge» (глобально).

## 2.6. Локальный exchange с сохранением порядка

`operator/exchange/`: `FIXED_PASSTHROUGH` → `PassthroughExchanger` (каждый sink пишет в «свой»
source 1:1, порядок внутри потока сохраняется); `LocalExchangeSource.pages()` — `WorkProcessor<Page>`
над блокирующей очередью. **Hash-режим (`PartitioningExchanger`) порядок НЕ сохраняет** — если
SMJ-сторона проходит через hash-локальный-exchange, SortNode обязан стоять после него.

## 2.7. WindowOperator: партиционирование + сортировка + spill (второй полный пример)

- Вход копится в `OperatorPageBuffer`; конвейер: `pages → (Spillable)PagesToPagesIndexes →
  pagesIndexToWindowPartitions → WindowPartitionsToOutputPages`.
- Группировка через `PagesIndexWithHashStrategies` (PagesIndex + `PagesHashStrategy` от JoinCompiler);
  сортировка внутри партиции — `pagesIndex.sort(ordering, start, end)` по диапазонам, с пропуском
  уже отсортированных префиксов.
- **Спилл**: спиллится отсортированный `pagesIndex.getSortedPages()` (+ первая страница текущей
  группы как маркер); unspill — `mergeSortedPages(все spill-раны + in-memory)` → вливание в новый
  PagesIndex **без дополнительной сортировки** (pre-grouped + pre-sorted). Готовый паттерн
  «спилл отсортированных партиций + merge + дозагрузка».

## 2.8. Спилл-инфраструктура (`io.trino.spiller`)

- `Spiller.spill(Iterator<Page>) → ListenableFuture<DataSize>` — каждый вызов создаёт новый run;
  `getSpills() → List<Iterator<Page>>` — все раны; `FileSingleStreamSpiller` — temp-файлы,
  `PagesSerdeFactory` (+AES опционально), асинхронная запись; чтение round-robin по файлам рана.
- `GenericPartitioningSpiller` — спилл по партициям (hash join).
- Паттерн использования: `startMemoryRevoke` → подготовить отсортированный поток →
  `spiller.spill(...)` (future) → `finishMemoryRevoke` очистить память → при выдаче `getSpills()` → merge.
- Плюс «самоспилл» при неудачной конвертации revocable→user memory (OrderByOperator, WindowOperator).

## 2.9. HashAggregationOperator: спилл с синтетическим sort-ключом

`SpillableHashAggregationBuilder`: спилл групп **отсортированных по хэшу группы** (в конце дописан
hashChannel) → `MergeHashSort.merge` (k-way по precomputed hash, «один hash — одна страница») →
`MergingHashAggregationBuilder` стримит группы подряд. Образец «спилл с синтетическим ключом +
merge + streaming-обработка сгруппированных подряд строк».

## 2.10. Вывод: что переиспользуется и чего не хватает

**Переиспользуется напрямую:**
1. Сортировка стороны: `PagesIndex` + `PagesIndexOrdering` + паттерн OrderByOperator
   (включая спилл отсортированными ранами).
2. K-way merge: `MergeSortedPages` + `WorkProcessor.mergeSorted` (для k>2; для k=2 — свой цикл).
3. Компараторы: `OrderingCompiler.compilePageWithPositionComparator` (bytecode, кэш) — для merge;
   `compilePagesIndexOrdering` — для сортировки; `TypeOperators` — примитивы любых типов.
4. Распределённое слияние: `ExchangeNode.mergingExchange` + `RemoteSourceNode.orderingScheme` +
   `MergeOperator` (remote) и `LocalExchange(PASSTHROUGH)` + `LocalMergeSourceOperator` (local);
   `AddExchanges.visitSort`/`AddLocalExchanges.visitSort` — образцы вставки в планировщик.
5. Спилл: `SpillerFactory`/`SingleStreamSpiller` как есть; паттерны revoke/самоспилл отработаны.
6. Partition-local: `WindowOperator.PagesIndexWithHashStrategies` (группировка + range-sort).
7. Join-обвязка: `JoinBridgeManager` (lifecycle), `JoinFilterFunctionCompiler` (residual filter),
   `JoinStatisticsCounter` (метрики), скелет `createLookupJoin` (два пайплайна в одной задаче).

**Отсутствует (нужно создать):**
- Join-оператор, потребляющий два отсортированных потока (в `operator/join/` только hash/NLJ);
- Двухканальный компаратор merge (левая/правая страница с разными layout'ами);
- Двусторонний merge с lookahead и группами равных ключей (буфер правой группы, resume);
- Partitioned merging exchange (REPARTITION + orderingScheme) — ослабление инварианта `ExchangeNode`;
- Координация спилла двух сторон;
- Сообщение sortedness от коннекторов (Iceberg: TODO в `getTableProperties`).
