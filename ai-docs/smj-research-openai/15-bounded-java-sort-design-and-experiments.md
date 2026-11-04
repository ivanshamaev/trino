# Java sorter для большого SMJ: контракт, реализация и эксперименты

Этот документ конкретизирует новый sorting substrate для кейса из [14](14-case-15b-17b-unsorted.md). Все предлагаемые интерфейсы и состояния — проект, не уже существующие классы. Начальный вариант опирается на Java Page/Block и существующие Trino comparators; native/Velox backend не нужен.

## 1. Сначала задать область и гарантии

Единица сортировки — **одна сторона одной execution join partition**, а не storage partition, bucket file, task целиком или глобальная таблица. Новый sorter должен:

- принимать unsorted Pages с заданными schema, key channels, directions и null ordering;
- получать явные memory, temporary disk, open-reader и pending-I/O budgets;
- формировать runs без index размером со всю partition;
- возвращать monotonic cursor после EOF input и подготовки runs;
- отдавать blocked/yield вместо неограниченной работы в driver thread;
- освобождать все attempt-owned ресурсы при failure/cancel/early finish;
- отдавать точные метрики отдельно для input, sort, spill и merge.

Сортировка не удаляет duplicates. Dedup или Top-N допустимы только после отдельного доказательства SQL semantics: для обычного JOIN они меняют результат. Pure inequality JOIN без equi-key не покрывается предложенным hash repartition по JOIN key; ему нужен отдельный range/replication алгоритм, а не этот fast path.

## 2. Предлагаемые слои

```text
PartitionSorter (Operator-compatible shell)
  -> RunBuffer (PagesIndex + reservation + row cap)
  -> RunSorter (existing comparator quicksort; later specialized strategies)
  -> RunWriter / RunReader (bounded page serde and asynchronous I/O)
  -> RunRegistry (bounded resident descriptors + spillable metadata)
  -> MergeScheduler (fan-in, size tiers, storage reservation)
  -> SortedCursor (final merge, yield, memory accounting)
```

`ReplayableGroupStore` принадлежит merge join, а не этому sorter. Можно разделять page format/storage primitives, но одноразовое чтение sorted run и многократное чтение equal-key group имеют разные lifecycle. Это distinction сохраняется даже при одном temporary directory.

Предлагаемый `RunHandle` содержит attempt/partition identity, schema/type/order version, row count, byte counts, location/extents, state и checksum capability. Min/max keys полезны для диагностики/merge pruning, но их надо учитывать в retained metadata. Нельзя хранить в каждом descriptor огромную VARCHAR boundary без ограничения или отдельного key store.

## 3. Состояния и планирование работы

```text
ACCEPT_INPUT
  -> BUFFER_FULL
  -> SORT_RUN
  -> WRITE_RUN (BLOCKED на I/O)
  -> REGISTER_RUN
  -> ACCEPT_INPUT

EOF -> FLUSH_TAIL -> PREMERGE_IF_REQUIRED -> SEAL_MANIFEST
SEAL_MANIFEST -> OPEN_FINAL_READERS -> PRODUCE_SORTED_PAGES -> CLOSED
ANY_STATE -> CANCEL/FAIL -> CLEANUP -> CLOSED
```

Это схема фаз, не требование синхронной последовательности всех instances. Intermediate merge можно выполнять раньше EOF для ограничения registry, если merge schedule не вызывает квадратичного переписывания accumulated run. Reader/writer операции и bookkeeping должны быть resumable.

Первая реализация run sort может использовать существующий синхронный `PagesIndexOrdering` с ограниченным размером batch. Но это **не доказательство требуемого yield/cancel latency**: 256 MiB run с длинными строками может сортироваться долго. В измерениях нужен максимум непрерываемого вызова. Если он неприемлем, варианты: smaller run budget, explicit-stack cooperative sorter с checkpoints либо managed sorting task pool. Последний должен соблюдать query admission; общий ForkJoin pool не является подходящей неявной политикой ресурсов.

## 4. Memory admission до добавления следующей page

Нужен контракт сильнее, чем `currentBytes < limit`:

```text
currentRetained
  + incrementalPageRetained
  + addressArrayGrowth
  + requiredSortScratch
  + outputAndSerializationReserve
  <= assignedRunBudgetAndExplicitReserves
```

Pages могут делить backing blocks; actual incremental retained memory не всегда равен logical size новой page. Если точный shared ownership accounting слишком сложен, допустим консервативный over-accounting с compaction, но не скрытое under-accounting. Oversized single row/page требует определённой политики: split/compact при допустимости или контролируемая ошибка. Ни один bounded-memory контракт не исполняет произвольно огромную атомарную строку с меньшим reserve.

Run row count остаётся ограничен возможностями PagesIndex/int positions; total rows, run offsets, bytes и output/candidate counters — long. Для 15/17 млрд это обязательное требование, даже если каждый individual Page маленький.

Если producer сортирует и пишет один run, его буфер нельзя переиспользовать до завершения потребления writer-ом. Консервативный baseline блокирует input на время записи. Double buffering включается лишь после reservation второго буфера. Sort/merge фазам нужна память для завершения spill при revoke; иначе revoke сам ждёт память, которая освободится только после spill.

## 5. Fan-in выбирается по байтам и descriptors

При финальном слиянии:

```text
F_memory = floor((mergeBudget - outputReserve - scratch - treeMetadata)
                 / worstCaseResidentReaderBudget)
F_effective = min(F_configured, F_memory, F_fileDescriptors, F_IOConcurrency)
```

Здесь reader budget включает decoded pages, buffers compressed data, decompress scratch, prefetch и cursor state. Если `F_effective < 2` и требуется merge нескольких runs, сначала необходимо получить минимальный progress reserve либо завершить запрос понятной ошибкой; нельзя рассчитывать на чудесное продвижение при нулевых буферах.

Большой fan-in при мелких read buffers может ухудшить locality и SSD throughput. Prefetch увеличивает sequential I/O, но умножает память readers. Нужен ограниченный пул страниц, а не отдельная неограниченная очередь на каждый run. При object storage дополнительно существенны request latency, range-read size и число запросов; local NVMe и S3 нельзя описывать одним throughput числом.

Size-aware policy из `SortingFileWriter.combineFiles` полезна как starting point. Для онлайн-варианта нужны level/size bins и total resident-metadata cap; если descriptors уходят на диск, их чтение тоже является частью state machine. Нельзя получить строгую память, просто заменив payload на список из миллионов `Path` objects.

## 6. Формат run и перенос payload

Вариант A для MVP — страницы со всеми projected columns, сериализованные последовательно. Run buffer сортирует адреса в памяти, writer материализует row permutation в output pages. Такой формат позволяет sequential merge без random fetch исходной таблицы и естественно переживает освобождение run buffer.

Вариант B — normalized keys + durable payload references. Он может уменьшить многократное перемещение широкого payload, но требует отдельного payload store, стабильных IDs, batched gathers, учёта disk random I/O и cleanup после последнего consumer. Source file position не универсален для connector scans и вычисленных columns. Это полноценный late-materialization проект, не бесплатное преимущество pointer sorting.

Hive temporary ORC полезен как существующий пример; для query core надо сравнить ORC и compact Page serde по CPU, startup cost readers, compression, primitive/nested types и sequential read bandwidth. Выбор формата нельзя делать только по compression ratio: при многих маленьких runs reader startup/metadata может доминировать.

Большие файлы-контейнеры с extents способны сократить число filesystem files, но усложняют random reopening, recovery и reclamation. На первом этапе достаточно files per run при registry bound и ограниченном fan-in; миллионы files требуют отдельного storage design review.

## 7. Область параллелизма и согласование с JOIN

Сначала параллелить независимые paired partitions. При `C` active pairs память обоих sorters и JOIN суммируется. Logical partition count можно сделать больше `C`, но тогда input неактивных partitions должен либо flow-control-иться без циклов ожиданий, либо spool-иться. Spooling добавляет write/read и должно попасть в cost model. Нельзя просто создать 2048 operators с полноценными buffers и назвать это cheap overpartitioning.

Parallel sort внутри одной partition — позднее улучшение. Несколько threads могут генерировать independent runs; final heap merge объединяет их при bounded readers. Parallel final merge требует range/rank indexed access и ordered output reassembly. JOIN group boundary может пересекать output chunks; он продолжает группу независимо от границ sorting tasks. Split equal group для parallel relational matching — отдельный protocol, рассмотренный в [04](04-sort-merge-algorithms.md).

Если source уже sorted, возможен bypass run generation, но только при verified ordering и scheduler progress. Для исследуемого unsorted baseline bypass не включается. В обоих случаях output не становится глобально ordered между hash partitions.

## 8. План extraction из существующего Java-кода

| Изменение | Существующая база | Новая работа |
|---|---|---|
| Comparator/run sorting | PagesIndex, OrderingCompiler | Strategy boundary, byte/row cap, latency metrics |
| External run spill | OrderByOperator/spiller patterns | Explicit proactive budget, async completion, safe failure cleanup |
| Multipass scheduling | SortingFileWriter.combineFiles | Общий core/shared component, resumability, complete memory accounting |
| Final merge | MergeSortedPages/WorkProcessorUtils | Bounded readers, measured allocation profile, later loser tree |
| Connector writer reuse | SortingFileWriter/Iceberg wrapper | Возможное подключение к общей library после regression tests |
| Distribution requirements | AddExchanges, LocalExchange | Paired SMJ topology и order-preserving final path |

Точная граница общего модуля определяется dependency graph. Отделять algorithm/storage contracts от FileWriter commit/rollback; не импортировать Hive connector в ядро. Допустим incremental подход: сначала отдельный generic sorter в `trino-main`, затем обобщение shared code после стабилизации контрактов. Это меньше риск, чем одновременно менять все sorted writes и JOIN.

## 9. Эксперименты перед выбором более сложного алгоритма

Существующие базы: [BenchmarkPagesIndexOrdering](../../core/trino-main/src/test/java/io/trino/operator/BenchmarkPagesIndexOrdering.java), [BenchmarkPagesIndexPageSorter](../../core/trino-main/src/test/java/io/trino/BenchmarkPagesIndexPageSorter.java), [TestOrderByOperator](../../core/trino-main/src/test/java/io/trino/operator/TestOrderByOperator.java), [TestSortingFileWriter](../../plugin/trino-hive/src/test/java/io/trino/plugin/hive/TestSortingFileWriter.java), [TestMergingPageIterator](../../plugin/trino-hive/src/test/java/io/trino/plugin/hive/util/TestMergingPageIterator.java).

| Эксперимент | Сравнение | Что измерить |
|---|---|---|
| Run sorting | Existing quicksort / primitive BIGINT radix / introsort-like alternative | Rows/s, key comparisons, allocations, scratch, max non-yield time |
| Key representation | Block comparator / prefix+address / normalized composite key | Bytes/row, prefix collisions, cache misses, run count |
| Merge structure | PriorityQueue / array loser tree | CPU и allocations при F=4/16/64, strings и duplicate keys |
| Merge schedule | Full balanced levels / size-aware small-runs merge | Bytes read/written, live disk peak, throughput |
| Run format | Page serde / temporary ORC prototype | Codec CPU, reader retained peak, startup, request size |
| Parallelism | 1/2/4/8 active pairs при одном worker budget | Throughput, GC, fairness, disk contention |
| Layout | Compatible bucket / composite mismatched / unrelated key | Remote bytes, effective local parallelism, correctness |

Обязательно проверять sorted/reverse/random/mostly-sorted/all-equal inputs; fixed and variable keys; key ranges около run/fan-in thresholds; 1-row oversize; NULL/NaN semantics при расширении типов. При равных seeds и schema сопоставлять ровно одинаковую projected нагрузку.

## 10. Проверки корректности layout и external phases

1. `bucket(a,b)` + JOIN по `a`: строки из разных old buckets должны дать все пары; тест, соединяющий старые bucket IDs, обязан выявлять потери.
2. `bucket(a)` + JOIN по `(a,b)`: доказать допустимую colocation, но исключить pairs с разным `b`.
3. Storage `(day,region)` + JOIN customer без equality day: найти matches между разными dates/regions.
4. Разные совместимые/несовместимые bucket counts и hash versions: проверить actual exchanges, а не только output маленького запроса.
5. Local hash после remote hash: проверить, что ожидаемый parallelism не схлопнулся из-за повторного использования битов.
6. Runs `F-1`, `F`, `F+1`, `F²+1`: наблюдать intermediate passes и предел readers; correctness одинаков.
7. Строки одного key через pages/runs/intermediate outputs: sort сохраняет multiplicities, merge join видит цельную группу.
8. Delayed write/read, failed disk, cancel и nested join: нет lost rows, leaked files/reservations и cyclic waits.
9. Retry после run publication: новая attempt не читает stale local handles и не удваивает committed output.
10. Huge group при малом RAM: sorter и group store оба bounded; отдельно измеряется candidate explosion.

В этой дополнительной работе перечисленные Java experiments **не запускались**. Выполнены source audit, проверка ссылок и арифметический расчёт 16 resource scenarios. Первый engineering milestone — bounded sorter с доказанным lifecycle на малых данных и принудительных spills; затем distributed 15/17-billion workload с известными key frequencies и payload widths.
