# Алгоритмы сортировки и их наличие в Java-коде Trino

Для 32 млрд входных строк нужен не один гигантский вызов `sort`, а композиция: **распределение → сортировка ограниченных runs → внешнее слияние → JOIN**. Выбор quicksort/radix относится прежде всего к generation отдельного run; он не заменяет external-memory protocol.

Проверены основные sorting paths в `core/trino-main`, SPI, Hive и Iceberg connectors текущего checkout. Утверждение «не найден» ниже относится к этим paths, а не ко всем сортировкам во всех сторонних библиотеках JVM. Velox и native runtime не рассматриваются.

## Точная карта существующей реализации

| Компонент | Реально реализованный алгоритм/роль | Можно использовать для SMJ | Чего недостаточно |
|---|---|---|---|
| [PagesIndex](../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java) | Blocks + long-адреса строк; сортируется permutation адресов | Bounded run buffer, восстановление sorted pages | Не external storage; index cap ниже 2 млрд entries |
| [PagesIndexOrdering](../../core/trino-main/src/main/java/io/trino/operator/PagesIndexOrdering.java) | Tuned Bentley–McIlroy quicksort, insertion sort для малых ranges | Первая Java run-sort реализация | Нет radix, disk spill, resumable sorting или introsort fallback в этом классе |
| [OrderingCompiler](../../core/trino-main/src/main/java/io/trino/sql/gen/OrderingCompiler.java) | Generated comparator; fallback на SimplePagesIndexComparator | Согласованные type/null/direction semantics | Не сам алгоритм сортировки |
| [PagesIndexPageSorter](../../core/trino-main/src/main/java/io/trino/operator/PagesIndexPageSorter.java) | SPI adapter: загрузить pages в PagesIndex, sort, вернуть iterator | Переиспользуемый in-memory sorter | Получает список pages, не управляет внешними runs |
| [OrderByOperator](../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java) | Run sorting, spill по revoke, финальный k-way merge всех runs | Шаблон memory/revoke и source materialization | Нет явного bounded multipass/fan-in в этом path; есть synchronous finish-spill |
| [MergeSortedPages](../../core/trino-main/src/main/java/io/trino/util/MergeSortedPages.java) | Merge упорядоченных Page streams | База sorted cursor | Не relational join; число input streams надо ограничить снаружи |
| [WorkProcessorUtils.mergeSorted](../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorUtils.java) | PriorityQueue для текущих элементов streams | Heap-based k-way baseline | Не loser tree; I/O iterator не становится async автоматически |
| [SortingFileWriter](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/SortingFileWriter.java) | Sorted temp runs, промежуточное объединение файлов, bounded fan-in | Алгоритмический образец для extraction в общий Java component | Connector write lifecycle, synchronous merge, неполный query memory contract |
| [IcebergSortingFileWriter](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSortingFileWriter.java) | Делегирует SortingFileWriter из Hive | Подтверждает reuse существующей реализации | Не самостоятельный sorter для JOIN |
| [MergeHashSort](../../core/trino-main/src/main/java/io/trino/operator/MergeHashSort.java) | Merge по hash values с группировкой равных hashes в page | Отдельные приёмы работы со streams | Hash order не key order; collision и политика page size не подходят как SMJ guarantee |

**Уточнение первого исследования:** отсутствие bounded multipass в `OrderByOperator` не означает отсутствия такого алгоритма во всём Trino. В Hive writer он уже есть, и Iceberg его использует. Для JOIN требуется перенести/обобщить нужные механизмы с другим lifecycle, а не придумать внешнюю сортировку с нуля.

## 1. Сортировка одного run: что делает PagesIndexOrdering

`sort()` вызывает `quickSort`. Для диапазона меньше 16 элементов используется insertion sort; для выбора pivot применяются median-of-three и, при длине больше 128, pseudomedian-of-nine. Равные pivot элементы обрабатываются отдельной группой. Переставляются row addresses через `PagesIndex.swap`, а не Java objects на каждую SQL-строку. Алгоритм нестабилен; SQL JOIN не требует стабильного порядка одинаковых ключей.

Это comparison sort: обычная ожидаемая стоимость порядка `n log n` сравнений на run. В классе нет depth-limit с heapsort fallback, поэтому worst-case гарантию introsort `O(n log n)` приписывать ему нельзя. Ограничение размера run ограничивает проблему, но не делает сортировку кооперативной: `pageIndex.sort` выполняется синхронно. Нужно измерить максимальную длительность одного такого вызова и cancel/revoke latency.

Generated comparator разыменовывает адреса, обращается к соответствующим blocks и сравнивает key channels. При длинных VARCHAR и composite keys CPU/cache misses могут доминировать. Bytecode generation сокращает часть dispatch overhead, но не убирает чтение payload/key buffers. `getSortedPages` затем физически собирает output pages в порядке permutation: в disk run нельзя записать только heap pointers и надеяться, что исходные blocks останутся доступны.

## 2. Bounded fan-in уже есть в connector writer

`SortingFileWriter` использует [SortBuffer](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/SortBuffer.java). При заполнении buffer создаётся sorted temp file. Очередь `tempFiles` — PriorityQueue по размеру файла. На commit `combineFiles()`:

```text
пока число файлов > maxOpenTempFiles:
    выбрать count самых маленьких файлов
    слить их в новый temp file
    добавить новый файл в очередь
    удалить успешно прочитанные старые файлы
слить оставшиеся файлы в outputWriter
```

`count = min(F, R-(F-1))`, где `F=maxOpenTempFiles`, `R` — текущее число файлов. Например, при `R=19,F=16` объединяются четыре файла, после чего остаётся 16. Это может потребовать меньше I/O, чем принудительно переписывать весь набор на одном промежуточном уровне. Выбор небольших runs уменьшает объём ранних повторных записей. Универсальная оптимальность для любых compression/parallelism/failure constraints здесь не утверждается.

[MergingPageIterator](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/MergingPageIterator.java) сливает позиции через Guava `Iterators.mergeSorted`, формирует PageBuilder и использует TypeOperators. [TempFileWriter](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/TempFileWriter.java) пишет ORC с LZ4, [TempFileReader](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/TempFileReader.java) читает ORC. Это storage-specific формат со своими readers/codec buffers; перенос всей writer stack в query core не обязателен и может быть дорогим.

[SortingFileWriterConfig](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/SortingFileWriterConfig.java) в этом срезе задаёт default buffer 64 MiB и max-open-sort-files 50. Это настройки sorted writes, **не рекомендуемые JOIN defaults** и не способ включить SMJ.

### Почему нельзя подключить writer как готовый JOIN sorter

- `writeSorted()` прямо содержит TODO про resumable execution и memory tracking; merge выполняется синхронно через iterators/consumer.
- `SortBuffer.canAdd` проверяет текущий `usedMemoryBytes < maxMemoryBytes`, но не сумму с размером следующей page. Buffer может пересечь лимит на размер добавляемой страницы.
- Учёт `SortBuffer` не является полной peak-моделью: создаваемые PagesIndex addresses, output builder, ORC readers/writer и temporary comparison structures требуют отдельного бюджета.
- `SortBuffer` допускает rowCount до `Integer.MAX_VALUE`, тогда как PagesIndex имеет более строгий cap 2 млрд; новый контракт должен учитывать минимум ограничений.
- Queue descriptors остаются в RAM до combine/cleanup: лимит одновременно открытых файлов не равен лимиту metadata всех файлов.
- Commit/rollback FileWriter и query operator `isBlocked/revoke/close` — разные протоколы. Для cancel/error нужны attempt-owned files и явная cleanup state machine.

Вывод: переносить merge scheduling policy, идею bounded runs и тестовые сценарии, но оформить общий storage/execution contract. `trino-main` не должен зависеть от `trino-hive` ради общего JOIN; подходящие части можно вынести в общий модуль либо реализовать в ядре на existing serde/filesystem primitives после dependency review.

## 3. Сравнение алгоритмов run generation

| Алгоритм | Когда полезен | Ограничения Java/SMJ | Наличие в исследованных Trino paths |
|---|---|---|---|
| Tuned quicksort + insertion | Универсальные keys, низкая дополнительная память | Comparator/cache cost, нет worst-case introsort bound | Есть: PagesIndexOrdering |
| Introsort / pattern-defeating quicksort | Защита от плохих patterns, in-place sorting | Новый код и доказательство type/permutation correctness | В указанном row-sort path не найден |
| TimSort / natural run merge | Частично отсортированные inputs | Scratch memory, stable semantics обычно не нужны JOIN | Не алгоритм PagesIndexOrdering; JVM library sort elsewhere не равен external row sorter |
| LSD radix | Fixed-width BIGINT/normalized keys, дешёвые passes | Scratch arrays и memory bandwidth; требуется правильная кодировка порядка | Готовый generalized PagesIndex radix path не найден |
| MSD radix / hybrid | Prefix locality, in-place partitions, fallback comparator | Сложная обработка variable keys и prefix ties | В исследованных paths не найден |
| Replacement selection | Формирование более длинных runs из input через heap | Per-row heap work, менее последовательные memory accesses; выигрыш зависит от данных | Run generation OrderBy/SortingFileWriter его не использует |
| Sample/range distribution sort | Parallel global sorting по общим ranges | Sampling/skew/boundary protocol; не нужен для базового equijoin | Existing SQL distributed sort не является sample/range sorter |
| Top-N heap | Требуется только ограниченное число лучших rows | Выбросит необходимые join rows при обычном полном JOIN | Top-N operators существуют, но задачу полного SMJ не решают |

Внешняя сортировка может использовать любой подходящий алгоритм для **каждого** run. Даже линейный по fixed key width radix не устраняет shuffle, запись runs, merge, payload copies и duplicate replay.

## 4. Практические Java-ориентиры из других engines

Spark `v4.0.1` содержит Java [UnsafeInMemorySorter](https://github.com/apache/spark/blob/v4.0.1/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeInMemorySorter.java): record pointer хранится рядом с key prefix, full comparator используется при совпадении prefixes. При допустимой radix strategy выбирается отдельный путь. Это Java-пример уменьшения random key accesses; зависимости TaskMemoryManager/UnsafeRow нельзя переносить без адаптации к Page/Block и memory accounting Trino.

Java [RadixSort](https://github.com/apache/spark/blob/v4.0.1/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/RadixSort.java) реализует LSD passes и требует дополнительное место в массиве. Нерадиксный [Sorter](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/util/collection/Sorter.scala) оборачивает Java TimSort. Это подтверждает реализуемость альтернатив на JVM, но не преимущество в Trino без measurement.

Для собственного BIGINT path можно сортировать primitive key/address arrays. При byte-wise radix signed порядок требует нормализации старшего sign bit; NULL — отдельного marker/partition. Prefix equality не заменяет full comparator для VARCHAR/composite keys. Для DOUBLE сохраняются NaN/±0 semantics из [07](07-correctness-and-lifecycle.md). Внутренний synthetic address действителен только пока жив соответствующий run buffer; на диск нужен сериализуемый payload либо отдельный устойчивый row store.

Полный normalized key может уменьшить comparator cost, но увеличить RAM на строку, число runs и merge levels. Сначала измерить баланс: более быстрый local sort с удвоенным index/scratch footprint способен добавить полный disk pass и ухудшить весь запрос. `List<Row>` и boxed Long comparator не рекомендуются как baseline для десятков миллиардов строк; стартовать с существующих Page/Block и primitive address arrays.

## 5. K-way merge: heap, loser tree и параллелизм

В `WorkProcessorUtils` используется PriorityQueue. Для `F` runs каждый следующий элемент требует порядка `log F` heap work. Heap-based merge — хороший correctness baseline. Кандидат на последующую оптимизацию — array-backed loser/tournament tree с фиксированными cursor IDs, уменьшающий повторные сравнения и allocations. Исследование *Implementing the Comparison-Based External Sort* сравнивает merge structures и показывает пользу loser tree в своей экспериментальной системе; это основание для JMH, а не гарантия выигрыша в Trino. [Статья](https://arxiv.org/abs/2207.12713).

Увеличение fan-in сокращает I/O, но увеличивает readers, decoded pages, codec state и pressure на файловую систему. При 16 inputs и 2 MiB полного reader budget нужно около 32 MiB только для входов **одного** merge, до heap/output/scratch. Для двух join sides и восьми instances на worker уже около 512 MiB. Если реальный reader держит больше, fan-in обязан уменьшиться либо бюджет увеличиться.

Сначала параллелить независимые partitions и runs на bounded executor, согласованном с Trino scheduling. Не запускать `parallelSort`/ForkJoin common pool внутри каждого driver без общего ограничения: одновременно работают scans, exchange, codecs и другие queries. Отдельная задача — parallel merge одной большой partition; для неё нужны indexed run blocks и ограничение одновременно pinned pages.

DuckDB демонстрирует эволюцию алгоритмов: в описании 2021 года использовались normalized keys, radix и cascading merge; в дизайне 1.4.0 от 2025 года — hybrid local sorting и parallel k-way Merge Path с выдачей chunks без полной финальной materialization. Это полезное различение: parallel two-way merge и minimization external I/O — разные цели. Native реализацию переносить не предлагается. [Описание 2021](https://duckdb.org/2021/08/27/external-sorting), [редизайн 2025](https://duckdb.org/2025/09/24/sorting-again).

Merge Path может разделять сортировочную работу по output ranks даже внутри равных значений, но SMJ не может независимо соединить только «одноимённые» chunks левой и правой стороны: потеряются cross-chunk pairs. Для JOIN нужна сборка непрерывного sorted stream или отдельный equal-group ownership/replication protocol. Parallel sort не равен готовому parallel skew join.

## 6. External merge policy: что выбрать

PostgreSQL `REL_17_0` использует quicksort run generation и balanced k-way external merge; комментарии объясняют отказ от replacement selection и старого polyphase merge. Также предусмотрено prereading для улучшения I/O locality. Это независимый подтверждённый пример того, что внешний merge остаётся полезен на современных storage, а «сложнее алгоритм» не означает «быстрее». [tuplesort.c](https://github.com/postgres/postgres/blob/REL_17_0/src/backend/utils/sort/tuplesort.c).

Для Trino предлагается два этапа:

1. Проверяемая bounded схема: existing comparator/run-sort, до `F` readers, multipass и streamed final merge. Все intermediate bytes измеряются.
2. Оптимизация schedule: byte-weighted объединение маленьких runs, online compaction с metadata bound, оптимальный выбор merge batch под disk/memory/parallelism. За основу взять уже существующий `SortingFileWriter.combineFiles`, адаптировав lifecycle.

Нельзя слепо сливать все runs, как только их стало `F+1`: частый merge большого accumulated run с несколькими маленькими может переписывать старые данные многократно. Нужны size tiers/levels либо известный batch plan; число уровней и manifests тоже ограничивается resident budget. Поэтому формула числа полных уровней в [14](14-case-15b-17b-unsorted.md) — модель конкретного balanced schedule, а не точный I/O любого merge scheduler.

## Практическая рекомендация

Первый Java SMJ sorter строить на Page/Block + PagesIndexOrdering и bounded external merge. В отдельном сравнении проверить BIGINT radix/prefix arrays, introsort-like worst-case protection и array-based loser tree. Для произвольных unsorted keys производительность определяется совокупностью **bytes, passes, CPU comparisons, payload materialization и распределения**, а не одним названием sort algorithm. Storage writer уже даёт полезный опыт multipass; общее reusable ядро всё ещё нужно.
