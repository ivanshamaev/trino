# Другие движки: что переносить, а что перепроверять

Исходники Spark проверены на `v4.0.1`, ClickHouse — `v25.3.3.42-lts`. Выбор фиксированных версий обеспечивает воспроизводимость и не означает, что это последние releases. Сравнение архитектурное; численные результаты чужих benchmarks на Trino не переносятся.

## Spark: явные requirements и отдельный duplicate buffer

`SortMergeJoinExec` объявляет child ordering по join keys, исполняет пары соответствующих partitions через `zipPartitions`/partition evaluator и задаёт output ordering в зависимости от типа join. Для FULL он не обещает выходной порядок. В том же файле interpreted `SortMergeFullOuterJoinScanner` хранит равные группы в `ArrayBuffer`, а matched flags — в bitsets. Следовательно, само название SMJ не доказывает bounded memory для каждого join type и skew pattern. [SortMergeJoinExec.scala](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinExec.scala).

`EnsureRequirements` сначала согласует распределения детей, затем проверяет ordering и добавляет `SortExec(..., global=false)` там, где порядок недостаточен. Это особенно полезный образец для Trino: partitioning и ordering задаются отдельно, а local sort не превращается в глобальный ORDER BY. [EnsureRequirements.scala](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala).

`SortExec` сохраняет partitioning child при local sort, использует external row sorter, prefix comparator и ограниченно применяет radix sort, когда один sort key полностью кодируется prefix. В метриках отдельно представлены sort time, peak memory и spill size. Для Trino переносима идея специализации comparator с fallback и раздельных метрик; заменять Trino Page на формат вроде UnsafeRow без анализа потери columnar locality не следует. [SortExec.scala](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/SortExec.scala).

`ExternalAppendOnlyUnsafeRowArray` начинает с копий rows в памяти, затем переключается на `UnsafeExternalSorter` без сортирующих comparators: здесь он служит spillable storage. `generateIterator(startIndex)` позволяет повторный проход; изменения буфера инвалидируют iterator. Это прямой пример того, почему **sort storage** и **replayable group storage** — разные интерфейсы. У Trino `FileSingleStreamSpiller` такого replay contract нет. [ExternalAppendOnlyUnsafeRowArray.scala](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArray.scala).

`SortMergeJoinEvaluatorFactory` связывает scanners с конкретными join semantics и bound residual condition. Не все ветви используют одинаковую структуру хранения; переносить нужно проверенные lifecycle и semantics конкретной ветки. [SortMergeJoinEvaluatorFactory.scala](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinEvaluatorFactory.scala).

AQE в Spark умеет по runtime statistics преобразовывать SMJ в broadcast/shuffled hash join и обрабатывать skew partitions. Это аргумент против SMJ как универсальной замены hash. Для Trino сначала нужен стабильный статический physical contract, затем adaptive selection на границе ещё не начатой стадии; переключение после выдачи результата гораздо сложнее. [Spark 4.0.1 Performance Tuning](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html).

## ClickHouse: full sorting merge и partial merge различаются

Официальная статья описывает full sorting merge как сортировку обоих входов с последующим merge, с возможностью пропустить sorting при подходящем physical order. Partial merge сортирует правую сторону целиком, левую — блоками; min/max ranges правых блоков сокращают чтение. Если каждый левый блок охватывает почти весь диапазон ключей, отбрасывание мало помогает. Статья полезна для алгоритмической картины, но её утверждения о памяти необходимо уточнять по исходникам конкретной версии. [ClickHouse: Full Sorting Merge and Partial Merge](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3).

В выбранном tag `FullSortingMergeJoin` является planning wrapper; `pipelineType()` возвращает `YShaped`, то есть оба входа исполняются симметрично. `isSupported` проверяет ограничения conditions/storage/USING и NULL semantics. Для Trino это свидетельство архитектурной потребности в двух входах, а не основание копировать ClickHouse semantics `ANY`, `ALL` и настройки NULL. [FullSortingMergeJoin.h](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/FullSortingMergeJoin.h).

Реальная логика merge находится в `MergeJoinTransform.cpp`. В пути `ALL` `AllJoinState` накапливает ranges/chunks с одинаковым ключом с обеих сторон и выдаёт произведение групп частями. Ограничение размера output block не ограничивает сумму удерживаемых input chunks. В просмотренном пути нет отдельного spillable duplicate-group replay. Поэтому эту реализацию нельзя использовать как доказательство строгой памяти, независимой от размера duplicate group. [MergeJoinTransform.cpp](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Processors/Transforms/MergeJoinTransform.cpp), [состояние групп](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Processors/Transforms/MergeJoinTransform.h).

`MergeJoin` в `Interpreters` — именно **PartialMergeJoin**, несмотря на общее имя. Он хранит min/max right blocks, ограничиваемый cache загруженных блоков и состояния продолжения; выбор memory/disk зависит от limits. Важное уточнение к упрощённому описанию из статьи: код допускает in-memory right blocks, а не всегда принудительно пишет всю правую сторону на диск. [MergeJoin.cpp](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/MergeJoin.cpp), [MergeJoin.h](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/MergeJoin.h).

`SortedBlocksWriter.premerge()` сливает файлы порциями по `num_files_for_merge`, повторяя проходы до допустимого числа входов. Это конкретный пример tradeoff RAM против дополнительного disk I/O и хорошая отправная точка для bounded fan-in в Trino. [SortedBlocksWriter.cpp](https://github.com/ClickHouse/ClickHouse/blob/v25.3.3.42-lts/src/Interpreters/SortedBlocksWriter.cpp).

Вывод исследования: partial merge проще встроить в build/probe pipeline, но worst-case rereads делают его сомнительным общим первым выбором для Trino. Full merge лучше соответствует целевому последовательному external processing, однако group spill нужно проектировать самостоятельно.

## Vertica: колонночное хранение с физическим порядком

Документация Vertica 24.2 описывает выбор merge join при projections, отсортированных по join columns, и hash join для неподходящего порядка; оптимизатор выбирает доступный план. Это пример, где инвестиция в storage layout окупается многократным использованием порядка. [Hash joins versus merge joins](https://docs.vertica.com/24.2.x/en/data-analysis/query-optimization/join-queries/hash-joins-versus-merge-joins/).

Для Trino отсюда следует отдельная исследовательская задача: connector должен доказывать порядок **выдаваемого потока**, включая split boundaries, parallel scan и exchanges. Hive/Iceberg sort metadata или sorted Parquet files сами по себе не являются таким доказательством. Эта рекомендация — вывод для Trino, а не утверждение о внутреннем API закрытого движка Vertica. Его spill/group internals по доступной документации не установлены.

## DuckDB: разграничить range merge и external equihash

DuckDB описывает piecewise merge join для inequality и IEJoin для двух range conditions. Это полезные примеры использования сортировки в колонночном execution engine, но IEJoin не является обычным equality SMJ и не заменяет его semantics. [Range Joins in DuckDB](https://duckdb.org/2022/05/27/iejoin).

Работа *Saving Private Hash Join* исследует larger-than-memory **hash** join: unified buffer pool, runtime compression и динамическое распределение памяти между одновременно активными joins. Это важный альтернативный baseline: невозможность держать build целиком в RAM не доказывает необходимость именно сортировки. [Описание и публикация DuckDB](https://duckdb.org/library/saving-private-hash-join-vldb/), [статья VLDB 2025](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf).

## Академические parallel merge algorithms

MPSM строит независимые частично сортированные partitions/runs и избегает обязательного финального глобального merge; работа ориентирована на NUMA и большие main-memory машины. Переносимая идея — параллелизм по partitions и locality. Результаты на main-memory hardware не доказывают эффективность disk spill, object storage или Java Page implementation. [Albutiu, Kemper, Neumann, VLDB 2012](https://arxiv.org/abs/1207.0145).

## Сравнительный вывод

| Движок/подход | Что полезно для Trino | Что нельзя предполагать |
|---|---|---|
| Spark | Physical distribution/order requirements, group replay, AQE | Одинаковая память у всех join types; совпадение NULL/NaN semantics |
| ClickHouse full merge | Двухвходовый pipeline, block processing, ordered read | Bounded duplicate groups автоматически следуют из external sort |
| ClickHouse partial merge | Disk ranges, cache, bounded file merges | Одинаковая эффективность при clustered и случайном left input |
| Vertica | Повторное использование storage ordering | Connector Trino уже выдаёт такой же доказанно ordered stream |
| DuckDB | Альтернатива external hash и память всего pipeline | Piecewise/IEJoin — готовый equijoin SMJ |
| MPSM | Parallel local runs, NUMA locality | In-memory speedups сохранятся на диске и в JVM |
