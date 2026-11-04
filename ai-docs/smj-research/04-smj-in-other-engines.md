# 4. Sorted Merge Join в других движках

Обзор по открытым источникам (документация, код, PR) — что уже реализовано и какие уроки стоит перенести в Trino.

## 4.1. Spark — SortMergeJoinExec (промышленный стандарт big-big join)

**Архитектура.** SMJ — стратегия по умолчанию для equi-join больших таблиц. Physical planning:
`EnsureRequirements` проверяет `requiredChildDistribution` (HashClusteredDistribution по join key
с обеих сторон) и `requiredChildOrdering` (ASC по join key), при необходимости вставляет
`Exchange(hash(key))` + `Sort` в каждую ветку. Т.е. shuffle → per-partition sort → merge.

**Исполнение** (`sql/core/.../joins/SortMergeJoinExec.scala`):
- `zipPartitions(leftIter, rightIter)`; ядро — **`SortMergeJoinScanner`**: streamed-сторона (левая)
  итерируется, buffered-сторона (правая) буферизуется группами равных ключей
  (`ExternalAppendOnlyUnsafeRowArray`), для каждого streamed-ключа — все buffered-матчи
  (декартово произведение групп), потом `bufferMatchingRows` сбрасывается.
- **`ExternalAppendOnlyUnsafeRowArray`** (из SPARK-13450/PR #16909): буфер правых матчей в памяти
  до `numRowsInMemoryBufferThreshold`, потом переключение на `UnsafeExternalSorter` (без сортировки!)
  со спиллом на диск по `numRowsSpillThreshold` — защита от skew-групп (OOM на миллионах правых матчей).
  Урок SPARK-30536: при спилле правых групп итератор по спиленным строкам создавался на каждую левую
  строку → миллионы I/O; лечение — **lazy-инициализация читателя спилла** и отказ от двойного
  буферизации при переключении режимов.
- **Сортировка**: `UnsafeExternalSorter` — Tungsten: записи в off-heap страницах, pointer-array
  (`long[]`: указатель + **8-байтовый order-preserving prefix ключа**), сортировка pointer-массива
  **radix sort** (по prefix) либо TimSort fallback; спилл — отсортированные раны, финал —
  `UnsafeSorterSpillMerger` (k-way merge). Prefix-comparison — ключ к скорости: сравнение ключей
  сводится к сравнению long'ов.
- Поддерживаются INNER/LEFT/RIGHT/FULL/LeftSemi/LeftAnti/ExistenceJoin (+ `onlyBufferFirstMatch`
  для semi/anti). Не-equi condition проверяется внутри групп.

**Уроки для Trino**: (1) буфер правой группы с порогами память→спилл обязателен; (2) prefix-key +
radix sort — центральный perf-приём; (3) shuffle+sort-планирование через requirements — простой и
надёжный контракт; (4) спилл-итераторы делать ленивыми.

## 4.2. ClickHouse — full_sorting_merge / partial_merge / parallel

Документация: «JOIN algorithms»; блог «ClickHouse fully supports joins» (part 3).

**`full_sorting_merge`** — классический sort-merge: обе стороны сортируются (в памяти или внешней
сортировкой по `max_bytes_before_external_sort`), матчи — interleaved-скан двух отсортированных
стримов. **Если вход читается в порядке ключа** (ключ — префикс sorting key `MergeTree`) —
**сортировка пропускается** (read-in-order). Поддерживает INNER/LEFT/RIGHT/FULL + ASOF;
merge-стадия — **однопоточная**; параллелизация двумя способами:
- `parallel_full_sorting_merge`: **hash-sharding входов по join key и независимые per-shard merge join'ы**
  (результат неупорядочен);
- `query_plan_join_shard_by_pk_ranges`: разбиение join'а двух MergeTree по **primary-key диапазонам**
  (когда ключи содержат префикс PK обеих таблиц) — сохраняет порядок и убивает hot-bucket skew.
- **Пре-фильтрация**: `max_rows_in_set_to_optimize_join` — построение множества уникальных ключей
  одной стороны для фильтрации другой до сортировки (близко к dynamic filtering).
- Ограничения: нет SEMI/ANTI, нет OR из нескольких ключей, нет условий в ON кроме равенств ключей.

**`partial_merge`** — вариант для минимальной памяти: **полностью сортируется только правая сторона**
(внешней сортировкой блоками по `partial_merge_join_rows_in_right_blocks` + **min-max индекс на каждый
отсортированный блок**); левая сортируется блоками в памяти и каждый блок мерджится с правыми блоками
(по min-max индексу пропускаются несовместимые). Повторяет hash-join-пайплайн (build=sorted right,
scan=left). Оптимален по памяти, медленнее; эффективен когда физический порядок левой близок к ключу.

**`auto`**: spill-порог → grace_hash; иначе hash → переключение на partial_merge по лимиту
(`JoinSwitcher`).

**RFC `sorted_merge`** (issue #109045, 2026): условный токен в списке `join_algorithm` — использовать
full_sorting_merge **только если оба входа уже отсортированы** на ключе, иначе fall-through к
следующему алгоритму. Точно та же семантика, что обсуждается для Trino («SMJ при Iceberg sorted таблицах»).

**Уроки**: read-in-order (пропуск сортировки) — главный выигрыш; hash-sharding per-shard merge'ы =
Spark-подход; PK-range шардирование — лечение skew; min-max блочный индекс — способ избежать
полной сортировки левой.

## 4.3. DuckDB — Piecewise Merge Join и IEJoin (range-joins)

DuckDB использует merge join в основном для **range-условий** (`<`, `<=`, `>`, `>=`), не для equi
(там hash join быстрее):

- **Piecewise Merge Join (PWMJ)**: build-сторона сортируется по предикату полностью
  (`GlobalSortedTable`), каждая probe-чанка сортируется тем же порядком; для значения —
  бинарный поиск первого матча, далее — последовательный скан; старт поиска следующего значения —
  после предыдущего (монотонность). Остальные условия проверяются пост-фактум.
- **IEJoin** (VLDB'15): два range-условия — двойная сортировка + permutation array + bitmap
  (Bloom-оптимизация), на 1-2 порядка быстрее NLJ; параллелизация join'ом пар отсортированных
  блоков по потокам; спул блоков на диск. ASOF-join тоже использует SortStrategy (partition по
  equality-ключам + сортировка по inequality-ключу).
- Инфраструктура: `TupleDataCollection` + новый sorting code (`SortedRun`/`SortedRunMerger`,
  PR #19174), sink/source-модель параллелизма (GlobalSinkState/LocalSinkState).

**Уроки**: PWMJ-паттерн «сортированные чанки probe против глобально сортированной build» —
правомерная альтернатива полной сортировке probe; Sink/Source — знакомый Trino паттерн
(build-pipeline/probe-pipeline).

## 4.4. Velox (Presto native) — MergeJoin

`velox/exec/MergeJoin.{h,cpp}` — наиболее близкий к Trino референс (Presto-планы):

- **Контракт**: оба входа отсортированы ASC по join key; правая сторона — отдельный пайплайн,
  заканчивающийся `CallbackSink` в **`MergeJoinSource`**, левая — `MergeJoin`-оператор в своём
  пайплайне (аналог моста двух пайплайнов).
- **Алгоритм**: структуры `Match` (левая/правая) — «contiguous set of rows with all join keys equal,
  may span multiple batches»; найдя окна, оператор выдаёт **декартово произведение окон**;
  `findEndOfMatch` собирает окно через батчи; **память пропорциональна самому длинному key-match**,
  не build-стороне; после выдачи окна батчи освобождаются.
- **Векторизация** (issue #9762 / PR #9763, июнь 2024): старая реализация копировала строки в
  выходной буфер; новая собирает **dictionary-обёртки вокруг входных векторов** (индексы левых/правих
  строк); правые флаттенятся только если окно пересекает >1 правый батч. Результат: **~10× ускорение
  оператора, ~4× end-to-end**. Требование: выход выровнен по левым батчам (словарь не может обернуть
  несколько векторов) → иногда батч меньше outputBatchSize.
- Поддержаны inner/left (+semi/anti через JoinTracker, filter); right/full — «not supported yet»
  (на момент доков; расширения идут). `kMatchedLeftRows`/`kMatchedRightRows` — runtime-статы.
- PrestoDB-интеграция: `MergeJoinForSortedInputOptimizer` (merged окт. 2025) — правило включает
  merge join **для native execution когда входы отсортированы** (обе или одна сторона — тогда Sort
  добавляется на другую, Presto-on-Spark), все типы join (Velox умеет), grouped-execution-tagger
  согласован.

**Уроки**: Match-окна + словарный вывод — эталонная реализация merge-цикла; мост правой стороны
через sink — ровно паттерн Trino `JoinBridgeManager`; «память ~ максимальная группа» — целевая
характеристика.

## 4.5. PrestoDB — Java Merge Join (PR #17829, 2022) + planning

Единственная Java-реализация в Presto-семействе (не смержен в master, но проревьюена):

- **Операторы**: `MergeJoinSinkOperator` (правая сторона: без прямого вывода, складывает страницы в
  shared `MergeJoinSource`; при `merge_join_buffer_enabled=true` (default) — буферизует всё и
  завершается, иначе блокируется после каждой страницы до потребления) + `MergeJoinOperator`
  (левая: читает правые страницы из MergeJoinSource, `MergeJoinPageBuilder` — страница на каждые
  1024 выходных строки, `MergeJoinRightMatch` — буфер группы равных ключей, `MergeJoiner` на тип join).
- **Критическое ограничение**: работает **только с grouped execution** (lifespans): пара драйверов
  создаётся на каждый bucket; причина — дедлоки шедулера при двух блокирующихся сторонах без
  coordination. Буферизация правой стороны решала «застревание» build-драйверов (сплиты правой
  стороны планировались все сразу). Также требование `order_based_execution_enabled`, session
  `prefer_merge_join`; в 2025 ограничение «только bucketed+sorted таблицы с одинаковыми key spaces»
  зафиксировано в планировщике (`MergeJoinForSortedInputOptimizer` для native; ASC-only, ключи в
  порядке criteria — расширение отложено).
- Планировка: план-узел `MergeJoinNode` (separate node) + аннотация JoinNode в #17407/#17759;
  grouped-execution-tagger проверяет применимость.

**Уроки**: главный — **streaming-модель «обе стороны текут» в scheduler'е Trino-типа требует
координации** (grouped execution или bounded-мост + watermark), иначе дедлоки; буферизация build
как sink — рабочий обход; pairing драйверов per-bucket — способ параллелизации. Для Trino-форка
выбор «build материализуется до конца (сортированной) через мост + probe стримится» снимает класс
проблем PrestoDB.

## 4.6. Прочие

- **PostgreSQL**: классический MergeJoin (state-машина EXEC_MJ_*: SKIP/ADVANCE/MARK/RESTORE,
  rescans при внешних матчах, Mark/Restore для nested кортежей); выбирается оптимизатором при
  отсортированных входах/индексах или когда сортировка дешевле хэширования; ограничение — memory
  per tuple (work_mem). Урок: state-машина merge — каноническая форма; полностью в C, не векторизован.
- **Apache Pinot** (MSE, 2026): streaming merge join поверх pre-sorted pre-partitioned данных
  (leaf-stage сортировка, lazy block reading); мотивация — избегать hash join на отсортированных
  сегментах. Подтверждает тренд «sortedness-aware join selection».
- **Trino issue #17422 «Support sort merge join»** (открыт, 2023, Tardigrade-команда) + PR #17423:
  подтверждение спроса; вывод тестов авторов — «stability of SMJ is better in large-table joins
  and data skew scenarios». Не смержен.

## 4.7. Сводная таблица

| Движок | Алгоритм | Сортировка | Спилл | Параллелизм | Особенности |
|---|---|---|---|---|---|
| Spark | SortMergeJoinExec | UnsafeExternalSorter (prefix-key + radix) | обе стороны + буфер правых матчей (ExternalAppendOnlyUnsafeRowArray) | shuffle-партиции | стандарт big-big; requirements-контракт |
| ClickHouse | full_sorting_merge | in-memory / external | обе стороны | merge однопоточный; parallel = hash-shard per-shard; PK-range | read-in-order skip; пре-фильтрация ключами |
| ClickHouse | partial_merge | только правая (блочно) + min-max индекс | правая | build/scan как hash | минимальная память |
| DuckDB | PWMJ / IEJoin | build глобально, probe чанками | спул блоков | sink/source потоки | range-join фокус |
| Velox | MergeJoin | входы уже отсортированы | — (память ~ max группа) | два пайплайна (CallbackSink-мост) | dictionary-вывод ~10× |
| PrestoDB | MergeJoinOperator+Sink | bucketed+sorted входы | буфер правой | grouped execution lifespans | дедлок-уроки |
| PostgreSQL | MergeJoin | индексы/сортировка | нет (work_mem) | нет | state-машина MARK/RESTORE |

## 4.8. Что переносим в Trino (консенсус brainstorm'а)

1. **Модель Velox**: два пайплайна + мост для правой стороны; Match-окна групп равных ключей;
   словарный вывод (DictionaryBlock поверх входных страниц) — максимальный выигрыш на широких строках.
2. **Модель Spark**: буфер правой группы с порогами (память → спилл); prefix-key материализация
   (order-preserving 8-байтовый префикс) для сравнений/radix-sort/galloping; спилл ключей отдельно
   от payload.
3. **Модель ClickHouse**: условное включение при уже отсортированных входах (=Iceberg sort order);
   hash-sharding per-shard независимые merge'ы (=partitioned merging exchange); read-in-order —
   пропуск сортировки при bucketed+sorted таблицах.
4. **Урок PrestoDB**: не делать streaming обеих сторон без координации — build материализуется
   (сортированной) через JoinBridgeManager-мост; probe стримится.
5. **Требования-контракт Spark** (`requiredChildDistribution/Ordering`) — аналог в Trino:
   `PreferredProperties.partitionedWithLocal(...)` + LocalProperties.match уже существуют.
