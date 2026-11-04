# Brainstorm: Java optimization engineer

Независимая роль: инженер оптимизации Java/JVM. Срез кода: `e112ffdf7304333b50bfdff50dda4229c897896d`, `485-SNAPSHOT`. Исследование выполнено чтением исходного кода; измерения производительности не проводились. Ни один из запрещённых каталогов исследований не использовался. Ниже отдельно обозначены наблюдения и предлагаемые решения.

## Основной вывод

Для больших equi-join перспективен partitioned SMJ с внешней сортировкой и ограниченной памятью **всех** промежуточных стадий. Само сравнение двух отсортированных потоков — небольшая часть работы. Основные риски: неограниченное число сортировочных runs, повторное чтение duplicate groups, удержание больших backing blocks маленькими views, семантика равенства и переходы между revocable/user memory.

Первая реализация должна сохранять `Page`/`Block` и операторы типов Trino, применять hash repartition перед локальной сортировкой и вводить явный контракт повторно читаемого хранилища группы одинаковых ключей. Обещание «SMJ использует O(1) памяти» корректно только после описания внешней сортировки, fan-in, duplicate groups, очередей и памяти сериализации.

## Что уже есть в коде

| Компонент | Проверенное наблюдение | Следствие для SMJ |
|---|---|---|
| [`Page`](../../../core/trino-spi/src/main/java/io/trino/spi/Page.java): `getRegion`, `compact`, `getRetainedSizeInBytes` | `getRegion` создаёт views блоков; `compact` копирует/уплотняет удерживаемые данные | Маленькая группа ключа может удерживать большой исходный блок. Учёт по logical size недостаточен |
| [`Block`](../../../core/trino-spi/src/main/java/io/trino/spi/block/Block.java): `getPositions`, `copyPositions`, `getRegion`, `copyRegion` | Views могут удерживать исходные данные и positions array; `copyRegion`/`copyPositions` имеют контракт компактного представления | Нужна политика ownership/compaction, а не безусловный zero-copy |
| [`PagesIndex`](../../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java): `addPage`, `calculateEstimatedSize`, `sort`, `getSortedPages` | Держит blocks и `LongArrayList` адресов строк; `addPage` запрещает индекс размером ≥2 млрд entries | Один `PagesIndex` не является контейнером для десятков миллиардов строк. Ограничивать каждый sort run байтами и числом позиций |
| [`OrderByOperator`](../../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java): `spillToDisk`, `finish`, `mergeSpilledAndMemoryPages` | Сортирует `PagesIndex`, сбрасывает run, затем объединяет все spill streams и оставшийся memory stream | Полезный прототип внешней сортировки; в этом пути нет явного многофазного merge с ограниченным fan-in |
| [`GenericSpiller`](../../../core/trino-main/src/main/java/io/trino/spiller/GenericSpiller.java): `spill`, `getSpills` | Каждому spill соответствует новый `SingleStreamSpiller`; список хранится до закрытия | Число runs влияет на ресурсы; нельзя считать финальный merge константным по памяти |
| [`MergeSortedPages`](../../../core/trino-main/src/main/java/io/trino/util/MergeSortedPages.java): `pageWithPositions`, `buildPage` | Учитывает retained bytes входных страниц и output builder; создаёт `PageWithPosition` на каждую позицию | Переиспользование семантики возможно; allocation rate в hot loop нужно измерить |
| [`WorkProcessorUtils`](../../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorUtils.java): `mergeSorted` | K-way merge использует `PriorityQueue` и ожидает head каждого активного источника | Память зависит от K, CPU примерно `O(N log K)` сравнений; blocked source ограничивает продвижение |
| [`FileSingleStreamSpiller`](../../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java): `getSpilledPages` | Повторное чтение запрещено через `writable.getAndSet(false)`; encryption key удаляется из поля после выдачи reader | Этот класс нельзя напрямую использовать как rewindable duplicate-group store |

Лимит `PagesIndex` относится к конкретному индексу, а не к суммарному размеру таблицы/запроса: распределённое исполнение и spill меняют масштаб задачи.

## Семантика ключей: comparator=0 не означает SQL equality

[`TypeOperators`](../../../core/trino-spi/src/main/java/io/trino/spi/type/TypeOperators.java) различает `getEqualOperator`, `getIdenticalOperator`, `getOrderingOperator` и comparison operators. [`SimplePagesIndexComparator`](../../../core/trino-main/src/main/java/io/trino/operator/SimplePagesIndexComparator.java) получает ordering с `SortOrder`; [`BlockTypeOperators`](../../../core/trino-main/src/main/java/io/trino/type/BlockTypeOperators.java) предоставляет специализированные интерфейсы над block positions. Предлагается использовать эти контракты, не Java `Comparable`, не произвольную сериализацию байтов и не битовый порядок floating-point значений.

Конкретный контрпример в текущем срезе: [`DoubleType`](../../../core/trino-spi/src/main/java/io/trino/spi/type/DoubleType.java) и [`RealType`](../../../core/trino-spi/src/main/java/io/trino/spi/type/RealType.java).

| Значения | Ordering | `EQUAL` | `IDENTICAL` |
|---|---:|---:|---:|
| NaN, NaN | 0 | false | true |
| +0, -0 | 0 | true | true |
| NULL, NULL | Зависит от null placement; одинаковая группа сортировки | SQL unknown | true |

`DoubleType.compare` специально уравнивает ±0; прямой `Double.compare` без этой обработки даст другую группировку. Hash-код нормализует ноль. NULL placement является свойством сортировки и не даёт разрешения соединять NULL в обычном equi-join.

Для сверки с hash baseline: [`DefaultPagesHash`](../../../core/trino-main/src/main/java/io/trino/operator/join/DefaultPagesHash.java) вызывает `positionEqualsRowIgnoreNulls` / `positionEqualsPositionIgnoreNulls`. [`JoinCompiler.typeEqualsBothNotNull`](../../../core/trino-main/src/main/java/io/trino/sql/gen/JoinCompiler.java) берёт `EQUAL` с `DEFAULT_ON_NULL`; [`JoinProbe.fillCache`](../../../core/trino-main/src/main/java/io/trino/operator/join/nonspilling/JoinProbe.java) отбрасывает null join keys. Наличие методов `IDENTICAL` в той же стратегии не означает, что equi-join использует их вместо `EQUAL`.

Предлагаемый безопасный контракт SMJ:

1. Ordering задаёт продвижение cursors и границы candidate group.
2. Join equality отдельно проверяет допустимость совпадения ключей; неизвестный результат трактуется как отсутствие match для обычного equi-join.
3. Residual `ON` predicate проверяется на каждой candidate pair.
4. Match flags для outer join выставляются только после успешных equality и residual checks.
5. Нужна проверка свойства: каждая пара, для которой join equality истинно, должна попадать в одну ordering-equivalence group. Иначе merge может пропустить match. Для типов без доказанного соответствия — fallback на hash.

Проверка equality может быть устранена для строго ограниченного набора типов только после доказательства эквивалентности и тестов. Для MVP разумно начать с `BIGINT`, затем расширять типы. `isOrderable()` — необходимая, но сама по себе недостаточная формальная спецификация поведения нового join.

## Duplicate groups: память и цена повторного чтения

Для ключа `k` с `Lk` и `Rk` строками результат inner join без дополнительного фильтра содержит `Lk × Rk` строк. SMJ уменьшает зависимость от памяти, но не устраняет эту нижнюю границу output work. Даже при residual filter с почти пустым результатом может потребоваться проверить все candidate pairs.

Предлагаются три варианта:

| Вариант | Плюсы | Цена / ограничения |
|---|---|---|
| Buffer right group; stream left group | Простой inner/left алгоритм, стабильная ориентация | Память зависит от максимальной правой группы, если нет spill |
| Spillable replayable right group | Ограниченная RAM; корректная обработка огромной группы | Повторное чтение группы для каждой левой строки может сильно увеличить I/O |
| Block nested replay внутри equal-key groups | Несколько левых строк используют один проход по правой группе; лучше I/O | Сложнее state machine, хранение левого чанка и outer match state; выходной порядок меняется |

Рекомендация: MVP должен иметь spillable group store и фиксированную ориентацию. Следующий шаг — bounded left chunk и один проход правого run на чанк. Это не новый алгоритм поиска ключей, а стратегия выдачи cross-product внутри уже найденной equal-key group.

**Новый контракт, которого нет в `SingleStreamSpiller`:** `seal`, `openReader`, повторное открытие reader, явное закрытие readers, хранение ключа шифрования до закрытия store, лимит числа readers, memory/spill accounting и очистка при cancel/failure. Существующий spiller остаётся пригодным для однократного потребления sort run; переименование его в group store функциональность не добавит.

FULL/RIGHT JOIN с residual predicate требуют хранить matched/unmatched состояние каждой строки буферизованной группы. Несжатый bitmap стоит `ceil(Rk / 8)` байт и тоже может превышать бюджет; нужен spillable/сегментированный контракт либо более узкая первая поддержка INNER/LEFT. Счётчики строк/пар/байтов — `long`; локальный индекс внутри `Page` может остаться `int`.

## Внешняя сортировка с ограничением fan-in

Предлагаемая схема для каждого локального join partition:

```text
input pages
  -> bounded in-memory run -> sorted spill run
  -> если runs слишком много: merge не более F runs -> новый run
  -> финальный merge не более F runs
  -> SMJ cursor
```

`F` выбирается из бюджета reader buffers, file descriptors и CPU; его нельзя задавать только количеством файлов. При `R` initial runs промежуточные merge passes дают ориентировочно `ceil(log_F R)` уровней объединения; каждый материализованный уровень читает и пишет данные. Последний merge можно подавать напрямую join, экономя дополнительную материализацию, но это требует согласованных backpressure и memory contracts двух сортировок и join.

Оценка рабочего бюджета одного SMJ instance:

```text
M = left sort/merge live buffers
  + right sort/merge live buffers
  + bounded transport queues
  + duplicate-group write/read buffers
  + left replay chunk (если включён)
  + output PageBuilder
  + metadata/codec/serde overhead
```

Здесь нельзя одновременно обещать каждому компоненту весь task memory limit. На worker итог умножается на число одновременно активных join instances; план должен учитывать concurrency. Materialized sort phases иногда упрощают resource scheduling ценой I/O и time-to-first-row.

У существующего `OrderByOperator.finish` есть отдельный риск для переиспользования: revocable память переводится в user memory; при неудаче reservation код делает дополнительный spill и синхронно ждёт future (`getFutureValue`). Во время output `startMemoryRevoke` не допускает обычного нового spill текущего `PagesIndex`. Это факт конкретной реализации, а не гарантия универсального fully-revocable external sort.

## Page ownership и горячий цикл

Предлагаемые оптимизации по порядку внедрения:

1. Проецировать только join keys, output columns и columns residual predicate до shuffle/sort. Размер строки часто важнее скорости comparator.
2. Хранить cursor как `(Page, int position)`, cached key blocks и небольшое число примитивных полей. Не создавать Java row object на каждую строку.
3. Выделять comparator/типовые операторы один раз на factory/operator, не искать method handles в inner loop; переиспользовать [`OrderingCompiler`](../../../core/trino-main/src/main/java/io/trino/sql/gen/OrderingCompiler.java).
4. Удерживать references на целую страницу только пока это выгодно; при длинноживущем малом tail делать `copyRegion`/`copyPositions`. Порог выбирать измерениями retained bytes и стоимости copy.
5. Output сначала собирать через `PageBuilder` как понятный baseline. Dictionary/RLE output рассматривать позднее: он может снизить копирование повторяющегося payload, но продлить жизнь больших backing blocks.
6. Проверять yield и output fullness с ограниченным work quantum, в том числе внутри огромной equal-key group с residual predicate, который отклоняет все пары. «Проверка при каждой выданной странице» недостаточна, если страница никогда не заполняется.
7. При переходе ownership между queue, cursor и group store гарантировать, что retained memory всегда кем-то учитывается. Двойной учёт shared blocks хуже эффективностью, нулевой — опаснее корректностью resource limits.

`MergeSortedPages.pageWithPositions` явно создаёт объекты по строкам; утверждать, что JVM гарантированно устраняет их escape analysis, нельзя. Сначала профиль allocation/CPU, затем рассмотреть merge queue из mutable per-run cursors или tournament/loser tree. Оптимизация должна сохранять корректное состояние blocked/yield и ownership страниц.

## Compact keys, radix и SIMD: отдельные эксперименты

**Compact/normalized keys.** Фиксированный префикс ключа способен ускорить сортировку строковых/составных ключей; collisions требуют fallback на полный comparator. Signed integer transforms, NaN, ±0, NULL markers, направление сортировки, precision/scale decimal и timezone semantics должны соответствовать Trino. Payload reference, пережившая удаление run/page, недопустима. Это расширение после корректного baseline, не обязательная часть MVP.

**Radix sort.** Кандидат для фиксированных numeric keys после определения order-preserving encoding. Общий lexicographic comparator остаётся fallback. Дополнительные arrays и passes входят в memory budget.

**SIMD.** Потенциально полезен для numeric key scans, обнаружения границ группы, null masks и массового копирования. Переменные длины строк, dictionary indirection, ветвление двух cursors и residual filters ограничивают ожидаемую выгоду. Руководство [Development: Vector API](../../../.github/DEVELOPMENT.md#vector-api) требует scalar equivalent, проверки hardware support и тестов на AMD/Intel/ARM; ускорение не следует предполагать без измерений.

**Off-heap.** Может изменить GC-профиль, но добавляет lifecycle и accounting. Не нужен как исходная предпосылка внешнего SMJ. Сначала следует доказать, что CPU/GC, а не disk/network/output volume, ограничивает workload.

## Проверка и benchmark plan

До оптимизаций необходим differential harness: одинаковые входные данные, Hash Join и SMJ, сравнение multiset результата. Входы сортируются тем же type ordering, который используется production path.

Обязательные cases: пустые стороны; 1:1, 1:N, N:M; группа через границы page/run; NULL; NaN и ±0; RLE/dictionary/lazy blocks; составные ключи; длинные строки; residual=true/false/NULL; left outer unmatched после отклонения всех pairs; repeated spill/replay; cancellation во время write/read/blocked; исчерпание диска; сильный skew; memory revoke в допустимых состояниях.

Для performance сравнивать:

| Измерение | Варианты |
|---|---|
| Размер build относительно доступной памяти | помещается; немного больше; многократно больше |
| Сортировка | обе стороны уже доказанно sorted; одна sorted; обе unsorted |
| Ключи | bigint; составные numeric; varchar; nullable |
| Распределение | unique; uniform duplicates; один огромный hot key |
| Payload | узкие строки; широкие projected строки |
| Ресурсы | разные sort budgets/fan-in/concurrency; disk throughput; shuffle throughput |
| Конкуренты | in-memory hash; spilling hash; SMJ full pipeline; merge-only SMJ отдельно |

Метрики: wall time, CPU, allocations/row, GC pause/allocated bytes, peak user/revocable/retained memory, spilled bytes/read amplification, sort run count/fan-in/passes, duplicate-group replay bytes, blocked/yield time, output rows, network bytes. Merge-only benchmark без shuffle и sort полезен для hot loop, но не является доказательством ускорения запроса.

Начальные образцы тестов: [`TestOrderByOperator`](../../../core/trino-main/src/test/java/io/trino/operator/TestOrderByOperator.java), [`TestMergeSortedPages`](../../../core/trino-main/src/test/java/io/trino/util/TestMergeSortedPages.java), [`TestFileSingleStreamSpiller`](../../../core/trino-main/src/test/java/io/trino/spiller/TestFileSingleStreamSpiller.java). JMH образец: [`BenchmarkPagesIndexOrdering`](../../../core/trino-main/src/test/java/io/trino/operator/BenchmarkPagesIndexOrdering.java). Команды запуска брать из [Development: Running benchmarks](../../../.github/DEVELOPMENT.md#running-benchmarks). Новые тесты/бенчмарки здесь не создавались и не запускались.

## Позиция для общего brainstorm

Рекомендую сначала корректный INNER/LEFT equi-SMJ для ограниченного набора типов, с bounded runs и spillable replay duplicate groups. Оптимизации normalized keys, SIMD, off-heap и «объединить sort+join в один сверхоператор» отложить до профилей.

Возражения к возможным решениям других ролей:

- «Переиспользуем `OrderByOperator`, значит внешняя память уже решена» — необходимо отдельно закрыть fan-in, merge memory и duplicate replay.
- «Одна строка каждой стороны — вся память merge» — неверно при duplicates и output backpressure.
- «Сортировка по SQL type гарантирует match при comparator=0» — контрпример NaN находится в текущем коде.
- «Сделаем zero-copy output» — без ownership policy маленькая output page может удержать большие input pages.
- «Hash repartition решает skew» — все одинаковые ключи остаются вместе; output complexity и replay I/O не исчезают.

Открытые вопросы к архитекторам: кто распределяет memory budget между двумя sort/merge pipelines и join; какой компонент владеет replayable store; как suspend/resume сохраняет cursors при revocation; будет ли FULL JOIN входить в MVP; как cost model учитывает потерю/сохранение sortedness и фактический объём output.
