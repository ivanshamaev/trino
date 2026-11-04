# Brainstorm: MPP engine architect

Дата: 2026-11-04. Исследован Trino `485-SNAPSHOT`, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Роль — распределённое исполнение, прогресс, skew и восстановление. Старые исследования не читались. Сначала выполнен анализ исходников; затем просмотрены три новых ролевых отчёта для cross-review. Все описанные SMJ-компоненты являются предложениями.

## Основная рекомендация

Первый распределённый SMJ следует строить поверх существующего hash repartition: одинаковые ключи обеих сторон должны попадать в одинаковые remote и local partitions, после последнего exchange каждая сторона сортируется локально. Hash здесь определяет адрес доставки строки, а merge — способ сопоставления строк. Для SMJ не обязательны range partitioning или глобальная сортировка.

Для первого промышленного этапа предпочитаю материализованные внешние runs и merge source с двумя явными partition handles. Streaming fast path для уже отсортированных входов — отдельный этап: он требует собственного доказательства прогресса scheduler. Дисковый бюджет, ограничение fan-in и spillable duplicate groups обязательны до заявления о поддержке данных существенно больше RAM.

## Что подтверждено кодом

| Участок | Факт | Значение для SMJ |
|---|---|---|
| [`AddExchanges.planPartitionedJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) | Согласует распределение двух сторон; использует partitioned exchanges и совместимые свойства источников | Полезная база co-location, но не доказательство порядка |
| [`AddExchanges.visitSort`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) | Distributed sort добавляет round-robin remote exchange, partial sort и remote merging gather; альтернативная ветвь собирает input на один node | Обычный SQL SortNode не реализует требуемую сортировку каждой join partition |
| [`AddLocalExchanges.visitJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) | Probe планируется с default parallelism, build — exactlyPartitionedOn(keys) при concurrency > 1 | Пара одинаковых локальных partition IDs обеих сторон отсутствует как контракт обычного lookup join |
| [`AddLocalExchanges.visitSort`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) | Может добавлять local merging exchange или требовать single stream | Нельзя добавлять стандартный sort pass и считать локальный параллелизм сохранённым |
| [`LocalExchange.createPartitionFunction`](../../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalExchange.java) | Для system partitioning создаёт LocalPartitionGenerator с числом partitions; connector partitioning имеет отдельное bucket mapping | Совпадение списка columns недостаточно: функция, типы и mapping также должны совпадать |
| [`LocalExchangeSource`](../../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalExchangeSource.java) | Queue страниц, учёт retained bytes, futures ожидания и WorkProcessor-адаптация | Готовый образец transport lifecycle; очередь из нескольких producers сама не сортирует страницы |
| [`PhasedExecutionSchedule.processJoin`](../../../core/trino-main/src/main/java/io/trino/execution/scheduler/policy/PhasedExecutionSchedule.java) | Добавляет зависимости probe upstream stages от завершения build upstream stages | Build/probe асимметрия проникает за пределы join operator |
| Тот же класс, `unblockStagesWithFullOutputBuffer` | Запускает непосредственные downstream stages заблокированных fragments | Нельзя утверждать неизбежный deadlock текущего scheduler; новый граф ожиданий надо проверять отдельно |
| [`EventDrivenFaultTolerantQueryScheduler.taskFailed`](../../../core/trino-main/src/main/java/io/trino/execution/scheduler/faulttolerant/EventDrivenFaultTolerantQueryScheduler.java) | Запрашивает следующий memory requirement; при невозможности увеличить reservation после OOM может завершить stage ошибкой | FTE не делает неограниченно большую hash partition автоматически исполнимой |
| [`AdaptiveReorderPartitionedJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java) | В FTE меняет ориентацию partitioned join по runtime size | Это не общий выбор SMJ и не обработка hot key |

Особенно важно не переоценивать существующий FTE splitting. [`HashDistributionSplitAssigner`](../../../core/trino-main/src/main/java/io/trino/execution/scheduler/faulttolerant/HashDistributionSplitAssigner.java) содержит `createTaskPartition`, способный делить крупнейший source и реплицировать остальные. Однако production `create` передаёт `canSplit = _ -> fragment.getPartitioning().isScaleWriters()`. Для обычного `FIXED_HASH_DISTRIBUTION` join этот путь нельзя описывать как доступный общий skew-join splitter. `assign` действительно раздаёт splits выбранного source между subpartitions, а остальные копирует всем; применимость ограничена вызывающим контрактом.

## Два уровня partitioning и доказательство корректности

Предлагаемая топология:

```text
L -> project/filter -> remote hash(KL, remoteMapping)
                   -> local hash(KL, P, localMapping) -> sort L[i] -> handle L[i]

R -> project/filter -> remote hash(KR, remoteMapping)
                   -> local hash(KR, P, localMapping) -> sort R[i] -> handle R[i]

matching task + matching i: merge(handle L[i], handle R[i]) -> downstream
```

Remote partition ID и local partition ID — разные сущности. Если FTE объединяет несколько remote partitions в один task, это не нарушает equijoin само по себе, но оба inputs должны пройти одинаковое объединение и последующую сортировку. Нельзя считать пришедший набор materialized exchange splits уже одним sorted stream.

Достаточные инварианты для базового equijoin:

1. Для каждой пары строк с истинным join equality их normalized keys дают одинаковые remote destination и local partition ID.
2. Каждый input row доставляется в единственный join instance в базовом partitioned режиме. Broadcast/skew replication требует отдельного доказательства, особенно для outer unmatched rows.
3. Каждый instance получает все строки назначенной ему левой и правой partitions; произвольное распределение страниц между consumers запрещено.
4. Вход каждого instance монотонен по одному согласованному key comparator. Если имеется несколько sorted runs, их сначала объединяет корректный k-way merge.
5. После sort нет разрушающего порядок exchange, adaptive shuffle или перераспределения drivers.
6. Равенство сортировки используется для candidate groups; фактические equality и residual semantics проверяются отдельно.

Глобальный порядок результата не обещается. Упорядоченность внутри hash partition не означает `ORDER BY` всего запроса. Для LEFT/RIGHT/FULL выводимые NULL-строки также требуют отдельного вывода ordering properties.

## Hash, range и storage co-location

| Вариант | Сильная сторона | Цена и ограничение | Решение |
|---|---|---|---|
| Hash partition + local sort | Переиспользует знакомую MPP доставку; естественная пара partitions | Не разделяет одинаковый hot key; требует локальной сортировки после обмена | MVP |
| Range partition + local sort | Может согласоваться с downstream order, range pruning и storage order | Общие splitters обеих сторон, sampling, handling равных boundary keys, skew, новый exchange контракт | Поздний отдельный проект |
| Совместимая storage partitioning + ordered scan | Потенциально устраняет shuffle и/или sort | Нужны доказательства обоих свойств, включая interleaving splits и streams | После базового SMJ |
| Single-node gather + merge | Простой correctness prototype | Сетевой и CPU bottleneck, не решает MPP масштабирование | Только тестовый стенд |

Range partitioning не исправляет hot key автоматически: все равные ключи обычно должны остаться в одной partition. Если разделить их на границе range без репликации другой стороны, потеряются пары. Если обе стороны независимо разрезать на одинаковое число chunks, соединение только соответствующих chunks также потеряет off-diagonal пары.

## Skew: три разных проблемы

**Data skew между partitions.** Несколько partitions получают намного больше байтов. SMJ с внешней сортировкой способен удержать RAM, но самый большой instance остаётся straggler. Измерять maximum/median partition bytes и sort time. Увеличение числа hash partitions помогает только если skew вызван многими разными ключами.

**Одна enormous equal-key group.** Hash/range repartition по исходному ключу не разбивает её. Нужен spillable replay, а для performance — chunked replay. При группах размеров `a` и `b` candidate work достигает `a*b`, даже если residual почти всё отвергает. Это предел вычисления результата, а не недостаток конкретной реализации сортировки.

**Слишком широкий output.** Даже небольшой input после many-to-many join может создать огромный output. Sort memory robustness не ограничивает output bytes, network или downstream aggregation. Benchmark должен отдельно показывать input/candidate/output cardinalities.

Поздняя skew-стратегия для INNER: разрезать большую группу одной стороны на chunks и реплицировать меньшую группу каждому chunk consumer. Это сохраняет все пары, если chunks не пересекаются. Цена — повторное чтение/передача меньшей группы; для FULL/RIGHT требуется координация unmatched flags, иначе строки ошибочно или многократно выдаются как unmatched. Простое случайное salting обеих сторон некорректно. Не включать эту стратегию в MVP без отдельного протокола и тестов.

## Progress и scheduler

У merge есть симметричная потребность в двух головах streams. Текущий lookup lifecycle предполагает полное потребление build. Поэтому one-sided queue с backpressure создаёт новый граф ожиданий: producer ждёт capacity, consumer ждёт вторую сторону, её stage может ещё ждать dependency.

Это **проектный риск**, не доказанная ошибка существующего Trino. `unblockStagesWithFullOutputBuffer` снимает часть зависимостей; нужно проверить, видит ли scheduler новый bridge blockage как task output blockage, и запускает ли именно недостающую сторону, а не уже работающий merge stage.

Материализация сортированных runs полезна для первого этапа: producer завершает ingestion независимо от потребления merge, публикует handles и освобождает основной run buffer. Это не разрешение накопить все runs/metadata без ограничения. Возможен disk-pressure blockage, поэтому disk quotas и metadata bounds входят в контракт.

Для presorted streaming path нужен algorithm-aware scheduler, где обе upstream branches могут запуститься. Разбудить stage недостаточно, если all runnable driver slots заняты pipeline, ждущими друг друга; blocked operators должны освобождать выполнение и иметь правильные futures. Нельзя ждать future завершения обоих inputs для шага, которому нужна лишь одна конкретная страница.

Dynamic filters тоже могут создать dependency: source ждёт build filter, а build stream не завершается из-за bounded bridge. В первой реализации предпочтительно отключить именно неподдерживаемое ожидание dynamic filter для SMJ либо завершать bounded filter collection до начала merge. Переносить `waitForBuild` из lookup path без аудита нельзя. Семантическая допустимость фильтрации зависит от join type: нельзя отбрасывать preserved unmatched сторону outer join.

## FTE и владение временными данными

Предлагаемый контракт первой FTE реализации: local sort runs и duplicate stores принадлежат одной task attempt. При retry они создаются заново из восстановимых upstream exchange данных. Не выдавать local spill files за durable exchange и не передавать их paths другой попытке на другом worker.

Нужно различать:

- exchange storage: межстадийные данные, используемые текущим retry-протоколом;
- local sort runs: результат внешней сортировки внутри попытки;
- duplicate-group store: локально переоткрываемая группа для relational matching;
- downstream output publication: подчиняется механизму выбора успешной task attempt.

Для SMJ не следует самостоятельно публиковать результаты нескольких попыток как независимые streams: это создаёт дубликаты. Первая интеграция должна остаться в существующем task/exchange commit lifecycle. Checkpointing курсоров внутри merge не нужен для базового retry whole-task, но пересортировка может быть дорогой; durable sorted shuffle — отдельное улучшение с comparator/version metadata и cleanup semantics.

Adaptive reordering тоже требует guard: после выбора SMJ смена сторон должна сохранить все key mappings, ordering, partition identity и preserved-side semantics. Существующее правило рассчитано на JoinNode и локальные exchange patterns lookup join; его совместимость нельзя предполагать.

## Ресурсная модель

Для worker недостаточно `M_join <= limit`. При `C` одновременно активных join instances:

```text
M_worker >= other operators + exchange buffers
          + sum_i(sortL_i + sortR_i + mergeHeads_i + groupStore_i
                  + replayBuffers_i + output_i + metadata_i)
```

Внешняя сортировка позволяет делить input на runs, но fan-in, открытые readers и manifests должны быть ограничены. Бюджет task следует делить между phases/instances; увеличение local concurrency одновременно увеличивает число buffers и конкуренцию за disk. Не назначать каждой партиции одинаковый большой memory cap без общей admission policy.

Disk budget должен включать живые старые и новые runs во время intermediate merge, duplicate store, одновременные tasks и retries. Данные могут быть записаны и в durable exchange, и в local runs, а затем повторно в group store. Формула «один размер input на диске» не является безопасной оценкой peak footprint.

## Cross-review трёх первых ролей

1. **Java developer: два sinks + merge source. Согласен с интерфейсной формой, уточняю первый transport.** Для MVP large-data предпочитаю публикацию completed run handles, а не только bounded page queues. Симметрия queues сама не доказывает scheduler progress. Streaming queue path оставить отдельным milestone. [Позиция роли](01-java-developer.md).
2. **Java architect: отдельный physical node и partition-local ordering. Согласен.** Отдельный node облегчает algorithm-aware scheduler и исключение hash-specific rules, но требует явной FTE split/assignment обработки. `P` одинаков с двух сторон — необходимо, однако недостаточно без одинаковых hash/coercion/bucket mappings. [Позиция роли](02-java-architect.md).
3. **Java optimization: hash repartition не устраняет skew. Полностью согласен, добавляю ограничение FTE.** Текущий split assigner нельзя считать общей готовой skew-join инфраструктурой: production predicate разрешает splitting для scale writers. Поэтому отдельная задача hot-key parallelism должна остаться открытой. [Позиция роли](03-java-optimization.md).
4. **Предложение bounded-memory RIGHT/FULL. Поддерживаю перенос после MVP.** Помимо bitmap spill есть распределённая проблема unmatched ownership при replication/skew splitting. Локально корректный FULL join ещё не доказывает correctness реплицированной распределённой схемы.
5. **Общий вопрос memory budget. Предлагаю phase-aware task budget.** Run-generation и final merge не всегда живут одновременно; ограничение phases сокращает peak RAM ценой I/O/latency. Это более проверяемый baseline, чем надежда на независимые revocations всех buffers.

## Приёмка и открытые решения

До distributed MVP требуются тесты с двумя и более workers/tasks, несколькими local partitions, совпадением ключей через разные splits/pages/runs, пустой стороной, skew и отменой. Проверять SQL multiset, а не порядок строк.

До FTE поддержки: task failure во время sort, duplicate spill, final merge и output publication; корректное повторение без пропущенных/повторных результатов; cleanup attempt files. До presorted fast path: scheduler regression с tiny queues, delayed branch, nested joins, dynamic-filter wait и downstream LIMIT. До обещания устойчивости памяти: большой partition при малом RAM, тысячи runs, один huge group, ограниченные descriptors/disk, несколько concurrent queries.

Открытые решения: физический node или algorithm field; распределитель общего memory budget; run manifest ownership; правила динамических фильтров; capability gating FTE; допускается ли материализация final sorted output или final merge будет fused с join; целевой профиль I/O и SLA для hot keys. Эти вопросы требуют prototype и измерений, а не только архитектурного согласования.
