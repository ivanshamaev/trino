# Brainstorm: Java architect

Дата исследования: 2026-11-04. База: Trino `485-SNAPSHOT`, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Это независимая архитектурная позиция, а не утверждённый дизайн. Каталоги предыдущих исследований не читались. Ниже **факт** означает наблюдение в текущем исходном коде; **предложение** — проектирование будущего механизма.

## Главный вывод

Добавить класс merge-оператора недостаточно. Сейчас планирование JOIN содержит асимметрию build/probe, а планирование SORT обеспечивает сортировку другого масштаба, чем требуется параллельному SMJ. Главный архитектурный контракт — две стороны одинаково распределены между tasks и между локальными join partitions, и внутри каждой такой пары полностью упорядочены по согласованным ключам.

Предлагаю начать с partitioned equi-SMJ, внешней сортировки после последнего repartition и отдельного физического узла, вводимого после логических преобразований. Выбор нового узла — обсуждаемое решение; поле алгоритма в `JoinNode` остаётся допустимой альтернативой при систематическом аудите существующих правил.

## Что уже есть в ядре

| Область | Проверенный факт | Следствие для SMJ |
|---|---|---|
| Логический join | [`JoinNode`](../../../core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java) хранит тип, equi clauses, residual filter, distribution, spillable, dynamic filters; поля алгоритма нет | Distribution и algorithm — разные решения; `PARTITIONED` не означает hash как алгоритм локального сопоставления |
| Физический join | [`LocalExecutionPlanner.visitJoin/createLookupJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java) использует nested loop для cross join и lookup join для остальных `JoinNode` | Новый алгоритм требует отдельной ветви построения физических pipelines |
| Remote partitioning | [`AddExchanges.planPartitionedJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) согласует partitioning обеих сторон, может использовать совместимые connector bucket schemes | Эта логика полезна SMJ, но гарантирует co-location, не порядок |
| Local partitioning | [`AddLocalExchanges.visitJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) задаёт probe default parallelism, build exactly-partitioned по join keys при concurrency > 1 | SMJ не может наследовать эту асимметрию: обе стороны должны попадать в один и тот же локальный partition |
| Unary runtime API | [`Operator`](../../../core/trino-main/src/main/java/io/trino/operator/Operator.java) принимает один `addInput(Page)`, имеет `needsInput/isBlocked/getOutput` | Два inputs надо соединить через отдельные drivers и bridge либо расширять runtime |
| Join lifecycle | [`JoinBridgeManager`](../../../core/trino-main/src/main/java/io/trino/operator/join/JoinBridgeManager.java) реализует build/probe/outer lifecycle, destruction после завершения использования | Полезен как образец владения ресурсами, но готовую lookup lifecycle нельзя автоматически считать симметричной |
| Sort | [`LocalExecutionPlanner.visitSort`](../../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java) создаёт `OrderByOperatorFactory`, передаёт настройку spill | Можно переиспользовать сортировочную реализацию, отдельно проверив memory bounds и run fan-in |
| Ordered exchange | [`ExchangeNode`](../../../core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java) разрешает merging exchange только `GATHER`; REMOTE требует `SINGLE_DISTRIBUTION`, LOCAL — passthrough | Готового ordered partitioned remote exchange здесь нет |
| Local merge | [`LocalExecutionPlanner.createLocalMerge`](../../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java) явно устанавливает один output driver | Использовать его как универсальную локальную SMJ-параллельность нельзя |

## Ловушка обычного SortNode

**Факты.** [`AddExchanges.visitSort`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) для distributed sort строит round-robin remote exchange, частичную сортировку и remote merging gather. Без distributed sort собирает данные на один node. [`AddLocalExchanges.visitSort`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) при parallel sort добавляет local merging exchange, иначе требует single stream. Наличие `partial` у [`SortNode`](../../../core/trino-main/src/main/java/io/trino/sql/planner/plan/SortNode.java) само по себе не делает произвольно вставленный узел безопасной сортировкой каждой join partition.

**Следствие.** План `Join(Sort(left), Sort(right))`, помещённый перед обычными exchange passes, может привести к сборке потоков и потере нужного partitioning. Если сортировать до последнего hash exchange, следующий exchange обычно уничтожит порядок. Правильный контракт: `partition -> local partition -> sort within partition -> merge`, без repartition между sort и merge.

**Предложение.** Явно представить сортировку каждой join partition: отдельным узлом/режимом с особыми обработчиками в `AddLocalExchanges`, либо как физический контракт `SortedMergeJoinNode`, который local planner разворачивает в sort operators. Второй путь сокращает первое изменение, но EXPLAIN должен показывать фактически выполняемые sort и spill, иначе стоимость алгоритма станет невидимой. Не переопределять значение обычного SQL `ORDER BY`.

## Три варианта представления алгоритма

| Вариант | Плюсы | Цена и риски | Вердикт |
|---|---|---|---|
| Только выбор в `LocalExecutionPlanner` | Небольшой diff для лабораторного prototype | Уже поздно задавать remote distribution, local partitioning и ordering; CBO и EXPLAIN не отражают выбор; implicit sorts | Только стендовый прототип с заранее подготовленными inputs |
| Поле `JoinNode.algorithm` | Сохраняет существующие logical rewrites и statistics; выбор виден в serialized plan | Все места создания/copy/flip `JoinNode` должны переносить или инвалидировать поле; множество правил содержит hash-specific assumptions | Рабочий путь при явной фазе окончательного выбора и guard'ах |
| Отдельный `SortedMergeJoinNode`, введённый поздно | Явные инварианты и отдельные visitors; hash-specific rules перестают случайно матчиться | Новый JSON subtype, visitors, stats/cost, sanity checks, EXPLAIN, scheduler handling; нет бесплатного полноценного logical/physical layer | Предпочтение этой роли для долгосрочного дизайна |

Раздельный узел не означает копирование SQL-семантики: общий immutable join specification/utility может хранить clauses, output mapping и filter. Но вводить абстрактную иерархию всех join operators ради одного MVP преждевременно.

### Предлагаемый ADR

1. Логические перестановки JOIN продолжают работать с `JoinNode`.
2. После reordering и определения допустимости выбирается physical algorithm. Для MVP — принудительный экспериментальный выбор на поддерживаемом подмножестве, а не автоматический эвристический выбор «при большом числе строк».
3. Выбор SMJ создаёт `SortedMergeJoinNode` до построения окончательных exchange contracts. Поля: type, left/right, упорядоченный список equi key pairs, residual, output symbols, distribution, требуемые ordering, политика локального parallelism. Это предлагаемый API, его сейчас нет.
4. `AddExchanges` согласует remote partitioning; `AddLocalExchanges` согласует локальные partition IDs и добавляет sort после local exchange. Необходимо исключить последующее разрушение этих контрактов правилами.
5. `LocalExecutionPlanner` материализует два producer pipelines и merge consumer, а не переписывает физический алгоритм по собственной эвристике.
6. Sanity checker непосредственно перед execution проверяет парность partitioning и ordering. Недоказанную упорядоченность считать отсутствующей.

Порядок passes надо закрепить тестами по фактическому [`PlanOptimizers`](../../../core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java): `DetermineJoinDistributionType` сейчас раньше `AddExchanges`, а `UseNonPartitionedJoinLookupSource` — после `AddLocalExchanges`. Новый выбор не должен оказаться между passes, которые молча отменяют его инварианты.

Если команда выберет поле в `JoinNode`, требуется тот же ADR для момента фиксации и инвалидирования алгоритма. В частности, [`UseNonPartitionedJoinLookupSource`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java) заменяет build local repartition на gather для малой стороны; такое правило надо исключить для SMJ или доказать новое соответствие partitions.

## Предлагаемый физический граф

```text
left scan  -> remote hash exchange(K_L) -> local hash exchange(K_L, P)
                                           -> external sort(partition i) --+
                                                                        |
right scan -> remote hash exchange(K_R) -> local hash exchange(K_R, P)     |
                                           -> external sort(partition i) -> SMJ[i] -> output
```

Параметр `P` одинаков с обеих сторон; значение `i` — явный identity partition, а не случайный номер созданного driver. Remote partition count может отличаться от local `P`. Равенство ключей должно означать равенство remote destination и local partition при выбранных hash functions/coercions. Внутри одной hash partition нужна сортировка всего её потока, но глобальная упорядоченность между partitions не нужна.

### Bridge без переделки Driver

**Предложение A:** одна сторона входит в unary join operator как обычный input; вторая публикуется producer'ом через `PartitionedSortedInputBridge`. Join operator читает правый cursor через bridge. Он может принимать левую page только при наличии места; `isBlocked` отражает ожидание нужной стороны, I/O или output capacity. Producer имеет отдельный future на освобождение capacity.

**Предложение B:** оба producers публикуют cursors/run manifests, а merge — source operator без upstream input. Симметрия лучше и проще явно сопоставить partitions, но нужны дополнительные drivers, memory ownership и careful wakeup.

Для первого промышленного прохода предпочитаю B с публикацией handles на завершённые external runs; это естественно отделяет ingestion/sorting от merge и снижает зависимость от build/probe lifecycle. Для presorted inputs позднее можно публиковать bounded streaming channels. Run manifest не должен означать, что сами строки хранятся в памяти.

Bridge обязан иметь `finishSide`, `fail`, `close`, idempotent destruction, ownership spill files, futures для producer/consumer, отслеживание всех partitions и ограничение retained pages. Внешние сортировки, bridge и duplicate-group storage должны делить общий memory budget: три независимо ограниченных компонента всё ещё способны переполнить task memory в сумме.

### Scheduling — часть корректности

В [`PhasedExecutionSchedule.processJoin`](../../../core/trino-main/src/main/java/io/trino/execution/scheduler/policy/PhasedExecutionSchedule.java) явно добавляются зависимости запуска probe upstream stages от завершения build upstream stages. Код также содержит механизм разблокировки stages с полными output buffers, поэтому **нельзя утверждать, что существующий scheduler обязательно зависнет**.

Однако новый bounded channel может создать цикл: build не может опубликовать очередную страницу без потребителя, merge ждёт probe, а probe ещё не запущен. Этот цикл особенно важен для presorted fast path, где нет поглощающей весь input внешней сортировки. Нужен отдельный обработчик нового node или algorithm-aware обработка: обе стороны доступны для запуска, ресурсные ограничения не делают одну сторону недостижимой. Необходим regression test с маленькими buffers, несколькими stages, исчерпанием driver concurrency и отменой запроса.

Полная материализация сортированных runs перед merge — возможный первый компромисс. Она увеличивает latency/I/O, но даёт явную границу lifecycle. Нельзя затем убрать materialization для fast path, не пересмотрев доказательство progress.

## Доказательство порядка и возможность reuse

[`PropertyDerivations.visitSort/visitExchange`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PropertyDerivations.java) отражает local sort properties и порядок merging exchange. [`StreamPropertyDerivations`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/StreamPropertyDerivations.java) различает single/fixed/multiple streams, partition columns и ordered exchanges. Эти механизмы полезны, но SMJ требует совместного доказательства свойств двух inputs.

Для пропуска sort должны выполняться все условия:

1. Доказана co-location равных join keys на task.
2. Доказано одинаковое распределение по локальным merge instances.
3. У каждого input именно полный логический поток partition, либо выполнен корректный k-way merge всех его sorted substreams.
4. Сортировочные ключи имеют подходящий prefix, одинаковые направления и null ordering с обеих сторон.
5. Приведения типов и expressions совпадают с equi-clause semantics. Произвольный cast не объявляется order-preserving автоматически.
6. После установленного порядка нет разрушающего его exchange, parallel interleaving или оператора.

`ConnectorTableProperties.getLocalProperties()` — источник возможных доказательств от connector, а не разрешение выводить глобальный порядок из sort order в метаданных файлов. Два отсортированных файла, прочитанные вперемешку, не образуют отсортированный поток. Даже если каждый файл ordered, необходимо знать границы диапазонов либо выполнить merge.

Не обещать порядок результата любого SMJ автоматически. INNER/LEFT могут сохранять подходящее ordering на соответствующих output keys при выбранной реализации. Для FULL JOIN строки без совпадения имеют NULL на одной стороне; порядок объединённого ключа не равен порядку отдельно `left.key` или `right.key`. Начальный property derivation должен быть консервативным.

## Семантические ограничения архитектуры

- Сортировочное равенство должно согласовываться с SQL equality на допустимых types; NULL, NaN, signed zero, decimal coercions и composite keys требуют отдельных тестов. `Type.isOrderable` само по себе не заменяет доказательство сравнения для join.
- Равные ключи дают декартово произведение групп. Внешняя сортировка не ограничивает память группы; duplicate group spool/rewind — обязательная часть bounded-memory контракта.
- Residual predicate вычисляется для каждой кандидатной пары. Для outer join «был равный ключ» не означает «было совпадение после residual»; отметки совпадения нужны по строкам.
- Dynamic filters сегодня привязаны к right/build. [`LocalExecutionPlanner.createLookupJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java) может ждать build ради фильтра. Для streaming SMJ такая задержка — отдельное решение. В MVP отключить генерацию именно таких dynamic filters согласованным planner pass, а не молча игнорировать уже созданные filter consumers.
- Стоимость [`CostCalculatorUsingExchanges.visitJoin`](../../../core/trino-main/src/main/java/io/trino/cost/CostCalculatorUsingExchanges.java) ориентирована на lookup join. SMJ требует стоимости двух sorts, spill I/O, merge, output и skew. Пока нет измерений, автоматический выбор ненадёжен.

## MVP и критерии принятия

Первый correctness prototype: INNER equi-join без residual, partitioned distribution, небольшой явно протестированный набор orderable scalar types, обязательная сортировка обеих сторон после repartition; один merge instance на task допустим как этап функциональной проверки. Это ещё не цель по масштабируемости.

Следующий обязательный этап: несколько paired local partitions на task, внешняя сортировка, spillable duplicate groups, отмена/очистка, измеряемый memory ceiling при обеих сторонах больше доступной памяти. Затем residual и LEFT; RIGHT/FULL, reuse existing ordering, dynamic filtering и retry integration проходят отдельные gates.

Принятие архитектуры требует:

- Plan tests подтверждают отсутствие глобального gather и сохранение парных partitions при `task_concurrency > 1`.
- Plan tests показывают точное место sort относительно всех exchanges и корректную serialization/deserialization узла.
- Semantic tests сравнивают результат с существующим hash join как multiset, включая duplicates, nulls, пустые inputs и page boundaries.
- Progress tests проверяют разные порядки готовности сторон, blocked I/O, memory revocation, маленькие queues, producer failure и query cancellation.
- Spill tests проверяют oversized equal-key group, ограниченный merge fan-in, очистку temporary files и суммарное использование памяти всех компонентов.
- Профилирование доказывает, что local parallelism увеличивает полезную пропускную способность, а не создаёт скрытый single-driver merge.

## Возражения другим ролям и вопросы для общего brainstorm

1. «Переиспользуем SortNode и всё заработает» — необходим анализ обоих exchange passes; обычный SORT имеет другую область упорядоченности.
2. «Bridge уже есть у hash join» — reference counting переиспользуем, но симметричный progress и partition pairing надо проектировать отдельно.
3. «Sorted table позволит убрать всю сортировку» — сначала доказать merged input ordering после scheduling файлов и exchanges.
4. «Выберем алгоритм только на worker» — без подготовленного physical contract worker не может безопасно исправить distribution и order.
5. Открытый выбор: отдельный node против поля algorithm. Стоимость нового visitor surface надо сопоставить с риском silent loss of algorithm у всех существующих `new JoinNode`/rewrites.
6. Открытый выбор: fully materialized run bridge для первого release против сразу streaming bridge. Первое проще для lifecycle, второе выигрывает на presorted inputs, но требует scheduler/progress доказательства с первого дня.

Проверки в этой роли: чтение исходников и статический анализ; код оператора, runtime эксперименты и benchmarks не выполнялись. Предложения о progress, performance и будущих API требуют экспериментальной проверки.

## Второй раунд: cross-review и согласованные решения

Прочитаны новые отчёты [Java developer](01-java-developer.md), [Java optimization](03-java-optimization.md) и [MPP architect](04-mpp-architect.md), а также главы об архитектуре и памяти. Решения ниже уточняют первоначальную позицию после обсуждения.

1. **Для MVP выбрать sealed run manifests, очереди оставить следующим этапом.** Поддерживаю MPP architect и возражаю против двух streaming queues как первого промышленного transport. Каждый producer должен завершить ingestion и опубликовать handle независимо от того, начал ли merge читать данные. Barrier действует для конкретной пары локальных partitions, а не требует ожидания всех workers запроса. Handle содержит ограниченное число итоговых runs после промежуточного merge; финальный k-way merge можно читать непосредственно в join, не материализуя ещё один полный файл. Это сохраняет bounded fan-in и не превращает manifest в неограниченный список. Простая очередь после существующего `OrderByOperator.getOutput()` не реализует этот контракт: нужен доступ к lifecycle внешних runs и явное владение ими.
2. **Поле algorithm в `JoinNode` допускаю для ограниченного MVP; предпочтение отдельного физического узла сохраняю для долгосрочного дизайна.** Согласие условно: окончательный выбор должен иметь одну фазу, все copy/flip/reconstruction paths сохраняют или инвалидируют значение, lookup-only и adaptive rules имеют guard, final sanity checker проверяет контракты. Без такого аудита меньший diff поля — ложная экономия. Отдельный node даёт более явную границу и сохраняется как альтернативный ADR; consensus о безусловном превосходстве поля отсутствует.
3. **Partition-local sort принадлежит физическому плану после последнего local repartition.** Независимо от representation join algorithm, обычный `SortNode` без специальной обработки непригоден. Для MVP приемлемо разворачивать partitioned sort producers внутри новой ветви local planner, если их стоимость, spill и расположение видны в EXPLAIN/метриках и sanity checks. Для явных sort nodes нужно отдельное обозначение scope и обработка обоих exchange passes. Никакого автоматического sort reuse по storage metadata на первом этапе.
4. **Materialization не заменяет бюджетирование и progress tests.** Принимаю замечания Java developer и optimization: manifest memory, fan-in, duplicate replay, retained pages и одновременные sorts должны иметь общий task-level budget. Если готовый manifest удерживает memory-only run до появления второй стороны, он всё ещё способен заблокировать её сортировку; нужно гарантированное освобождение run buffer посредством spill либо заранее зарезервированный минимум для обеих сторон. Отмена обязана закрывать handles и разбудить ожидающие producers/consumer.
5. **Расширить критерии correctness до candidate equality и outer state.** Замечание optimization о NaN показывает, что сортировочная группа — только группа кандидатов. Поддерживаю BIGINT INNER как минимальный gate, затем equality-aware scalar types и residual, далее LEFT. FULL/RIGHT откладываются до bounded match tracking. Это влияет и на planner eligibility: `isOrderable()` без доказательства согласованности ordering/equality недостаточно.

Review общих глав: описание пути planner, асимметрии локальных partitions, hash unspill и однократного чтения `FileSingleStreamSpiller` соответствует проверенным исходникам. Формулировку «FTE дробит работу на большее число partitions» следует читать как adaptive partition planning, а не как рекурсивное разбиение уже выпавшей по OOM join partition: ограничение `HashDistributionSplitAssigner` отдельно обнаружено MPP architect. Дополнительный полезный факт для FTE: `SystemSessionProperties.isDistributedSortEnabled` возвращает false для `RetryPolicy.TASK`; существующий distributed SQL sort нельзя объявлять готовым FTE-safe sorter для SMJ.
