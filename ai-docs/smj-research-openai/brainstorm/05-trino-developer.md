# Brainstorm: Trino developer / maintainer

Роль — сопровождение ядра и review будущего изменения. Исследован commit `e112ffdf7303`, Trino `485-SNAPSHOT`. Это пятая отдельная агентская роль: сначала изучены исходники planner/operator/tests, затем выполнен cross-review четырёх новых записок. Каталоги прежних исследований не читались. Измерений производительности и реализации SMJ в этой работе нет.

## Что определяет стоимость внедрения

Основной merge-loop относительно локален. Самый большой риск — нарушить инварианты существующего планирования и lifecycle при добавлении физического алгоритма, который должен получать два согласованно упорядоченных входа.

| Факт текущего кода | Значение для изменения |
|---|---|
| [`JoinNode`](../../../core/trino-main/src/main/java/io/trino/sql/planner/plan/JoinNode.java) имеет SQL type, criteria, residual filter, distribution, spillable и dynamic filters; отдельного algorithm нет | Семантика и физические атрибуты уже частично объединены в одном узле |
| `JoinNode.flipChildren()` сбрасывает dynamic filters | Переворот сторон требует инвалидирования физических свойств; одного копирования поля algorithm недостаточно |
| [`PlanOptimizers`](../../../core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java) выполняет reordering, distribution selection, connector pushdown, remote exchanges, дополнительные rewrites, local exchanges, consumer optimizations, partial aggregation и dynamic filter source passes | Нет одной существующей границы «все логические преобразования закончились, дальше только исполнение» |
| [`AddLocalExchanges.visitJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddLocalExchanges.java) отдельно задаёт build partitioning и probe parallelism | SMJ должен изменить contract для обеих сторон, а не только заменить operator factory |
| [`UseNonPartitionedJoinLookupSource`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UseNonPartitionedJoinLookupSource.java) после local exchanges заменяет build repartition на gather при малом build | Hash-specific правило способно разрушить парность SMJ partitions |
| [`AdaptiveReorderPartitionedJoin`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AdaptiveReorderPartitionedJoin.java) при TASK retry может переворачивать JoinNode и исправляет local exchanges под build/probe topology | Адаптивный план требует такой же проверки SMJ-контрактов, как первоначальный |
| [`Operator`](../../../core/trino-main/src/main/java/io/trino/operator/Operator.java) предоставляет один `addInput(Page)` | Двухвходовой алгоритм должен интегрироваться через pipelines/bridge, либо потребовать более широкого изменения execution API |

Практическая рекомендация: до оптимизаций реализовать явный validator physical requirements. Для каждого SMJ он проверяет key types, одинаковую схему partitioning двух входов, соответствие локальных partition IDs и отсутствие потери ordering после sort. Без validator перестановка одного optimizer pass может превратиться в тихую потерю или дублирование результатов.

## JoinNode.algorithm или новый SortedMergeJoinNode

Оба варианта допустимы, но оба требуют полноценного planner audit.

**Предпочтение этой роли для первого интегрированного MVP — algorithm field в JoinNode плюс явный контракт требований.** Это сохраняет статистики, traversals, JSON join semantics и существующую инфраструктуру assertions. Поле не должно устанавливаться произвольной worker-side эвристикой. Выбор фиксируется в planner до формирования нужных exchanges; правила после этой точки обязаны сохранять или инвалидировать physical requirements. `UseNonPartitionedJoinLookupSource` и `AdaptiveReorderPartitionedJoin` получают guard или SMJ-aware ветвление. Все constructors/copy/flip helpers должны явно передавать либо сбрасывать algorithm и связанные свойства.

Предлагаемые `JoinAlgorithm`, `SortedInputRequirements` и validator пока не существуют. В MVP достаточно принудительного режима для подходящих joins; SQL type/distribution остаются независимыми осями. Название свойства и публичный API выбирать отдельным review, не закреплять случайное имя из лабораторного прототипа.

**Отдельный SortedMergeJoinNode — сильная альтернатива**, если команда готова оплатить visitor surface ради изоляции hash-specific rules. Но его нельзя просто добавить после reordering и считать остальную цепочку безопасной:

- [`PlanNode`](../../../core/trino-main/src/main/java/io/trino/sql/planner/plan/PlanNode.java) регистрирует JSON subtypes и прямо напоминает об обновлении Web UI child traversal.
- [`ValidateDependenciesChecker`](../../../core/trino-main/src/main/java/io/trino/sql/planner/sanity/ValidateDependenciesChecker.java) в общем `visitPlan` выбрасывает unsupported exception; требуется явная проверка нового node.
- [`RemoveUnsupportedDynamicFilters`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/RemoveUnsupportedDynamicFilters.java), `visitJoin`, определяет разрешённые consumers и переписывает JoinNode. Новый node требует equivalent traversal либо сознательного отказа от соответствующих filters.
- [`AddDynamicFilterSource`](../../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AddDynamicFilterSource.java), cost/statistics, EXPLAIN, symbol mapping, build-side traversal и schedulers тоже должны узнать новый узел.

Поэтому решение должно приниматься по размеру проверяемого изменения и риску нарушений, а не по эстетике названия класса. Долгосрочная отдельная physical representation привлекательна, но создавать полноценный новый planner layer только ради SMJ не предлагается.

## Bridge: materialized runs как первый production baseline

Согласен с source-подобным merge operator, который не меняет `Driver` и читает два источника. Для первого большого-data milestone предпочитаю **два producer pipelines с публикацией handles на завершённые bounded external runs**, а не две очереди бесконечных sorted pages.

Run handle содержит partition identity, ordering/type descriptor, row/byte statistics и ownership. Manifest передаёт ссылки на временные файлы/ограниченные buffers, а не удерживает все строки. Число manifest entries также не должно неограниченно расти в RAM: sorter обязан выполнять intermediate merge, ограничивать fan-in и освобождать consumed metadata. Final k-way merge можно соединить с SMJ без обязательной записи одного полностью слитого файла, если это не нарушает lifecycle и лимиты readers.

Преимущество — ingestion каждого producer способен закончиться без потребления merge output, пока есть разрешённое временное дисковое пространство. Это упрощает прогресс в build-first scheduling. Цена — более высокая first-row latency, spill I/O и неполное использование presorted inputs. Это осознанный baseline, а не утверждение о максимальной эффективности.

Две bounded queues остаются полезным следующим вариантом для уже упорядоченных источников. Перед его включением нужны scheduler tests с задержанной стороной, tiny buffers, nested joins, downstream LIMIT, ограничением driver concurrency и memory revocation. Build-first scheduling нельзя ни игнорировать, ни объявлять гарантированным deadlock без учёта существующей разблокировки stages.

## Dynamic filtering и FTE

Dynamic filtering нельзя «отключить» только отсутствием build filter collector в новом operator. В плане могут остаться scan consumers и ожидания завершения producer. Предлагаются два допустимых пути: поддержать end-to-end producer/consumer lifecycle или согласованным planner pass убрать producer IDs и соответствующие predicates для SMJ. Начальный MVP выбирает второй путь. При этом filters, относящиеся к другим joins, должны сохраниться.

Семантическая граница также важна: фильтрация preserved side LEFT/FULL по отсутствующим ключам другой стороны может удалить обязательные unmatched rows. Переиспользовать только доказанные существующие правила, не переносить двустороннее key filtering из чужого engine без разбора SQL semantics.

FTE exchange storage и local sort/group storage — разные ресурсы. Завершённый локальный run не становится durable input другой task attempt. Для TASK support нужны повторное чтение входных exchanges, новые attempt-owned temporary files, удаление файлов неуспешной попытки и отсутствие повторного output в committed result. Existing adaptive join reordering имеет hash-specific local exchange rewrite, поэтому initial SMJ integration должна либо явно не выбирать SMJ при TASK, либо получить FTE coverage сразу. «Работает при RetryPolicy.NONE» не доказывает поддержку TASK.

Если запретить TASK на первом этапе, это ограничение обязано быть видно в EXPLAIN/диагностике forced mode. Unsupported forced SMJ должен давать понятный отказ или явно объяснённый planner fallback; тихо выполненный hash join не докажет ни корректность, ни производительность тестируемого SMJ.

## План небольших reviewable изменений

1. **Контракты и тестовый стенд.** Зафиксировать ordinary equality, INNER, orderable key subset, Page/Block ownership, cursor/blocked/yield protocol. Сделать kernel tests на готовых sorted streams; planner пока не меняется.
2. **Внешняя память.** Bounded sorter с fan-in, replayable duplicate-group storage, quota accounting, cleanup и interruption. Storage отдельно тестируется на повторном чтении, коротких страницах, большом числе runs и превышении дисковой квоты.
3. **Принудительный local integration.** Source-shell, producer/run bridge, paired partition identity, validator. Проверки driver lifecycle и retained-memory ceiling; никакой автоэвристики.
4. **Распределённый planner path.** Algorithm field либо отдельный node, поддержка remote/local exchange requirements, guarding rewrites, serialization, EXPLAIN. Тесты с несколькими workers и local partitions.
5. **Расширение SQL subset.** Residual predicate и LEFT; затем отдельный design review RIGHT/FULL match tracking. Null-safe equality, semi/anti semantics и сложные key types — отдельные gates.
6. **FTE и dynamic filters.** Согласованное восстановление producer/consumer dependencies, adaptive plans и task failure injection. Presorted streaming fast path проходит собственную проверку progress.
7. **Автовыбор и optimization.** Cost model учитывает sort, I/O, skew, output и доступный порядок. Пороги выбираются по benchmarks, включая проигрыши SMJ на малом build. Normalized keys, SIMD и off-heap не являются предварительным условием правильной реализации.

Это последовательность engineering gates, не оценка сроков. Ступени 1–3 ещё не дают обещание масштабируемого MPP JOIN на десятках миллиардов строк.

## Проверки, которые maintainer будет ожидать

| Уровень | Отправная точка | Что добавить |
|---|---|---|
| Operator | [`TestHashJoinOperator`](../../../core/trino-main/src/test/java/io/trino/operator/join/TestHashJoinOperator.java), nonspilling counterpart | Differential multiset tests, duplicates, nulls, residual FALSE/NULL, page boundaries, blocked/yield/close |
| SQL semantics | [`AbstractTestJoinQueries`](../../../testing/trino-testing/src/main/java/io/trino/testing/AbstractTestJoinQueries.java) | Принудительный SMJ runner, assertions фактического plan path, типовые и adversarial запросы |
| Spill | [`TestSpilledJoinQueries`](../../../testing/trino-tests/src/test/java/io/trino/tests/TestSpilledJoinQueries.java) | Oversized equal group, thousands of runs, bounded fan-in, disk quota, cancellation, file cleanup |
| Planner | [`TestDynamicFilter`](../../../core/trino-main/src/test/java/io/trino/sql/planner/TestDynamicFilter.java), rule tests | Algorithm survives/invalidation после rewrites, отсутствие global gather, paired streams, unsupported type fallback |
| FTE | [`AbstractTestFaultTolerantExecutionJoinQueries`](../../../testing/trino-testing/src/main/java/io/trino/testing/AbstractTestFaultTolerantExecutionJoinQueries.java) | Failure во время sort/group replay/merge/output, retry without lost/duplicate result, temporary file ownership |

Не ограничиваться checksum: collision или неверное число одинаковых строк может скрыть ошибку. Малые результаты сравнивать как multiset целиком. Для больших нагрузок использовать count плюс несколько независимых агрегатов и отдельно проверяемую генерацию данных. `count(*)` над JOIN может позволять optimizer удалить payload; для wide-row benchmark нужно сохранить реальную необходимость payload и проверить EXPLAIN.

Metrics первого релиза должны разделять sort spill и group replay: input/output rows, run count, merge passes, maximum fan-in, peak retained memory, group bytes/rows, replay bytes, blocked I/O time, CPU и spill cleanup failures. Без этого трудно отличить skew/output explosion от плохой сортировки или медленного диска.

## Cross-review четырёх ролей

1. **[Java architect](02-java-architect.md): отдельный node.** Согласен с явными physical invariants и запретом скрытой worker-side смены алгоритма. Оспариваю выбор отдельного node как безусловно меньшего риска: текущие late passes, DF cleanup и sanity visitors показывают значительную integration surface. Предлагаю сохранить обе альтернативы в ADR; для MVP предпочитаю guarded algorithm field с validator.
2. **[Java developer](01-java-developer.md): source-shell + две queues.** Поддерживаю source-shell и отказ от переделки `Driver`. Для первого memory-robust distributed baseline меняю transport на run handles; queues требуют более сильного progress proof и scheduler coverage. Это не возражение против streaming как целевого fast path.
3. **[Java optimization](03-java-optimization.md): bounded fan-in и replay storage.** Полностью поддерживаю как обязательные acceptance gates. Добавляю bound на manifest metadata и reader bookkeeping: перенос payload на диск сам по себе не исключает O(number of runs) memory growth.
4. **[MPP architect](04-mpp-architect.md): hash partitioning для начала.** Согласен: распределение и локальный алгоритм независимы. Range partitioning и hot-key replication пока отложить. Фиксирую отдельную maintainer задачу: отключить/адаптировать `AdaptiveReorderPartitionedJoin`, иначе FTE может разрушить paired local topology даже после корректного первоначального планирования.
5. **Все роли: откладывание RIGHT/FULL.** Поддерживаю. Даже простой bitmap matched rows растёт с огромной группой, а distributed replication требует единственного владельца unmatched output. Лучше явный capability subset, чем формально широкий JOIN operator без bounded-memory доказательства.

Открытые решения для команды: представление algorithm, ownership общего task budget, schema run handles, phase boundary materialization, политика forced-mode fallback и срок включения TASK. Предложения здесь не подменяют reviewer agreement или измерения; они делают следующий прототип проверяемым.
