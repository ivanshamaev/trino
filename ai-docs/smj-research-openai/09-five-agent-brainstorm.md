# Brainstorm пяти агентов

Проведены пять отдельных ролевых исследований в две волны при ограничении доступных параллельных слотов. Первые три роли самостоятельно изучали operator/planner/optimization; затем MPP и Trino роли добавили собственный анализ и cross-review. Java architect выполнил второй раунд review после получения остальных позиций. Пятый агент был прерван лимитом использования, затем возобновлён и завершил записку. Ни одна роль не читала два запрещённых каталога.

Это инженерный brainstorm, а не пять независимых performance experiments. Индивидуальные записки сохранены, включая отличающиеся предложения; финальный выбор ниже — синтез ведущего агента.

| Роль | Записка | Основной вклад |
|---|---|---|
| Java developer | [01](brainstorm/01-java-developer.md) | Одно-входовый Operator, source-shell, resumable state machine, revoke/close |
| Java architect | [02](brainstorm/02-java-architect.md) | Physical node vs algorithm field, partition-local order, scheduler dependencies |
| Java optimization | [03](brainstorm/03-java-optimization.md) | Equality vs comparator, bounded fan-in, retained bytes, group replay |
| MPP engine architect | [04](brainstorm/04-mpp-architect.md) | Парность partitions, skew, task attempts, progress и общий budget |
| Trino developer | [05](brainstorm/05-trino-developer.md) | Planner rule audit, validator, DF cleanup, phased PRs и тестовые базы |

## Вопрос 1: как подключить два входа

Java developer предложил два sinks, две bounded queues и merge source: симметричный lifecycle без изменения Driver. MPP architect возразил против queues как первого большого-data milestone: ограниченная очередь может связать producer с consumer, а consumer — с ещё не запущенной второй ветвью. Existing scheduler умеет снимать часть зависимостей при заполнении output buffer, поэтому зависание не объявлено доказанным, но новые bridge waits могут не совпадать с наблюдаемыми scheduler событиями.

Java architect и Trino developer поддержали publication завершённых runs. Во втором раунде архитектор уточнил: барьер нужен для конкретной пары partitions; последний merge может непосредственно питать join, не обязательно материализовать один общий файл.

**Решение:** source-like shell и два producer pipelines сохраняются; первый transport — sealed run manifests. Streaming queues остаются отдельным этапом для presorted inputs. Memory-only runs должны освобождать общий бюджет или иметь гарантированный резерв для второй стороны.

## Вопрос 2: отдельный node или algorithm field

Java architect предпочёл новый `SortedMergeJoinNode`: явный physical contract изолирует новый оператор от hash-specific rules. MPP роль поддержала эту ясность. Trino developer привёл стоимость нового узла: `PlanNode` JSON subtype/UI traversal, dependency checker, DF cleanup/producer passes, cost/stats, visitors и schedulers. При этом существующий JoinNode уже содержит физические атрибуты.

**Решение для MVP:** новое algorithm field с явными requirements, guard audit и финальным validator. Во втором раунде Java architect согласился условно, сохранив аргументы за отдельный physical node в долгосрочном дизайне. Это не консенсус «поле всегда лучше», а выбор меньшего проверяемого integration surface при строгих условиях.

Из фактических правил особенно важны `UseNonPartitionedJoinLookupSource` после local exchanges и `AdaptiveReorderPartitionedJoin` при TASK. Сохранить algorithm enum в copy constructor недостаточно: нужно сохранить весь partition/order contract.

## Вопрос 3: можно ли переиспользовать сортировку и spill

Все роли согласились переиспользовать PagesIndex, type comparators и merge primitives. Java optimization показал две границы: текущий OrderBy объединяет все runs без явного bounded multipass, а FileSingleStreamSpiller запрещает повторное чтение. Java developer дополнительно обнаружил синхронный spill wait в OrderBy finish и отсутствие готового spill lifecycle в WorkProcessor adapter.

**Решение:** отдельный bounded sorter и отдельный replayable group store с понятным ownership. Existing classes — база для частей реализации, но не готовый memory guarantee. Ограничиваются readers, descriptors, retained blocks и match metadata, а не только размер основного буфера.

## Вопрос 4: что входит в semantics MVP

Optimization роль указала на NaN и ±0; developer — на residual filters и unmatched rows; MPP — на ownership outer rows при replication. Все согласились, что FULL требует отдельного bounded matched-state и более сложного тестирования.

**Решение:** kernel начинает с ordinary BIGINT INNER equijoin, включая duplicates и residual. Затем LEFT и расширение типов через differential tests. FULL, null-safe equality, SEMI/ANTI и skew replication не добавляются под видом «небольшого расширения». Это граница первого milestone, а не отказ от будущей функциональности.

## Вопрос 5: FTE и масштаб

MPP architect проверил `HashDistributionSplitAssigner`: наличие кода split-largest-source не означает общий готовый JOIN skew splitter — production `canSplit` ограничен scale writers. Trino developer показал, что adaptive reordering сейчас перестраивает local topology под hash. Архитектор дополнительно проверил отключение existing distributed sort при TASK.

**Решение:** initial integration явно ограничивает retry support, TASK включается отдельным milestone. Local runs принадлежат attempt и пересоздаются из exchange inputs. Миллиардный input test не заменяет hot-key test, а bounded RAM не означает отсутствие straggler или дешёвый результат N:M.

## Уточнения, сделанные при cross-review

- В предварительном сообщении optimization роли была гипотеза, что hash equijoin использует IDENTICAL. Проверка `DefaultPagesHash` и `JoinCompiler` показала EQUAL path; окончательная записка и синтез используют исправленную версию.
- Scheduler dependency сама по себе не доказывает deadlock; в коде есть unblock mechanism. Итог формулируется как риск и обязательный regression scenario.
- Partial merge в статье ClickHouse описан упрощённо как always external right sort; выбранный исходный tag допускает in-memory right blocks. Итоговый обзор различает статью и код.
- Materialized manifests уменьшают producer/consumer coupling, но metadata и memory-only runs тоже должны иметь bound. Иначе новый дизайн переносит OOM в другую структуру.

## Оставшиеся разногласия и проверки

Нужно проверить prototype-ом, окупает ли более ранний streaming path дополнительную scheduler сложность; сравнить fan-in и reader budget; выбрать interface для temporary storage, совместимый с дальнейшим TASK support; измерить цену replay больших групп; определить workloads, где SMJ действительно лучше улучшенного external hash. Ни одно из этих решений не объявлено доказанным голосованием агентов.
