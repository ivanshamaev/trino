# Корректность, типы и жизненный цикл

Первый критерий готовности — совпадение SQL multiset с существующим execution path, затем ограничения ресурсов и только после этого throughput. Порядок результата без ORDER BY не является проверкой корректности join.

## Инварианты ключей

Для поддерживаемой пары типов нужны три независимых отношения:

1. `ordering(a,b)` определяет продвижение cursors и границы candidate groups.
2. `equality(a,b)` определяет совпадение join keys.
3. `residual(left,right)` определяет окончательное совпадение пары.

Если equality истинно, ordering должен помещать строки в одну группу; иначе merge может безвозвратно пропустить match. Обратное не обязательно: ordering=0 может включать строки, неравные по SQL. `isOrderable()` само по себе не является полным доказательством этой совместимости.

Конкретный факт этого checkout: [DoubleType](../../core/trino-spi/src/main/java/io/trino/spi/type/DoubleType.java), `equalOperator`, использует `left == right`; comparison группирует NaN вместе, а `identical` считает две NaN identical. Это не следует заменять привычными предположениями о других версиях SQL engine.

| Пара | Ordering текущего DoubleType | EQUAL | Следствие |
|---|---|---|---|
| NaN / NaN | 0 | false | Candidate group не означает match |
| +0 / -0 | 0 | true | Java `Double.compare` без поправки не годится |
| NULL / NULL | Одна sort group | SQL unknown | Ordinary `=` не соединяет их |

[JoinCompiler](../../core/trino-main/src/main/java/io/trino/sql/gen/JoinCompiler.java), `typeEqualsBothNotNull`, и [DefaultPagesHash](../../core/trino-main/src/main/java/io/trino/operator/join/DefaultPagesHash.java) дают baseline equality path. [TypeOperators](../../core/trino-spi/src/main/java/io/trino/spi/type/TypeOperators.java) различает equality, identity и ordering; нельзя подменять их сериализованным byte compare.

Начать следует с BIGINT. Затем отдельно проверять VARCHAR/CHAR, DECIMAL precision/scale, date/time/timestamp, DOUBLE/REAL, composite keys и complex types. Casts должны быть вычислены до partitioning и sorting; ключи обеих сторон должны иметь согласованные типы. Для выражений ключа важно не вычислять недетерминированное выражение повторно при replay — materialize соответствующий symbol один раз по существующей semantics плана.

## NULL и виды JOIN

Для ordinary equijoin достаточно NULL в любом компоненте ключа, чтобы пара не совпала. INNER может пропустить такую строку; LEFT обязан сохранить левую unmatched; FULL — обе стороны. NULL placement используется для сортировки, а не для разрешения match.

Null-safe `IS NOT DISTINCT FROM`, SQL `IN`, `NOT IN`, SEMI и ANTI имеют дополнительные правила. Нельзя свести их к ordinary equality с одним флагом без анализа planner representation и SQL three-valued logic. Они остаются вне первого MVP.

Для residual predicate match существует только при TRUE. FALSE и NULL оба означают отсутствие match. Особенно важен пример:

```sql
SELECT l.k, r.k
FROM (VALUES (1)) AS l(k)
LEFT JOIN (VALUES (1)) AS r(k)
    ON l.k = r.k AND l.k + r.k > 10;
-- Ожидается одна строка (1, NULL), хотя sort keys совпали.
```

Этот пример — тестовая спецификация, здесь он не исполнялся. Для FULL обе строки должны быть выданы как unmatched. Match bit выставляется после всех predicates, а не после обнаружения key group.

## Duplicate groups через все границы

Ключ может пересекать Page, source split, sort run, reader buffer и yield boundary. Group store закрывается только после доказанного изменения ключа или EOF, а не по границе страницы. Вход с двумя левыми и тремя правыми строками одного ключа должен дать шесть пар при INNER без residual. Сравнивать нужно multiplicities, иначе тест пропустит потерю duplicates.

Для LEFT replay одной правой группы может хранить один matched flag текущей левой строки либо bounded left tile. Для FULL matched bitmap всей правой группы может сам стать большим: `ceil(rows/8)` байт плюс indexing и storage. Поэтому spill input rows без spill/segmentation matched-state не даёт строгой bounded-memory гарантии. Конкретную FULL стратегию следует разработать отдельно.

## Output ordering

INNER на отсортированных inputs обычно позволяет сохранить порядок join key внутри instance, если grouping/output policy это действительно соблюдает. LEFT может сохранять левый ключ. FULL, projected right keys с NULL-расширением, flip и coalesce требуют отдельного property derivation. До доказательства безопаснее не объявлять output ordering вообще. Local order никогда автоматически не становится глобальным SQL ORDER BY.

## Ownership и memory revocation

| Событие | Обязательное поведение |
|---|---|
| Передача Page в async writer | Данные остаются валидными до завершения записи; ownership/accounting однозначны |
| Сохранение одной строки ключа | Copy или учёт retained backing blocks; маленький view не считается маленьким allocation |
| Output page filled | Сохранить оба cursor positions и matched-state; не повторять уже выданную пару |
| Yield в scanning loop | Не потерять позицию даже если output пока пуст |
| Memory revoke | Сохранить state, инициировать spill, завершить переход после future; оставить progress reserve |
| Replay reset | Новый reader начинает в корректной точке sealed group; старый reader закрыт |
| Producer failure | Разбудить consumer и peer producers с той же ошибкой |
| Downstream finish/LIMIT | Прекратить ненужное чтение, закрыть обе стороны и все временные ресурсы |
| Cancel во время I/O | Не удалять используемый файл до безопасного завершения/прерывания reader; cleanup idempotent |
| Disk full / quota exceeded | Явная ошибка и освобождение ресурсов; никакого бесконечного blocked state |

[Operator](../../core/trino-main/src/main/java/io/trino/operator/Operator.java) и [Driver](../../core/trino-main/src/main/java/io/trino/operator/Driver.java) — источник текущего threading/revoke contract. `WorkProcessor` и future не заменяют ownership protocol. Hot loop должен освобождать выполнение при блокировке; future должен сигнализировать именно возможность следующего шага, а не произвольное завершение другой фазы.

## Распределение и retries

Для каждой истинной пары equality оба ряда должны попасть к одному merge instance, а каждая пара должна быть выдана ровно один раз. Проверяются remote/local partition identities, coercions и mappings, а не только число buckets. Broadcast и skew replication требуют отдельного ownership для unmatched rows; в базовом MVP они исключены.

FTE tests после включения TASK обязаны проверять failure во время run generation, intermediate merge, capture/replay группы и output publication. Новая попытка строит собственное локальное состояние. Завершённый файл прошлой попытки на другом worker не считается доступным. Exactly-once видимость результата обеспечивает existing task/exchange attempt protocol, а не случайное отсутствие повторов в merge-loop.

## Что проверяется отдельно от SQL output

Успешный маленький join не доказывает отсутствие memory leak, deadlock или ошибочного resource accounting. Нужны deterministic interleavings с задержанными futures, tiny budgets и отменой; проверка нулевых оставшихся reservations/files/readers; выявление роста памяти с количеством runs и размером группы; validator tests после optimizer rewrites. Программа конкретизирована в [08](08-validation-and-benchmarks.md).
