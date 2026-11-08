# Третий аудит Sorted Merge Join

Дата: 2026-11-08. Ветка `smj-plan`, HEAD `0393e45fcf9` (`Fix sorted merge join audit findings`). База software diff — `e112ffdf730`. Исследуется реализация после исправлений второго аудита.

## Вердикт

**Концепция partitioned external merge join жизнеспособна, но отправлять текущую ревизию как готовую к upstream merge преждевременно.** Основной merge/replay path заметно укреплён: штатные regressions и пятисценарная проверка на 10 млн строк проходят, воспроизведённый ранее рост replay устранён. При этом третий аудит выявил два новых воспроизводимых P1: поздний rewrite превращает MERGE в некорректно подготовленный hash join; auxiliary match bitmap обходит spill encryption.

Тихая подмена SQL-результата в завершённых корректных differential-проверках пока не обнаружена. Это не доказательство её отсутствия. Падающий обычный grouped join и нарушение заявленного encryption contract — достаточные основания исправить код до upstream MR. P0 без доказанного контрпримера не назначается.

Автор уточнил, что сейчас это личный MVP: CLA не подписан, scope с upstream не согласован. Это известные условия будущего submission, а не незакрытые ошибки текущего личного проекта. Этот аудит не публикует PR и не меняет production Java.

| ID | Приоритет | Результат |
|---|---|---|
| V03-01 | P1 | PushPartialAggregationThroughJoinWithProjection сбрасывает MERGE после выбора local topology; запрос падает с `spillable not yet set`, без spill также воспроизведён `no more sources` |
| V03-02 | P1 | DiskBackedMatchBitmap сохраняет открытый match-state при включённом spill encryption |

## V03-01. Поздняя projection/aggregation rewrite нарушает физический контракт JOIN

### Причина

[PushProjectionThroughJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushProjectionThroughJoin.java):123 создаёт JoinNode с `algorithm = Optional.empty()`. Это допустимо как повторный логический выбор до фиксации физического плана, но helper используется также в [PushPartialAggregationThroughJoin.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughJoin.java):143.

Этот optimizer вызывается в [PlanOptimizers.java](../../core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java):1026 **после AddExchanges и AddLocalExchanges**. До rewrite AddLocalExchanges уже выбрал single-stream MERGE path и не назначил hash-specific spillability. После rewrite LocalExecutionPlanner выбирает createLookupJoin, где [LocalExecutionPlanner.java](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java):3005 требует установленный spillable. В EXPLAIN исчезает `Algorithm: MERGE`, но остаются элементы topology, подготовленные для него.

Проблема затрагивает обычный INNER JOIN с partial aggregation над выражением и достаточным grouping set. `push_partial_aggregation_through_join=true` является default. Воспроизведение использует FORCE; любой иной режим, который установит тот же MERGE marker, проходит через тот же последующий rewrite.

### SQL-воспроизведение

В Iceberg test runner создаются две небольшие таблицы:

```sql
CREATE TABLE fact WITH (sorted_by = ARRAY['k']) AS
SELECT CAST(n % 16 AS BIGINT) k, CAST(n AS BIGINT) v
FROM UNNEST(sequence(1, 640)) t(n);

CREATE TABLE dim WITH (sorted_by = ARRAY['k']) AS
SELECT CAST(n AS BIGINT) k, CAST(n % 2 AS BIGINT) g
FROM UNNEST(sequence(0, 15)) t(n);

ANALYZE fact;
ANALYZE dim;

SET SESSION prefer_merge_join = 'FORCE';
SET SESSION join_distribution_type = 'PARTITIONED';
SET SESSION join_reordering_strategy = 'NONE';
SET SESSION push_partial_aggregation_through_join = true;
SET SESSION spill_enabled = true;

SELECT r.k, r.g, sum(l.v + 1)
FROM fact l JOIN dim r ON l.k = r.k
GROUP BY r.k, r.g;
```

Результат текущего HEAD: `QueryFailedException: spillable not yet set`. В том же fixture `sum(l.v)` проходит. Отдельное воспроизведение на VALUES подтверждает отказ без зависимости от Iceberg metadata; в режиме spill=false наблюдался `IllegalStateException: no more sources` из LocalExchange.

Негативные probes: `TestMergeJoinPostSelectionAudit` и `TestIcebergSortedWriting#testAuditV03PostSelectionAggregation`. Первичные логи — [core-probes-final.log](evidence/core-probes-final.log), [distributed-aggregation.log](evidence/distributed-aggregation.log). В окончательном локальном повторе тот же `sum(l.v + 1)` **сначала успешно выполнен с OFF и FORCE/pushdown=false**: по 16 строк, результаты совпали, для spill=true и spill=false. Затем FORCE/pushdown=true воспроизвёл обе ошибки. Distributed Iceberg повтор также подтвердил оба успешных controls и последующий отказ `spillable not yet set`. Это независимые controls, а не подмена упавшего выражения предыдущим успешным `sum(v)`. Финальные логи — [negative-controls.log](evidence/negative-controls.log), [distributed-controls.log](evidence/distributed-controls.log).

### Что исправлять

- Определить контракт helper-а для уже выбранного алгоритма. Payload-only projection, сохраняющая equi-keys, не должна стирать MERGE после physical planning.
- Если алгоритм всё-таки меняется, заново согласовать local exchanges, driver cardinality и spillability. Один новый Optional.empty() либо один default для spillable этого не делает.
- Проверить соседний PushPartialAggregationThroughExchangeWithProjection: его pattern не отвергает ordered exchange, в отличие от варианта без projection. Простая правка маркера должна сопровождаться regression, запрещающей последующую потерю ordering.
- Проверять final plan после поздних rewrites: MERGE input ordering/partitioning/single-stream и hash spillability. JUnit test первоначального выбора алгоритма такой инвариант не проверяет.

Приёмка: приведённый grouped SQL и варианты с projection/без неё, spill on/off, INNER/LEFT/RIGHT/FULL там, где применим rewrite; результат равен OFF oracle, маркер и ordered topology согласованы, нет `spillable not yet set` / `no more sources`.

## V03-02. Match bitmap не соблюдает включённое spill encryption

### Причина и влияние

[FileSingleStreamSpillerFactory.java](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java):175 создаёт encryption key для native spiller. Но [AppendingSpiller.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/AppendingSpiller.java) выдаёт путь auxiliary match file, а [DiskBackedMatchBitmap.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/DiskBackedMatchBitmap.java):56 открывает его обычным RandomAccessFile. В `flush():123` buffer записывается напрямую, без encryption/decryption.

Это match-state по build-row ordinal: строка отмечается, если хотя бы один кандидат пережил residual filter. Это производные данные запроса, а не только offsets или размеры контейнера. Тест не доказывает восстановление исходных column values из bitmap, но непосредственно показывает открытое хранение результатов matching.

Контракт [properties-spilling.md](../../docs/src/main/sphinx/admin/properties-spilling.md#spill-encryption-enabled) обещает encrypt/decrypt data spilled to disk, а [spill.md](../../docs/src/main/sphinx/admin/spill.md#spill-encryption) — защиту spill contents. Ограничение на plaintext match-state не описано. Проблема касается RIGHT/FULL с residual predicate, когда match-state переходит на disk bitmap; default encryption=false её не активирует как нарушение включённой защиты.

### Воспроизведение

Audit test создаёт реальную FileSingleStreamSpillerFactory с encryption=true, через AppendingSpiller получает auxiliary path и создаёт production DiskBackedMatchBitmap на 9000 позиций. Помечаются чётные позиции первого блока; обращение к позиции 8192 принудительно flush-ит первый блок.

Файл имеет длину 1125 bytes, его первые 1024 bytes равны известному открытому шаблону `0x55`. Проверка ожидаемой защиты падает: `firstBlockPlaintext=true`. См. `TestAppendingSpiller#testAuditV03EncryptedMatchBitmap`, [core-probes-final.log](evidence/core-probes-final.log); финальный повтор — [negative-controls.log](evidence/negative-controls.log).

Offset index отдельно хранит seek metadata. В этом замечании не утверждается, что metadata-only offsets сами по себе раскрывают исходные значения; доказанный контрпример относится именно к match bitmap.

### Что исправлять

Шифрование auxiliary storage должно быть частью spiller API/контракта, а не зависеть от того, обёрнут ли основной stream сериализатором. Для mutable bitmap нужны encryption-aware random-access blocks и согласованный lifecycle. Допустимая временная граница MVP — явный безопасный fallback/eligibility restriction для неподдержанного сочетания, с корректной документацией. Молчаливая запись plaintext при включённой защите недопустима.

Приёмка: forced disk bitmap при encryption=true, RIGHT/FULL и residual; результаты как у OFF, match-state не обнаруживается открытым шаблоном в auxiliary files, cleanup/cancel и spill accounting проходят. Отдельно проверить компрессию с encryption и повторный replay; успешный round-trip основного data file не закрывает bitmap contract.

## Что подтвердилось после v02

- Штатный core набор: 279 tests, 0 failures/errors, 1 expected extreme skip. Включены пять opt-in сценариев по 10 млн build rows и дополнительный TestPushPartialAggregationThroughJoin.
- Iceberg config и методы исходного TestIcebergSortedWriting: 16 tests, 0 failures/errors, 2 opt-in skips.
- Differential fuzz: 40 seeds × четыре join type × in-memory/native spill = 320 проверок мультимножеств. Scalar/composite keys, NULL, duplicates, residual и случайные page boundaries. Финальный повтор с NONE/LZ4/ZSTD и encryption on/off прошёл; распределение по seeds описано в [VALIDATION.md](VALIDATION.md).
- Дополнительно прошли 40 SQL comparisons для десяти scalar families и четырёх join types с независимым OFF oracle и проверкой MERGE в EXPLAIN.
- Replay sweep 768/3072/12288 pages, группы 2/3/4 pages: read/write 2.4053–2.8875, без прежнего роста с размером build. Это counters serialized data/index bytes, не physical disk I/O.
- Multi-file OFF Window/TopN/GROUP BY comparisons прошли. Отдельный ordered-exchange aggregation SQL пять раз совпал с OFF; в выбранном плане RemoteMerge сохранился. Тихая потеря ordering на этом сценарии не установлена.

Матрица отдельных исправлений v02 и все ограничения прогонов — в [VALIDATION.md](VALIDATION.md). Число SQL executions внутри JUnit test не равно числу JUnit tests.

## Scope и готовность к upstream

Архитектура решает узкую ресурсную задачу. Полная стадия multi-driver/FTE/range splitting/joint CBO и масштабные gates исходного плана не следуют из закрытого MVP checklist. Публичный экспериментальный scope это в основном отражает; [CONCEPT_REVIEW.md](CONCEPT_REVIEW.md) разбирает контракты отдельно.

Исходная smj-plan всё ещё содержит прежнюю историю и private ai-docs. Для внешнего review подготовлена отдельная smj-upstream-v02. Она не исправляет V03-01/V03-02 автоматически: её software snapshot содержит те же проблемные места. CLA и upstream scope известным образом пока не оформлены. Это не повод переписывать/push-ить историю в рамках аудита.

Порядок следующих работ — [REMEDIATION_CHECKLIST.md](REMEDIATION_CHECKLIST.md). Production code не исправлялся; новый аудит не объявляет устранёнными найденные им дефекты.
