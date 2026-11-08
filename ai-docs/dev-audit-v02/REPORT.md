# Второй аудит Sorted Merge Join перед upstream MR

Дата: 2026-11-08. Ветка `smj-plan`, HEAD `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`. База ветки: `e112ffdf730`. Номера строк относятся к этому HEAD, до временной вставки проверочных тестов.

Последующие исправления и их фактическая проверка ведутся в [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md). Итог исправлений — [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). Этот отчёт сохраняет результаты аудита исходной ревизии.

## Вердикт

**В текущем виде отправлять реализацию как готовую к включению в Trino преждевременно.** Концепция partitioned external sort + merge + spill-backed group replay работоспособна. Исправления v01 существенно улучшили реализацию: целевой core-набор повторно проходит, группы больше памяти действительно обслуживаются через spill, прежние ошибки на границах страниц покрыты тестами. Однако обнаружены новые воспроизводимые дефекты, включая падение обычного Iceberg SELECT при выключенном SMJ и нелинейное чтение spill на небольших повторяющихся группах.

Новых случаев тихого неправильного результата SQL в выполненных проверках не обнаружено. Это не доказательство их отсутствия. Падение планирования допустимого FULL JOIN, регрессия Iceberg при OFF и нарушение требований к scheduler являются самостоятельными основаниями отложить MR. Предлагаемая классификация: P1 — исправить до MR; P2 — устранить либо явно согласовать ограничение и критерий проверки. P0 с доказанной потерей/подменой результата в этом аудите не установлен.

| ID | Приоритет | Результат |
|---|---|---|
| V02-01 | P1 | Iceberg: допустимый отсутствующий `sort_order_id` вызывает NPE даже при SMJ OFF |
| V02-02 | P1 | Два существующих `TestIcebergConfig` уже падают на текущем HEAD |
| V02-03 | P1 | FULL JOIN с повторяющимся символом в equi-критериях падает при построении сортировки |
| V02-04 | P1 | Разреженный replay-индекс даёт квадратичный рост чтения при линейном размере результата |
| V02-05 | P1 | Построчный merge и ряд scan/drain-циклов не соблюдают yield-контракт |
| V02-06 | P2 | Статистика SMJ не сериализуется/не агрегируется как штатный OperatorInfo |
| V02-07 | P2 | Измеренный task memory не включает output PageBuilder; абсолютный heap-cap этим не доказан |
| V02-08 | P2 | Вспомогательные bitmap-файлы не охвачены очисткой spill после рестарта |
| V02-09 | P2 | Переписывание residual expression сбрасывает установленный MERGE marker |
| V02-10 | P1 | RIGHT MERGE сохраняет dynamic filter без работающего build-side producer |

Архитектурный разбор и проверка предпосылок вынесены в [CONCEPT_REVIEW.md](CONCEPT_REVIEW.md). Команды, результаты и ограничения экспериментов — в [VALIDATION.md](VALIDATION.md), перечень исходников — в [SOURCE_REVIEW.md](SOURCE_REVIEW.md), условия готовности MR — в [MR_READINESS_CHECKLIST.md](MR_READINESS_CHECKLIST.md).

По отдельному запросу выполнена также [проверка соответствия CONTRIBUTING.md и DEVELOPMENT.md](GUIDE_COMPLIANCE.md). Её результаты и новые pre-PR запуски дополняют исходный функциональный аудит, не меняя нумерацию V02-01–V02-10.

## V02-01. Iceberg SELECT падает на отсутствующем sort order ID

**Подтверждено distributed-тестом на реальной Iceberg-таблице.** В [IcebergMetadata.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java), `filesHaveSortOrder():1823`, сравнение `fileScanTask.file().sortOrderId() != sortOrderId` распаковывает nullable `Integer` в `int`. Отсутствующий ID вызывает NPE. При этом [IcebergConfig.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergConfig.java):73 включает `reportSortedness` по умолчанию, независимо от `prefer_merge_join`.

Тест создал sorted-таблицу, записал настоящую строку и заменил только файловый metadata record через Iceberg API на запись с `sortOrderId=null`. После commit повторный `planFiles()` подтвердил null. `SELECT k FROM ...` при `prefer_merge_join=OFF` завершился ошибкой:

```text
Cannot invoke "java.lang.Integer.intValue()" because the return value of
"org.apache.iceberg.DataFile.sortOrderId()" is null
```

Это допустимые метаданные: отсутствующий/неизвестный sort order означает отсутствие гарантии сортировки. [Спецификация Iceberg, Sorting и manifest data_file](https://iceberg.apache.org/spec/#sorting).

Исправление: null-safe проверка, возвращающая отсутствие sortedness для неизвестного ID. Добавить тесты на null/0/старый/current/mixed ID, в том числе обычный SELECT при SMJ OFF. До исправления отключение catalog property `report_sortedness` обходит эту ветку по коду; отдельный runtime-прогон этого обхода в v02 не выполнялся.

Свидетельство: [iceberg-null-sort-order.txt](evidence/iceberg-null-sort-order.txt). Два соседних теста обычной sortedness и multi-file SMJ прошли в том же запуске.

## V02-02. Текущий HEAD не проходит штатные тесты IcebergConfig

**Подтверждено запуском существующих тестов без изменений их исходников.** [TestIcebergConfig.java](../../plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestIcebergConfig.java):50 и :198 дают:

```text
testDefaults: Untested attributes: [ReportSortedness]
testExplicitPropertyMappings: Untested properties [iceberg.report-sortedness]
```

В классе три теста, два упали. Изменение `IcebergConfig` не сопровождалось обновлением defaults и полного property mapping. У `setReportSortedness():127` также отсутствует `@ConfigDescription`, требуемый локальным developer guide.

Исправление: обновить оба теста и описание свойства; документировать config/session property и её значение по умолчанию. Эти ошибки не связаны с нестабильностью JVM или новыми негативными probes.

Свидетельства: [iceberg-config.txt](evidence/iceberg-config.txt), [полный запуск](evidence/iceberg-config.log).

## V02-03. Повторяющийся equi-символ ломает FULL JOIN

**Подтверждено SQL-тестом.** [AddExchanges.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java):1038–1042 строит карту порядка из списка equi-символов через `toImmutableMap`, не устраняя повторения. Допустимый JOIN:

```sql
SELECT l.x, r.x, r.y
FROM (VALUES 1, 2, 3) l(x)
FULL JOIN (VALUES (1, 1), (2, 3), (3, 3)) r(x, y)
    ON l.x = r.x AND l.x = r.y
```

При `prefer_merge_join=FORCE`, `join_distribution_type=PARTITIONED`, `join_reordering_strategy=NONE` он падает с `Multiple entries with same key ... ASC NULLS LAST`. Hash-вариант возвращает четыре строки: `(1,1,1)`, `(3,3,3)`, `(2,NULL,NULL)`, `(NULL,2,3)`.

В выполненной матрице этот пример для INNER/LEFT/RIGHT прошёл, для FULL упал. Нельзя рассчитывать, что предварительное equality inference всегда нормализует критерии: FULL сохраняет оба условия. Аналогичное построение карты есть в `PropertyDerivations.visitJoin():610` для INNER/LEFT.

Исправление: нормализовать физический OrderingScheme, сохранив все equi-условия и согласованность сравнений на обеих сторонах. Простое удаление одного условия JOIN недопустимо. Проверить повторения слева/справа, составные ключи и все четыре join type.

Свидетельство: [statistics-and-criteria.txt](evidence/statistics-and-criteria.txt), тест `testRepeatedJoinSymbol` в [patch](repro/regression-probes.patch).

## V02-04. Bounded replay index не обеспечивает линейное чтение

**Подтверждено реальными spill-файлами и проверкой всех выходных строк.** В [FileSingleStreamSpiller.java](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java):332–360 сохраняется не более 128 checkpoints; по мере роста файла интервал между ними удваивается. `getSpilledPagesFrom():217–223` открывает reader у ближайшего checkpoint и читает сериализованные страницы до нужного индекса. В [MergeJoinBridge.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java):465–500 кэшируются две страницы.

Если группа занимает три страницы, discovery проходит её конец, а replay возвращается к началу, уже вытесненному из кэша. Возврат снова оплачивает промежуток от checkpoint. Число таких возвратов растёт с количеством групп, а средняя дистанция до checkpoint — с длиной всего файла.

Проверка: восемь BIGINT-строк в build-странице, три страницы на ключ, одна probe-строка на ключ, INNER без residual, memory limit 16 КиБ, compression NONE:

| Build pages | Записано, байт | Прочитано, байт | Read/write |
|---:|---:|---:|---:|
| 768 | 74 052 | 455 103 | 6,146× |
| 3 072 | 302 742 | 5 486 382 | 18,122× |
| 12 288 | 1 215 324 | 80 345 628 | 66,110× |

В последнем случае выдано и проверено 98 304 строки. Размер результата линейный, many-to-many explosion отсутствует. Это счётчики сериализованных байтов, прочитанных из spill, **не измерение физических чтений накопителя**: часть запросов может обслужить OS page cache.

При постоянных трёх страницах на группу дополнительная работа имеет порядок `Θ(P² / 128)` в страницах, а не `O(P)`. Тесты v01 с уникальными ключами и одной огромной группой обходили эту геометрию. Верхнюю границу «read ≤ 2× write» нельзя распространять с них на общий случай. Cost model этот механизм тоже не учитывает.

Исправление: хранить точный byte-offset/bookmark текущей группы либо использовать индекс на диске с ограниченным RAM-кэшем. Увеличение константы 128 не исправляет асимптотику. Нужны масштабные sweeps по числу страниц и ширине группы, включая 2/3/4 страницы и смешанные границы.

Свидетельства: [сырые значения](evidence/replay-amplification.json), [assertion](evidence/replay-amplification.txt), [полный лог](evidence/initial-probes.log).

## V02-05. Длительные циклы игнорируют driver yield

**Построчный путь подтверждён отдельным тестом; остальные перечисленные циклы проверены статически.** В [MergeJoinOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java):828 цикл останавливается на конце группы или заполнении output page. При отклоняющем residual output не заполняется, и цикл может пройти огромную группу за один вызов. `DriverYieldSignal` вообще не передаётся transformation.

В тесте сигнал yield был установлен заранее. Один `getOutputPages().process()` всё равно выполнил все 200 000 residual evaluations. Отдельные длинные циклы `loadGroup():888`, `finishGroupScan():952`, `drainMatchedGroup():1027` также не имеют yield/time budget. Лимит 65 536 в chunk-пути не покрывает их.

Следствие: несправедливое использование executor, медленная реакция на отмену, длинные синхронные чтения spill в driver thread. Сам по себе `WorkProcessorOperator` не делает внутренний цикл кооперативным. Штатный `DefaultPageJoiner` проверяет yield внутри обработки группы.

Исправление: передать yield signal и сохранять resumable состояние всех долгих scan/replay/drain-циклов. Добавить тесты rejected residual, большого unmatched gap, matched-bitmap drain и отмены на spill read. Синхронный reader допустим как отдельный компромисс, но длительная работа должна регулярно отдавать управление.

Свидетельство: [memory-and-yield.txt](evidence/memory-and-yield.txt), `testAuditV02YieldDuringRejectedGroup`.

## V02-06. Runtime statistics не доходят до штатного JSON/aggregation контракта

**JSON round-trip воспроизведён; aggregation/summarization проверены по коду.** [MergeJoinOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java):235 содержит `MergeJoinStatistics implements OperatorInfo`, но:

- Подтип не зарегистрирован в [OperatorInfo.java](../../core/trino-main/src/main/java/io/trino/operator/OperatorInfo.java):27, нет JSON-creator/property контракта. `JsonCodec<OperatorInfo>` выдаёт только `{"@type":"MergeJoinOperatorFactory$MergeJoinStatistics"}`, без счётчиков, и не может прочитать свой результат.
- Нет `Mergeable`; [OperatorStats.java](../../core/trino-main/src/main/java/io/trino/operator/OperatorStats.java):608–623 возвращает null info при объединении такого объекта.
- `isFinal()` остаётся false; `OperatorStats.summarize():626` убирает info. Объявление getter и обновление AtomicLong сами по себе не обеспечивают WebUI-наблюдаемость.

Не утверждается, что это ломает каждый distributed query: summarized task protocol может удалить info до передачи. Воспроизведён именно неподдерживаемый wire format, а обещанная финальная статистика теряется независимо от него.

Исправление: штатный сериализуемый и mergeable OperatorInfo со snapshot-значениями либо Metrics для аддитивных счётчиков и корректной max-агрегацией группы. Проверить JSON, aggregation двух задач, summarize и конечный query info.

Свидетельство: [statistics-and-criteria.txt](evidence/statistics-and-criteria.txt), `testStatisticsRoundTrip`.

## V02-07. Peak task memory — не доказательство полного heap-cap

**Измерена конкретная недостающая составляющая.** `PageBuilder` создаётся в [MergeJoinOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java):394; его retained size не входит в локальные memory contexts. `userMemoryContext` используется для match bitmap, `probeChunkMemoryContext` — для probe chunk.

Тест с 20 000 build-строками в компактной RLE-странице, одной probe-строкой и принимающим residual измерил **465 328 байт retained output builder при 160 байтах task user reservation и лимите 16 384 байта**. Измерение сделано внутри residual до выдачи output, поэтому речь не только о уже переданной downstream странице.

Это **не доказательство неограниченного роста heap**: builder ограничивается output-page policy; у штатного hash join transient output buffers также не полностью отражены в данном счётчике. Поэтому здесь P2, а не объявление нового SMJ OOM. Но заявления «фактическая память всей задачи ≤ cap» и численная калибровка memory model требуют уточнения. В тестах v01 проверялся Trino accounting; Java heap и все transient/serde/output allocations этим не измерены.

Доработка: определить заявляемый memory contract. Для строгого лимита включить buffer accounting/запас и размеры отдельных страниц, для обычного Trino-контракта явно описать ограниченный overhead сверх учтённой памяти и подтвердить профилем. Отдельно проверять большие VARCHAR, RLE/dictionary и удержанные probe pages.

Свидетельство: [memory-and-yield.log](evidence/memory-and-yield.log), `testAuditV02OutputMemoryAccounting`.

## V02-08. Bitmap-файлы переживут рестарт без штатной очистки

**Статическая проверка именования и lifecycle; аварийный рестарт не выполнялся.** [FileSingleStreamSpiller.java](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java):363 создаёт `merge-join-matches*.bin`. Эти файлы удаляет `DiskBackedMatchBitmap.close()`. Штатный [FileSingleStreamSpillerFactory.java](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java):75/:155 на старте ищет только `spill*.bin`.

При завершении JVM без close bitmap останется в spill directory и не будет найден последующим cleanup. Это ограниченная диском, но повторяемая утечка; накопление способно исчерпать spill volume. Обычная отмена и рестарт — разные сценарии.

Исправление: общий namespace/реестр временных spill-файлов и безопасная startup cleanup policy. Добавить тест оставленного bitmap, повторного создания factory и очистки только принадлежащих ей временных файлов.

## V02-09. Join expression rewrite сбрасывает MERGE marker

В [ExpressionRewriteRuleSet.java](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/ExpressionRewriteRuleSet.java):291–306 при изменении residual expression новый JoinNode получает `Optional.empty()` вместо `joinNode.getAlgorithm()`. Переписывание фильтра само по себе не меняет equi-ключи, distribution и eligibility.

`SimplifyExpressions` входит в `simplifyOptimizerRules`, которые запускаются и после AddExchanges (`PlanOptimizers:952/:979`). Поэтому нельзя обосновать сброс тем, что все expression rewrites всегда выполняются до выбора алгоритма. В отличие от адаптивной перестройки, здесь нет явного решения вернуться к hash с повторным cost comparison.

Прямой rule-test с эквивалентным упрощением `TRUE AND TRUE → TRUE` подтверждает потерю маркера. Конкретный SQL, в котором именно пост-layout запуск правила теряет marker, в v02 не установлен; не заявляется доказанная неправильность SQL-результата. Риск — незапланированная замена выбранного по памяти SMJ на hash и лишние сортировки.

Исправление: сохранять marker в преобразованиях, не меняющих его предпосылок; для намеренного сброса определить фазовый контракт. Проверить также `PushProjectionThroughJoin`/`PushDownDereferenceThroughJoin`, где маркер сбрасывается, и их фактическую достижимость после выбора SMJ. `PlanCopier` при раннем копировании logical plan может обоснованно обнулять physical choice — это другой случай.

Свидетельство: [marker-rewrite.txt](evidence/marker-rewrite.txt), отдельный тест в [patch](repro/regression-probes.patch).

## V02-10. RIGHT MERGE оставляет dynamic filter без producer

**Подтверждено distributed SQL и final QueryStats.** [PredicatePushDown.java](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/PredicatePushDown.java):619 разрешает dynamic filters для INNER и RIGHT. Но [LocalExecutionPlanner.java](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java):2938–2940 создаёт consumer, а сборщик `DynamicFilterSourceOperator` добавляет только при `node.getType() == INNER`. Для RIGHT не удаляются ни назначения фильтра, ни probe-side consumers.

Другой путь через отдельный DynamicFilterSourceNode ситуацию не исправляет: `AddDynamicFilterSource.RewriteJoinDynamicFilter.isEnabled()` включён только при TASK retries, тогда как eligibility SMJ требует NONE.

Проверка на Iceberg-таблице из трёх строк, `FORCE`, `PARTITIONED`, `join_reordering_strategy=NONE`, dynamic filtering включён:

| JOIN | Физический узел | Зарегистрировано DF | Завершено DF | Результат строк |
|---|---|---:|---:|---:|
| INNER | InnerJoin, Algorithm MERGE | 1 | 1 | 3 |
| RIGHT | RightJoin, Algorithm MERGE | 1 | 0 | 3 |

EXPLAIN RIGHT сохраняет `dynamicFilterAssignments` и probe ScanFilter. Это не ошибка результата в данном тесте: чтение может продолжиться после ожидания connector timeout. Но ожидаемое pruning отсутствует, а producer/consumer contract нарушен; при большом настроенном ожидании появляются ненужные задержки. Полный distributed deadlock этим тестом не установлен.

Исправление: либо поддержать сбор корректного домена для RIGHT, как допускает штатный join semantics, либо согласованно удалить его назначения и consumers до регистрации final plan. Недостаточно пропустить установку producer; публикация пустого домена также недопустима, поскольку может удалить реальные probe matches. Добавить regression с конечными dynamic-filter stats, временем ожидания и oracle результата.

Свидетельства: [right-dynamic-filter.log](evidence/right-dynamic-filter.log) с обоими EXPLAIN, [assertion](evidence/right-dynamic-filter.txt), `TestMergeJoinDynamicFilterAuditV02` в [patch](repro/regression-probes.patch).

## Что подтверждено положительно

- Повторный штатный core-набор: 134 теста, 0 failures/errors, 6 ожидаемых пропусков тяжёлых сценариев. Проверялись текущие исходники, без оставленных audit probes.
- В том числе проходят прежние исправления page-boundary/group replay, INNER/LEFT/RIGHT/FULL, residual, append/replay и bitmap, bounded sorting spill, async finish/cancel, model/selection/config, colocation и базовые SQL oracle.
- В отдельном Iceberg-запуске прошли sortedness и multi-file SMJ oracle, включая INNER/FULL и scan partitioning true/false. Воспроизведённый null-ID дефект относится к отсутствовавшему в них metadata case.
- Физическая схема «hash repartition совместимых ключей → sorted stream в каждой задаче → merge» обоснована. Iceberg per-file order используется как проверяемая подсказка для merge страниц, а не заменяет построение task-wide порядка.

## Что изменено аудитом

Добавлены только материалы в `ai-docs/dev-audit-v02/`. Временные Java probes отформатированы, запущены, сохранены как применимый patch и удалены из source tree; изменённые для проверки test-файлы восстановлены byte-for-byte к HEAD. Исправления production-кода в рамках этого аудита не выполнялись. Отчёт v01 и его checklist не переписывались. `hs_err_pid*.log` не читались.
