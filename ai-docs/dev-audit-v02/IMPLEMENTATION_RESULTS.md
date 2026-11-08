# Результаты исправлений аудита v02

Исходный аудит относится к `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`. Его отчёты и исходные evidence сохранены как история обнаружения дефектов. Критерии завершения — в [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md). Этот документ описывает исправления и результаты проверок.

**Технические исправления V02-01–V02-10 и G-01–G-07 завершены и проверены.** Кандидат — локальная `smj-upstream-v02`, HEAD `a0a11bff2fa450885f748da16549e4f8a9c202a7`. Автор уточнил: это личный MVP; Trino CLA не подписан, upstream scope не согласован. Эти условия относятся к будущему submission и не блокируют завершение текущего MVP.

## Исправления

| Пункт | Изменение и проверка |
|---|---|
| V02-01 | Nullable sort ID обрабатывается консервативно до unboxing. Current/null/zero/unknown/mixed file IDs, включая обычный SELECT с SMJ OFF, проверены Iceberg integration tests. |
| V02-02 / G-02 | Добавлены ConfigDescription, recorded defaults и explicit mappings Iceberg. Описания порогов согласованы с модельной allowance, равной половине per-node query limit. |
| V02-03 | Canonical sorts нормализуют повторные символы, включая отображение через exchange. Все equi-constraints сохраняются. SQL regressions проверяют повтор слева и справа для четырёх join types. |
| V02-04 | Append-only spiller включает opt-in дисковый индекс точных page offsets. На страницу хранится 8 байт на диске; индекс не растёт в heap. Обычные spillers сохраняют прежний режим. Проверены native/encrypted append/replay и масштабирование. |
| V02-05 | Scan, discovery, replay и unmatched drain проверяют DriverYieldSignal и ограничивают работу между возвратами. Continuation сохраняется до смены строки/группы. Есть forced-yield, rejected-residual, scan и cancellation regressions. |
| V02-06 | MergeJoinOperatorInfo — immutable JSON/mergeable operator snapshot. Счётчики суммируются, maxBuildGroupRows агрегируется максимумом. Проверены JSON, merge и статистика завершённого distributed query. |
| V02-07 | Отдельный memory context учитывает retained output PageBuilder; builder ограничен относительно query allowance. Освобождается необязательный replay cache. Попадание во второй cache slot поднимает текущую страницу в основной, сохраняя её reservation при освобождении optional slot. Проверены полный вывод и освобождение памяти. |
| V02-08 | Новые bitmap names попадают в spill cleanup; cleanup удаляет также legacy bitmap имена. Recovery test сохраняет посторонний похожий файл. Индекс использует штатное имя spill-файла. |
| V02-09 | Expression-only rewrite сохраняет JoinNode.algorithm. Документирована граница этапа выбора алгоритма; rule regression проверяет marker. |
| V02-10 | RIGHT dynamic filter получает build-side producer при наличии consumers. Distributed test проверяет результат и завершённость DF statistics. |
| G-04 / G-06 / G-07 | Удалён второй header, исправлены имена/Javadoc, reflection-only проверка заменена JSON round-trip; упрощены сложные ветвления, исправлен error code cleanup и избыточные FQN/imports. |

## Границы общих изменений

Bounded SortingSpiller включается только явным SortNode.boundedSpill для SMJ sorts. Обычный ORDER BY использует прежний final spill merge. Признак сохраняют plan copying, symbol rewriting, limit pushdown и JSON. Native spiller, ORDER BY и presorted/fallback regressions проверяют обе ветви.

Public documentation находится в [optimizer/merge-join.md](../../docs/src/main/sphinx/optimizer/merge-join.md), [optimizer properties](../../docs/src/main/sphinx/admin/properties-optimizer.md) и [Iceberg connector](../../docs/src/main/sphinx/connector/iceberg.md). Описаны шесть properties, supported scalar keys и четыре join type, PARTITIONED/retry NONE, single merge driver, spill, metrics и ограничения.

Reservation не является абсолютным пределом heap. Учитываются output builder, retained replay/probe state, match state и spill reservations; upstream input pages, временные serializer allocations, JVM overhead и уже переданные downstream pages могут потреблять дополнительную память. Это явно отражено в публичном контракте. QueryStats peak reservation — агрегированная runtime метрика, а не точное измерение максимального heap отдельного worker.

## Replay и выбор алгоритма

[Replay sweep](implementation-evidence/replay-sweep.json) использует 768, 3072 и 12288 страниц и группы по 2, 3 и 4 страницы, проверяя все строки. Отношение serialized bytes read/write остаётся между 2.388 и 2.888; для трёхстраничной группы прежний рост 6.146 → 18.122 → 66.110 устранён. В counters включены байты offset index. Это логическое чтение сериализованных данных; physical disk I/O из них не выводится.

Cost model учитывает indexed replay и остаётся локальной эвристикой после выбора join order/distribution. Она не выполняет глобальную CBO enumeration, а общие EXPLAIN cost totals не воспроизводят её оценку. Средний размер группы по NDV не описывает произвольный hot-key skew.

Нагрузочная проверка выявила отдельное ограничение входных статистик: Iceberg TableStatisticsReader оценивает VARCHAR через on-disk column_sizes с фиксированным множителем 2.7. Сжимаемые 512-байтовые строки могут быть значительно шире оценки даже после ANALYZE. На таком входе FORCE проходит при 128MB/node, а COST_BASED может оставить hash и получить memory-limit error. Сценарий сохранён явно; автоматический выбор не объявлен memory admission. Произвольный поправочный множитель и незаявленная глобальная переделка статистик не добавлялись. Отдельный high-entropy workload проверил корректное успешное исполнение FORCE/COST_BASED; результаты приведены ниже.

## Подготовка истории

Локальная ветка `smj-upstream-v02` подготовлена поверх upstream master `8ae1f91f85313e50fd72286810c52f4b058b1292` (на 53 коммита новее прежней базы). В ней четыре логических коммита без ai-docs и исследовательских файлов. SHA, деревья, границы и результаты промежуточных проверок записаны в [review-series.json](implementation-evidence/review-series.json). Исходная опубликованная история smj-plan не переписана, push/PR не выполнялись.

[UPSTREAM_PREPARATION.md](UPSTREAM_PREPARATION.md) содержит английский PR draft по шаблону и release-note proposal. По прямому сообщению автора CLA не подписан, а scope с maintainers не согласован. Для текущего личного MVP эти действия не требуются; в checklist готовности к будущему upstream submission они остаются невыполненными.

## Распределённые workload results

Проверка использует embedded IcebergQueryRunner с двумя workers, без исполнения задач на coordinator. Это воспроизводимая функциональная нагрузка с реальным exchange/spill, а не промышленный benchmark дискового устройства.

| Сценарий | Результат |
|---|---|
| Wide FULL, 750 000 строк, VARCHAR 512 bytes, 128MB/node | OFF/hash — memory-limit error. FORCE/MERGE — 10.47s, COST_BASED/MERGE — 10.70s; count/checksums совпали с OFF при 1GB/node. |
| Сильно сжимаемый вариант той же ширины | FORCE/MERGE — 10.35s и правильный результат; COST_BASED оставил hash и получил memory-limit error. Документированное ограничение approximate statistics. |
| Пороги small/pressure/big, 0.9/1.0/1.1 от модельной границы | 27 executions OFF/FORCE/COST_BASED, все результаты совпали с reference. Размеры рассчитаны из фактической SHOW STATS оценки, фиксированы два workers и memory budget. Выбор не обязан переключаться на каждом пороге: есть дополнительные gates cost advantage и ordering. |
| Два конкурентных join chains, skew и задержанная ветвь | Три итерации по два запроса, все шесть завершились; проверены count и checksums. |

Времена — отдельные наблюдения без повторов для confidence interval; общего выигрыша latency из них не следует. Peak user reservations агрегируются QueryStats по query. Спилл порядка 4.3–4.4GB в wide MERGE отражает несколько проходов сортировки и replay, а не размер исходной таблицы. Исходные метрики и их ограничения — [distributed-workloads-final.json](implementation-evidence/distributed-workloads-final.json), полный лог — [review-iceberg-tests-workloads.log](implementation-evidence/review-iceberg-tests-workloads.log).

## Финальная проверка кандидата

| Проверка | Результат |
|---|---|
| Commit 1 main/test build и native spill regressions | SUCCESS; 19 tests, 0 failures/errors/skips. |
| Commit 2 main/test build и ORDER BY/SortingSpiller regressions | SUCCESS; 17 tests, 0 failures/errors/skips. |
| Commit 3 main/test build и все изменённые core Test* классы | SUCCESS; 272 tests, 0 failures/errors, 6 skips. |
| Commit 4 main/test build Iceberg + trino-tests | SUCCESS. |
| Iceberg config/sortedness и opt-in workloads | 22 tests, 0 failures/errors, 2 skips; workload class включён явно. |
| TPC-DS partitioned plan fixtures OFF/COST_BASED | 206 tests, 0 failures/errors/skips. Это planner fixtures, а не физическое исполнение SF-1000; join-order golden strings не являются fingerprint всего физического плана. |
| Закреплённый официальный commit-message checker | Все четыре предлагаемых сообщения проходят. |
| Финальный verify -Pci на upstream-кандидате | SUCCESS во всех трёх модулях, 6 мин 48 с. Checkstyle: 0 violations; Airstyle/license/enforcer и включённые profile gates прошли. |
| Финальный clean main/test Error Prone на upstream-кандидате | SUCCESS во всех трёх модулях, 10 мин 56 с; main и test compilation завершились. |
| Sphinx -W --keep-going на upstream-кандидате | SUCCESS, без предупреждений. |

Команды, timestamps, exit codes и длительности последовательных финальных проверок — [final-pipeline-results.json](implementation-evidence/final-pipeline-results.json). Полный набор методов JUnit всего Trino не запускался: выполнены перечисленные regression classes, config tests и planner fixtures; verify запускается с -DskipTests, а JUnit отдельно. SpotBugs/PMD не выдаются за выполненные проверки, когда профиль проекта их пропускает.

На исходной базе ранее также прошли verify и Error Prone трёх модулей; финальный повтор также прошёл на дереве поверх обновлённого upstream с последними изменениями. Ошибки промежуточных прогонов сохранены в evidence, включая исправленный return type workload helper и запуск surefire без initialize. Они не заменяют итоговый успешный прогон.

Окружение: Oracle JDK 25 и Maven wrapper; точные flags, зависимости и проверяемые SHA — [validation-environment.json](implementation-evidence/validation-environment.json). Обновлённые upstream dependency modules собраны в отдельном worktree, для неизменённых dependencies использованы существующие build outputs. Это не clean build всего monorepo.

Целостность исходников и сохранение пользовательской ветки подтверждены в [final-source-integrity.json](implementation-evidence/final-source-integrity.json): candidate software tree равен объединённому snapshot исправлений, исключены только private ai-docs; reverse-check patch и git diff --check проходят. Контрольные суммы новых материалов — [implementation-evidence/SHA256SUMS](implementation-evidence/SHA256SUMS); исходный evidence/SHA256SUMS сохранён и проверен отдельно.
