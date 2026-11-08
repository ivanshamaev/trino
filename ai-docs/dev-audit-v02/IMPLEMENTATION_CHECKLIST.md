# Доработки по аудиту v02

Начало: 2026-11-08, исходная ревизия `773afdee3bd`. Отчёты аудита сохраняют состояние до исправлений; результаты доработок фиксируются здесь и в `implementation-evidence/`.

## Последовательность исправлений

- [x] V02-01: безопасная обработка отсутствующих/неизвестных sort IDs и compatibility tests Iceberg, включая SMJ OFF.
- [x] V02-02 / G-02: description, defaults/mappings Iceberg config и публичное описание свойств.
- [x] V02-03: нормализация физических ключей сортировки без удаления JOIN constraints, зеркальные SQL regressions.
- [x] V02-04: точная позиция replay с ограниченной памятью; линейные sweeps для групп 2/3/4 страницы.
- [x] V02-05: cooperative yield на scan/discovery/replay/drain; continuation/cancel regressions.
- [x] V02-06: сериализация, aggregation и final query info статистики SMJ.
- [x] V02-07: учёт output memory и документированный reservation/heap contract.
- [x] V02-08: штатная startup cleanup вспомогательных spill-файлов.
- [x] V02-09: сохранение algorithm при expression rewrite и проверка phase contract.
- [x] V02-10: согласованные dynamic-filter producers/consumers RIGHT join и distributed regression.
- [x] G-04 / G-06 / G-07: cleanup tests, readability, Error Prone main/test compilation.

## Интеграция и подготовка upstream

- [x] Compatibility matrix Iceberg: sort evolution, transforms, deletes, несколько splits, TopN/Window/aggregation, SMJ OFF.
- [x] Изоляция общего SortingSpiller либо отдельные benchmarks/regressions обычного ORDER BY.
- [x] Cost model согласована с replay и явно обозначена как локальная эвристика; scope EXPLAIN costs документирован.
- [x] Distributed нагрузочные проверки: пороги, skew, chains, memory, concurrency, delayed branches; результаты без необоснованных заявлений об ускорении.
- [x] G-03: пользовательская документация supported types/joins/retry/distribution, properties, spill, metrics и release-note proposal.
- [x] Штатные config tests, расширенные module regressions, CI verify и main/test Error Prone для изменённых модулей; точный scope прогонов указан в IMPLEMENTATION_RESULTS.md.
- [x] G-01 / G-05: подготовлена reviewable история/разбиение предлагаемого PR с проходящими commit messages и buildable commits.
- [x] Кандидат собран и проверен поверх upstream `8ae1f91f853`; опубликованная история smj-plan сохранена.
- [x] Статус проекта уточнён автором: личный MVP; Trino CLA не подписан, upstream scope не согласован. Эти действия относятся к будущему upstream submission и не входят в завершение текущего MVP.

## Принципы проверки

Исправления не считаются завершёнными только по изменению кода. Для каждого пункта нужны проверка и запись её результата. История опубликованной ветки и внешние организационные сведения не подменяются локальными предположениями. `hs_err_pid*.log` не читаются.

## Проверенные результаты

- `core-third.log`: package SUCCESS; 147 тестов, 0 failures/errors, 6 предусмотренных skips. Включены регрессии повторных ключей, marker rewrite, yield, output reservation, JSON/merge stats, spill replay, sorter и обычный ORDER BY.
- Replay sweep: 768/3072/12288 страниц, группы 2/3/4 страницы, все строки проверены; read/write остаётся в диапазоне 2.39–2.89 вместо роста до 66.11. Индекс записывает точные offsets на диск только для AppendingSpiller.
- `docs-build.log`: Sphinx HTML build с `-W --keep-going` прошёл.
- Первый Iceberg-прогон: TestIcebergConfig 3/3 прошли; null-ID SELECT дошёл до oracle, где обнаружен и исправлен несовместимый с H2 синтаксис ожидаемого результата. Расширенная матрица запущена отдельно.
- Upstream master `8ae1f91f85313e50fd72286810c52f4b058b1292`: база отстаёт на 53 коммита; merge-tree исходного HEAD завершился без конфликтов. Это пока не проверка сборки объединённой ревизии.

- `iceberg-regressions-retry.log`: 13 тестов, 0 failures/errors/skips. Проверены current/null/0/unknown и mixed file IDs, sort evolution, transform prefix, deletes, TopN/Window/aggregation OFF, несколько splits, four join types, RIGHT DF completion и final operator info. Подготовка полных Avro metadata records исправлена после первого тестового прогона.
- Bounded sort выделен признаком SortNode.boundedSpill, который выставляет только SMJ planning; обычный ORDER BY сохраняет legacy merge. Копирование, rewrite и JSON нового признака проверены core-набором.

- Первый `verify -Pci` остановился на одном Checkstyle EmptyBlock в новом memory test. Цикл заменён полной проверкой 20 000 выходных строк и прогресса; дополнительно очищены три избыточных FQN вне первых диагностик аудита. Это не считается успешным CI-прогоном; повтор выполняется после форматирования.
- Добавлены scheduler regressions для всех четырёх join types, ограниченного build scan и отмены незавершённого group discovery.
- Дополнительные нагрузочные проверки собраны в TestMergeJoinWorkloads и включаются `-Dsmj.validation=true`; обычный CI не создаёт их широкие таблицы.

- `ci-verify-retry.log`: verify SUCCESS во всех трёх модулях (7 мин 04 с), JUnit намеренно skipped. После этого дополнены только descriptions/комментарий, legacy bitmap cleanup и тестовые параметры sorted_by. Финальные проверки этих изменений выполняются отдельно.
- `errorprone.log`: clean main/test compilation SUCCESS во всех трёх модулях (11 мин 21 с), без подавления диагностик. Это проверка всего source set каждого модуля, включая ранее не достигавшуюся core test compilation.
- Подготовлена локальная `smj-upstream-v02` поверх `8ae1f91f853`: четыре логических коммита, без ai-docs, без изменения/публикации истории smj-plan. Все четыре сообщения проходят официальный checker через --message-file. В широком диапазоне от старого origin/master есть отдельный upstream dependency-bot commit с длинным body; он не входит в предлагаемые четыре коммита.
- Первый review-коммит и обновлённые upstream dependencies собираются (`review-stage-1-build.log`). Проверка остальных промежуточных деревьев продолжается.

- `core-expanded-regressions.log`: 272 теста, 0 failures/errors, 6 предусмотренных skips. Включены все изменённые core Test* классы и одноимённые связанные classes; scheduler, stats, costing, predicate extraction, dynamic-filter service, все v02 regressions. Scheduler regressions для четырёх join types, bounded scan и отмены discovery прошли.
- `review-stage-1-tests.log`: 19 тестов, 0 failures/errors/skips, включая native spill, indexed append/replay и startup cleanup новых/legacy bitmap names с сохранением постороннего файла.
- Прямой surefire:test без initialize сначала не запускал tests из-за невычисленной surefireEnablePreview. Повтор через initialize surefire:test использует штатный build-helper; startup logs сохранены отдельно и не являются дефектом Java/SMJ.

- `iceberg-expanded-regressions.log`: полный TestIcebergConfig + TestIcebergSortedWriting — 17 тестов, без failures/errors, 2 opt-in skips. Проверены свежие Error Prone-compiled classes.
- Публичные ConfigDescription/session descriptions уточняют модельную (половина per-node limit) allowance и исключение small-build gate для совместимо распределённых отсортированных входов. Production Java больше не ссылается на исключённый из upstream research document.

- Первый distributed workload выявил ошибочное предположение приёмки AUTO для сильно сжимаемого VARCHAR: TableStatisticsReader берёт column_sizes на диске и умножает на 2.7, а не получает logical string lengths. На 750 000 строк по 512 байт FORCE проходит при 128MB/node; AUTO с этой приблизительной статистикой оставляет hash и превышает лимит. Это воспроизведённое ограничение исходных статистик, а не неправильный SQL-результат merge. Сценарий сохранён явно; отдельный high-entropy workload проверяет ресурсную пользу FORCE/AUTO при более информативных оценках. Ограничение отражено в public docs. Не вводится произвольный множитель, имитирующий знание logical width.

- Дополнен контракт replay cache: попадание во второй слот поднимает страницу в основной. Теперь освобождение необязательного слота ради output buffer сохраняет текущую обрабатываемую страницу зарезервированной и не удерживает её незаметно в локальном chunk cursor. Финальный кандидат проверен повторно: core regressions, CI verify и Error Prone прошли.
- Причина AUTO/compressible failure подтверждена исходником TableStatisticsReader: data_file.column_sizes является on-disk size, а оценка VARCHAR использует фиксированный множитель 2.7. Это не upper bound logical/retained bytes; точную длину произвольной строки из этой метрики восстановить нельзя. Публичный контракт AUTO уточнён, compressible counterexample сохранён как явный сценарий, high-entropy case отдельно требует успешного AUTO. Sweeps теперь привязаны к фактически сообщаемым input estimates, а не к предположению logical width=reported width.

- `review-stage-3-tests.log`: финальный replay-cache fix проверен теми же 272 core tests (0 failures/errors, 6 skips).
- `review-stage-4-build.log`: финальное upstream-дерево собирается (Iceberg и trino-tests). Ошибка return type во вспомогательном workload method исправлена; первый лог сохранён с suffix `.first`.
- `review-iceberg-tests-workloads.log`: 22 tests, 0 failures/errors, 2 opt-in skips. Реальный distributed workload: wide FULL 750k × 512 bytes, hash OFF превышает 128MB/node, FORCE и COST_BASED/MERGE проходят с проверкой checksum/count. Все 27 threshold runs и 6 concurrent chain/skew runs проходят. Compressible counterexample AUTO/hash memory-limit воспроизведён отдельно и документирован, не скрыт как успешный hash execution. Machine-readable measurements: `distributed-workloads-final.json`.

- `review-tpcds-plans.log`: 206 planner fixture tests, 0 failures/errors/skips. Это не SF-1000 runtime benchmark и не полный fingerprint physical plan.
- `review-ci-verify.log`: SUCCESS в core, Iceberg, trino-tests за 6 мин 48 с; Checkstyle без нарушений, Airstyle и включённые profile checks прошли. JUnit запускается отдельно; SpotBugs/PMD skipped согласно конфигурации проекта.

## Завершение технических доработок

Все V02-01–V02-10 и технические G-01–G-07 закрыты в документированном experimental scope. Финальный `review-errorprone.log`: clean main/test compilation SUCCESS во всех трёх модулях, 10 мин 56 с. `review-docs-build.log`: Sphinx `-W --keep-going` SUCCESS. Команды и exit codes — `final-pipeline-results.json`; итоговый разбор — [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). По уточнению автора это личный MVP. CLA не подписан, scope с upstream не согласован; эти условия остаются для будущего submission и не блокируют завершение текущего MVP.
