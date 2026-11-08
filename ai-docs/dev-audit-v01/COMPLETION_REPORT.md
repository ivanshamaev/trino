# Результаты доработки SMJ по аудиту v01

Дата: 2026-11-08. Все пункты [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md) закрыты проверками. Исходный [REPORT.md](REPORT.md) сохранён как описание состояния до исправлений. `prefer_merge_join=OFF` остаётся значением по умолчанию; COST_BASED доступен при явном включении.

## Результат реализации

Исправлены ошибки на границах probe/build/output страниц, обработка повторяющихся групп, unmatched RIGHT/FULL, порядок после repartition и lifecycle bridge. Build читается из spill лениво; завершённые батчи доступны probe до окончания build. Групповые позиции и счётчики используют long. Отметки совпадений ограничены текущей группой и могут храниться на диске. Для RIGHT/FULL без residual filter исключены ненужный bitmap и третий проход по matched-группе.

Canonical SortNode получил режим MergeSortedPages: порядок проверяется внутри каждой страницы, затем страницы объединяются в существующий адресный индекс. Некорректный порядок или недостаток памяти курсоров включает обычную comparison sort. Свойство сортированности одного Iceberg-файла используется для этого проверяемого режима. Общий порядок потока строится физическим merge; вход может буферизоваться и spill. EXPLAIN и runtime-метрики показывают выбранный путь.

Широкий macro дополнительно обнаружил неограниченный fan-in штатного OrderBy spill: одновременное чтение всех ранов удерживало сотни МиБ. Добавлен SortingSpiller с попарным online-слиянием одноуровневых ранов, spill-страницами около 64 КиБ и одним финальным файлом. Промежуточные файлы сразу закрываются, количество файлов/буферов растёт логарифмически. Завершение и отмена работают через futures. Этот общий spill-путь используется также обычным OrderBy при включённом spill; дополнительные проходы I/O учтены в модели стоимости.

COST_BASED учитывает строки/байты на задачу, workers, NDV, совместимое partitioning, сортировку/merge страниц, сеть, spill/replay и память. Проверка hash учитывает загрузку целой локальной партиции при unspill и отсутствие hash spill для RIGHT/FULL. Неизвестные row count/NDV, выключенный spill и малый build сохраняют hash. Включена проверяемая session property merge_join_cost_margin; параметры и допущения описаны в [COST_MODEL.md](COST_MODEL.md).

## Проверки

| Проверка | Результат |
|---|---|
| Последняя core-регрессия | 134 теста, ошибок нет, 6 ожидаемых пропусков опциональных больших сценариев |
| Полный FULL JOIN одной группы 2³¹ строк | Проверены все 2 147 483 648 выходных строк, peak task memory ≤16 МиБ; 846,5 с |
| Ранее выполненная большая операторная матрица | 10M×10M для INNER/LEFT/RIGHT/FULL и FULL одной группы 10 млн; реальный spill, каждая строка проверена, peak task memory ≤16 МиБ, линейное чтение spill |
| Реальные spill-файлы сортировки | 129 перекрывающихся ранов, 132 096 проверенных строк, учтённая память ≤1 МиБ, не более 16 файлов; после close резерв и каталог пусты |
| Асинхронность sorter | Успешное ожидание finish, отмена записи и начавшейся compaction, ошибка compaction; ресурсы освобождаются |
| Iceberg | 4 теста: macro, INNER/FULL oracle на перекрывающихся файлах при scan partitioning true/false, sortedness и кеш на 128 файлах |
| TPC-DS SF-1000 | 103 query parts, ошибок нет; фиксированный query memory limit 16 ГиБ/узел, 8 задач, task_concurrency=1 |
| Стиль и статические проверки | Airstyle для 4284 core, 494 Iceberg и 137 testing Java-файлов; Modernizer трёх модулей; git diff --check — успешно |

В тесте 2³¹ строк вход задан двумя компактными RLE-страницами и не использует spill; полный вывод действительно сформирован и проверен. Способность работать с физическим build больше RAM отдельно подтверждена реальным spill на 10 млн строк. Эти проверки покрывают разные ограничения.

## Калибровка выбора

DistributedQueryRunner: три узла, task_concurrency=4, partitioned join, spill включён. На каждой стороне 1 млн строк. Sorted/unsorted/skew используют VARCHAR payload 128 байт и лимит 64 МиБ на узел; FULL — payload 512 байт из salted SHA-512 и лимит 128 МиБ. Для теста sink/local exchange buffers ограничены 4 МиБ, memory-revoking-threshold=0.02, target=0. Один прогрев и три измерения, другие тестовые наборы во время замеров не выполнялись. Результат проверяется count/checksums против hash-oracle с большим бюджетом.

| Сценарий | OFF/hash, с | FORCE/merge, с | COST_BASED, с | Автоматический выбор |
|---|---:|---:|---:|---|
| Sorted | 1,789 | 5,615 | 1,736 | HASH |
| Unsorted | 1,734 | 5,993 | 1,725 | HASH |
| Skew, 128 build-ключей | 1,086 | 5,109 | 1,069 | HASH |
| FULL, широкий build | OOM при 128 МиБ | 13,589 | 13,523 | MERGE |

COST_BASED сохраняет быстрый hash в трёх проходящих сценариях и выбирает bounded merge, когда hash не помещается. Значения порогов 0.7/0.3/1.0 и margin 0.1 сохранены как консервативные. Сырые времена, memory/spill stats и параметры находятся в [CALIBRATION_RESULTS.json](CALIBRATION_RESULTS.json). Peak user memory из QueryStats относится ко всему распределённому запросу; его нельзя напрямую сравнивать с лимитом одного узла.

## Изменения планов TPC-DS

Для 100 query parts физический план совпадает с OFF. Для q04, q11 и q74 зафиксированы отдельные ожидаемые планы с одним верхним MERGE; порядок JOIN и сканов сохранён. Сырые build bytes по статистике планировщика: q04 — 8,512 ТБ суммарно (~1,064 ТБ на задачу), q11/q74 — 253,324 ГБ (~31,666 ГБ на задачу). Уже эти оценки превышают лимит 16 ГиБ; локальная hash-партиция при task_concurrency=1 не помещается.

Это проверка планов на существующих Iceberg metadata fixtures. Выполнение TPC-DS SF-1000 и фактический размер этих промежуточных результатов не измерялись. Использована более крупная доступная fixture SF-1000 вместо упомянутой в исходном checklist SF-100.

## Границы выводов и воспроизведение

Стоимость остаётся статистической эвристикой. NDV не описывает точную частоту hot keys; Iceberg выводит оценку размера VARCHAR из размера на диске, поэтому сжимаемость влияет на прогноз. Конкурирующие запросы, огромная отдельная строка и бюджеты всех upstream-операторов также влияют на выполнимость. Runtime-учёт памяти и контролируемое отклонение превышения лимита сохраняются. Локальные времена и metadata latency не являются CI-порогами или универсальной оценкой производительности.

В итоговых проверках использован Oracle JDK 25.0.5.0.3+1, SerialGC, TieredStopAtLevel=1 и отключённые compressed oops. Проверка полного вывода 2³¹ строк прошла на Temurin 25.0.5+8 с теми же флагами. Первые прогоны Temurin с query runner прерывались авариями JVM; для остальных проверок использована другая сборка JDK 25. Файлы hs_err_pid*.log не читались.

Команды для подготовленного workspace с установленными Maven-зависимостями:

```bash
export JAVA_HOME=/home/ivan/tools/smj-oracle-jdk25
export PATH="$JAVA_HOME/bin:$PATH"
export JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'

./mvnw -pl core/trino-main test \
  -Dtest=TestSortingSpiller,TestOrderByOperator,TestMergeJoinCostModel,TestDetermineJoinAlgorithm,TestMergeJoin,TestMergeJoinOperator,TestColocatedJoin,TestPagesIndex,TestOptimizerConfig,TestDiskBackedMatchBitmap,TestAppendingSpiller,TestFileSingleStreamSpiller,TestJoinNodeAlgorithm \
  -Dair.check.skip-all -DskipITs

./mvnw -pl core/trino-main test \
  '-Dtest=TestMergeJoinOperator#testFullJoinGroupAboveIntegerRangeWithBoundedMemory' \
  -Dsmj.extreme=true -Dair.check.skip-all -DskipITs

./mvnw -pl core/trino-main install -DskipTests -Dair.check.skip-all -DskipITs
./mvnw -pl plugin/trino-iceberg test \
  '-Dtest=TestIcebergSortedWriting#testMergeJoinCostCalibration+testSortedMergeJoinAcrossMultipleFiles+testSortednessReportedToEngine+testSortednessMetadataPlanningCost' \
  -Dsmj.calibration=true -Dsmj.large=true \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs

./mvnw -pl testing/trino-tests test \
  '-Dtest=TestPartitionedTpcdsCostBasedPlan#testCostBasedMergeTpcdsPlans' \
  -Djunit.jupiter.execution.parallel.config.strategy=fixed \
  -Djunit.jupiter.execution.parallel.config.fixed.parallelism=2 -Dair.check.skip-all -DskipITs

./mvnw -pl core/trino-main,plugin/trino-iceberg,testing/trino-tests airstyle:check modernizer:modernizer
```
