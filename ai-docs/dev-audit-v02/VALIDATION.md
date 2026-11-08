# Выполненные проверки и воспроизведение

> Историческое состояние до исправлений: `773afdee3bd`. Текущие изменения и результаты проверок: [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md), [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md).

Аудировался HEAD `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`, рабочее дерево перед аудитом было чистым. Production-код не исправлялся. Отрицательные tests специально проверяют ожидаемый контракт и падают на найденном дефекте; такие падения не скрыты под зелёным `assertThrows`.

## Окружение

- Oracle JDK `25.0.5.0.3+1`, `/home/ivan/tools/smj-oracle-jdk25`.
- `./mvnw`, модульные запуски с уже подготовленными зависимостями workspace.
- `MAVEN_OPTS=-Xmx2g`.
- `JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'`.
- `-Dmaven.compiler.useIncrementalCompilation=false -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs`.

Последние флаги ограничивают запуск выбранными tests; это не полный CI verify. Флаги JVM сохранены для сопоставимости с предыдущими рабочими прогонами. Файлы `hs_err_pid*.log` не открывались.

## Матрица полученных результатов

| Запуск | Результат | Свидетельство |
|---|---|---|
| Штатный core regression, исходники HEAD | 134 tests, 0 failures/errors, 6 expected skips | [core-regression.log](evidence/core-regression.log) |
| Новые SQL/JSON probes | 2 tests, 2 errors: FULL repeated symbol, JSON subtype | [statistics-and-criteria.txt](evidence/statistics-and-criteria.txt) |
| Новый group replay sweep | 1 test, 1 assertion failure; все строки трёх сценариев проверены | [replay-amplification.txt](evidence/replay-amplification.txt), [JSON](evidence/replay-amplification.json) |
| Новые memory/yield probes | 2 tests, 2 assertion failures | [memory-and-yield.txt](evidence/memory-and-yield.txt) |
| Новый expression rewrite probe | 1 test, 1 assertion failure | [marker-rewrite.txt](evidence/marker-rewrite.txt) |
| Iceberg: null sort ID + существующие sortedness/multi-file oracle | 3 tests, 1 failure на null-ID, 2 passed | [iceberg-null-sort-order.txt](evidence/iceberg-null-sort-order.txt) |
| Существующий TestIcebergConfig, без правок | 3 tests, 2 failures, 1 passed | [iceberg-config.txt](evidence/iceberg-config.txt) |
| Distributed INNER/RIGHT dynamic-filter contract | 1 test, 1 failure на незавершённый RIGHT DF; INNER control прошёл | [right-dynamic-filter.txt](evidence/right-dynamic-filter.txt), [EXPLAIN/лог](evidence/right-dynamic-filter.log) |

Число SQL-вариантов внутри одного JUnit test не равно числу JUnit tests. `testRepeatedJoinSymbol` успел сравнить INNER/LEFT/RIGHT с hash oracle и упал на FULL. На replay sweep проверены три размера; последний assertion ограничивает read/write, а не проверяет результат вместо него.

## Повторный core regression

```bash
export JAVA_HOME=/home/ivan/tools/smj-oracle-jdk25
export PATH="$JAVA_HOME/bin:$PATH"
export MAVEN_OPTS=-Xmx2g
export JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'

./mvnw -pl core/trino-main test \
  '-Dtest=TestSortingSpiller,TestOrderByOperator,TestMergeJoinCostModel,TestDetermineJoinAlgorithm,TestMergeJoin,TestMergeJoinOperator,TestColocatedJoin,TestPagesIndex,TestOptimizerConfig,TestDiskBackedMatchBitmap,TestAppendingSpiller,TestFileSingleStreamSpiller,TestJoinNodeAlgorithm' \
  -Dmaven.compiler.useIncrementalCompilation=false \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs
```

Core regression запущен после восстановления затронутых существующих test-файлов к HEAD. Audit-классы исключены явным списком `-Dtest`. Успешный итог записан в отдельный лог, поэтому его нельзя спутать с запусками отрицательных probes.

## Негативные проверки

Все новые Java tests сохранены в [repro/regression-probes.patch](repro/regression-probes.patch). Patch применим к указанному HEAD; его применимость проверена через `git apply --check`. Порядок установки и удаления — в [repro/README.md](repro/README.md).

```bash
./mvnw -pl core/trino-main test \
  '-Dtest=TestMergeJoinAuditV02,TestMergeJoinOperator#testAuditV02ReplayAmplification+testAuditV02OutputMemoryAccounting+testAuditV02YieldDuringRejectedGroup,TestMergeJoinMarkerAuditV02' \
  -Dmaven.compiler.useIncrementalCompilation=false \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs

./mvnw -pl plugin/trino-iceberg test \
  '-Dtest=TestIcebergSortedWriting#testAuditV02NullSortOrderId+testSortednessReportedToEngine+testSortedMergeJoinAcrossMultipleFiles' \
  -Dmaven.compiler.useIncrementalCompilation=false \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs

./mvnw -pl plugin/trino-iceberg test -Dtest=TestIcebergConfig \
  -Dmaven.compiler.useIncrementalCompilation=false \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs

./mvnw -pl plugin/trino-iceberg test \
  '-Dtest=TestMergeJoinDynamicFilterAuditV02#testRightMergeJoinCompletesDynamicFilter' \
  -Dmaven.compiler.useIncrementalCompilation=false \
  -Djunit.jupiter.execution.parallel.enabled=false -Dair.check.skip-all -DskipITs
```

В фактическом аудите core probes запускались несколькими командами, соответствующими файлам логов; команда выше объединяет их для повторения. На текущем HEAD ожидаются описанные в REPORT failures. После исправлений tests должны проверять тот же контракт и проходить.

При первом конструировании memory probe вместо компактного RLE была случайно создана обычная LongArrayBlock-страница, которая ожидаемо не поместилась в 16 КиБ ещё на build.addInput(). Обвязка исправлена на RLE и тест повторён. В отчёт включён только повтор с измеренным output builder; первоначальный сбой не считается дефектом реализации.

## Интерпретация памяти и I/O

Replay-измерения используют `SpillerStats.getTotalSpillReadBytes()`: это логические сериализованные байты, прошедшие через reader, включая страницы поиска от checkpoint. Они не измеряют storage-device bytes или cache misses. Рост этого счётчика показывает дополнительную работу алгоритма даже при горячем OS page cache.

Memory probe читает `PageBuilder.getRetainedSizeInBytes()` из реального оператора во время фильтрации и сравнивает с task aggregate user reservation. Небольшая reflection-обвязка нужна только для наблюдения private builder, не подменяет его работу. Probe не профилирует полный heap и не доказывает unbounded growth.

Null-ID probe изменяет metadata record через Iceberg API после настоящей записи данных. Reflection выставляет nullable поле у копии DataFile; после commit файл перечитывается через planFiles() и null проверяется снова. Никакие повреждённые Parquet bytes не создаются. Проверяется допустимое отсутствие необязательного свойства сортировки.

JSON probe создаёт настоящий класс MergeJoinStatistics через его private constructor, поскольку публичного snapshot constructor нет, и использует штатный Airlift JsonCodec<OperatorInfo>. Проблема JSON не выводится из одного отсутствия аннотации, а воспроизводится сериализацией и обратным чтением.

## Что здесь не перепроверялось

Тяжёлые 10M×10M/2³¹ output tests, macro calibration и 103 TPC-DS planning cases повторно не запускались: их сохранённые результаты v01 рассмотрены отдельно. TPC-DS golden printer не включает SortNode/exchange ordering и не является полным fingerprint физического плана. В исходном функциональном аудите полный reactor CI, Error Prone, distributed failure/restart injection, 200–400 partition performance gate и concurrent-query benchmark не выполнялись. Результаты последующего запуска module verify и Error Prone по запросу о developer guides приведены в [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md). Эти модульные проверки не равнозначны полному upstream CI.

Временные Java probes отформатированы Airstyle. После сохранения patch исходники восстановлены; итоговый scope изменений должен содержать только `ai-docs/dev-audit-v02/`.
