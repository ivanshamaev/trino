# Проверки третьего аудита

Дата: 2026-11-08. Production snapshot: `0393e45fcf954473db34a33ecebe587c8cfff94d`, `smj-plan`. Все новые прогоны выполнялись в detached worktree `/tmp/trino-smj-audit-v03-check`. Исходники пользовательской ветки не редактировались. Audit tests сохраняются отдельным [patch](repro/regression-probes.patch), инструкция — [repro/README.md](repro/README.md).

## Окружение и воспроизводимость

```bash
export JAVA_HOME=/home/ivan/tools/smj-oracle-jdk25
export PATH="$JAVA_HOME/bin:$PATH"
export MAVEN_OPTS=-Xmx3g
export JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'
```

Использован Oracle JDK `25.0.5.0.3+1`, Maven wrapper. `mvnd` отсутствует; DEVELOPMENT.md допускает wrapper. Worktree имеет собственные outputs трёх изменённых модулей; 45 dependency targets переиспользованы через symlinks. Это не clean build всего monorepo. Точный список — [build-environment.json](evidence/build-environment.json). Flags выбраны для стабильного запуска в данной среде, а не для сравнения производительности JVM.

Свежий обычный `package -DskipTests -Dair.check.skip-all=true` для `core/trino-main,plugin/trino-iceberg,testing/trino-tests` прошёл за 3 мин 47 с: [build.log](evidence/build.log). В нём заново скомпилированы production и test sources этих модулей. Затем добавлены audit probes и выполнены отдельные format/test-compile/test commands.

Все surefire-запуски идут через `initialize surefire:test`, чтобы штатный build-helper вычислил preview flag. `-Dair.check.skip-all=true` означает, что эти команды не являются CI-style verify или Error Prone. Новые Java probes форматировались через `airstyle:format`.

Полные argv, exit codes, длительности и имена логов:

- [final-test-results.json](evidence/final-test-results.json): исходный regression suite, большой прогон, первые окончательные негативные probes;
- [additional-test-results.json](evidence/additional-test-results.json): ordered-exchange SQL и первая scalar matrix;
- [controls-results.json](evidence/controls-results.json): последние независимые controls, codecs/encryption fuzz и scalar matrix;
- [probe-second-results.json](evidence/probe-second-results.json): multi-file SMJ OFF comparisons;
- [junit-results.json](evidence/junit-results.json): фактически запущенные классы и JUnit totals, разобранные из логов.

## Штатные regressions

| Набор | JUnit tests | Failures / errors | Skips | Источник |
|---|---:|---:|---:|---|
| Core, включая дополнительный TestPushPartialAggregationThroughJoin и `-Dsmj.large=true` | 279 | 0 / 0 | 1 | [core-regressions-large.log](evidence/core-regressions-large.log) |
| Iceberg config и выбранные исходные методы SortedWriting | 16 | 0 / 0 | 2 | [iceberg-regressions.log](evidence/iceberg-regressions.log) |

Core selection — [baseline-selection.json](evidence/baseline-selection.json). В двух расширенных audit-классах исходные методы выбраны явно через `Class#method+method`, чтобы новые заведомо падающие probes не изменили результат штатного набора. `TestEliminateCrossJoins` совпадает с двумя классами разных packages; фактически запущено 30 core-классов. Дополнительное имя `TestPushDownDereferenceThroughJoin` не нашло отдельного класса и не считается проверенным тестом.

В большой проверке действительно выполнены пять отдельных сценариев с 10 млн build rows: INNER/LEFT/RIGHT/FULL на последовательных ключах и одна огромная build-группа с одной probe-строкой. Проверены результаты и managed reservations при 16MB query limit. Это не 10M×10M many-to-many output, не измерение JVM heap и не gate 15B×17B. Единственный core skip — extreme FULL scenario с выводом более двух миллиардов строк. Два Iceberg skips — опциональные calibration/planning workloads.

Iceberg selection не равен всему модулю или всему набору унаследованных методов класса. Здесь 3 config tests и 13 JUnit invocations выбранных SortedWriting methods; исторический полный class-run v02 имел ещё один generic inherited test.

## Новые положительные probes

### Differential fuzz

40 фиксированных seeds × INNER/LEFT/RIGHT/FULL × in-memory/native forced spill = **320 сравнений мультимножеств**. Oracle независимо строит результат вложенными циклами; в ключах BIGINT/NULL, часть seeds использует составной ключ. Есть повторяющиеся группы, residual `(leftPayload + rightPayload) % 7 < 3` и случайные границы pages. Фактические seeds дали 2–189 probe rows и 1–247 build rows; пустые стороны проверяются отдельными штатными tests, а не этим fuzz-прогоном.

Native spiller принудительно активируется при threshold=1; query memory limit 128KiB. Первоначальный завершённый прогон с codec NONE и частью encrypted seeds прошёл: [core-probes-final.log](evidence/core-probes-final.log). Финальный повтор с NONE/LZ4/ZSTD и encryption on/off также прошёл: [positive-controls.log](evidence/positive-controls.log), 320 comparisons. Комбинации codec/encryption распределены по seeds, а не перемножены на все seeds. Порядок строк внутри одинакового ключа и порядок unmatched output не являются oracle: сравниваются мультимножества с сохранением кратности.

### Scalar SQL matrix

Audit probe сравнивает OFF/hash с FORCE и отдельно проверяет `Algorithm: MERGE` в EXPLAIN. Семейства: BOOLEAN; BIGINT extrema; DECIMAL(38,6); CHAR padding; VARBINARY; DATE; TIMESTAMP(12); TIMESTAMP(12) WITH TIME ZONE с равными instant в разных zones; UUID; IPADDRESS, включая IPv4-mapped representation. Четыре join types на семейство — 40 comparisons. Все 40 comparisons прошли: [positive-controls.log](evidence/positive-controls.log). Вместе с fuzz и независимым timestamp baseline control — 3 JUnit tests, 0 failures/errors/skips.

### Iceberg metadata при SMJ OFF

[sortedness-off.log](evidence/sortedness-off.log): пять файлов с перекрывающимися диапазонами ключей; task_concurrency=1/4; сравнение report_sortedness=true/false. Проверены глобальный row_number, partitioned running SUM, GROUP BY и FETCH FIRST WITH TIES — восемь SQL comparisons. Все прошли. Отдельные sorted files не объявляются глобально отсортированным task input.

### Ordered exchange после aggregation pushdown

[ordered-exchange.log](evidence/ordered-exchange.log): distributed SQL с INNER JOIN, residual и SUM; OFF oracle и FORCE без pushdown совпали; затем пять FORCE executions с pushdown совпали с ними. В исследованном плане сохранились MERGE и RemoteMerge. Эта гипотеза **не подтвердила тихую потерю ordering**. Неодинаковые guards у двух вариантов PushPartialAggregationThroughExchange остаются предметом приёмки будущего исправления, а не третьим доказанным дефектом.

### Replay scaling

Свежие девять samples: 768/3072/12288 build pages и группы 2/3/4 pages. Отношение bytes read/write — **2.4053–2.8875**, без прежнего роста с размером build. Данные — [replay-sweep.json](evidence/replay-sweep.json). Все строки сверяются исходными regression methods. Счётчики включают serialized data и offset index; из них нельзя вывести physical device I/O или общий latency speedup.

## Негативные probes и независимые controls

V03-01 воспроизведён локально на VALUES и распределённо на Iceberg: projection в `sum(l.v + 1)` над grouped join сбрасывает algorithm после local planning. С spill=true отказ `spillable not yet set`; с spill=false в локальном runner — `no more sources`. Первичные логи: [core-probes-final.log](evidence/core-probes-final.log), [distributed-aggregation.log](evidence/distributed-aggregation.log).

Финальные probes **сначала** исполняют тот же SQL с OFF и FORCE/pushdown=false, сравнивают результаты и печатают `hashRows=16 forcedNoPushdownRows=16`. Локальные controls прошли для обоих выражений и spill on/off. Затем FORCE/pushdown=true воспроизвёл ошибки на `sum(v+1)`. Эти controls введены отдельно: успешный `sum(v)` не является oracle для упавшего `sum(v+1)`. Итоги — [negative-controls.log](evidence/negative-controls.log) и [distributed-controls.log](evidence/distributed-controls.log).

Финальный core negative run: 3 JUnit tests, 1 failure (plaintext bitmap assertion), 2 errors (SQL runtime failures), exit=1. Финальный distributed run: 1 test, 1 error `spillable not yet set`, exit=1; оба OFF/pushdown=false controls для того же выражения прошли с 16 строками. Это ожидаемое воспроизведение двух замечаний, а не успешная проверка исправленной реализации.

V03-02 воспроизведён с реальной FileSingleStreamSpillerFactory(encryption=true), AppendingSpiller и production DiskBackedMatchBitmap. После вытеснения первого блока из 8192 bits файл 1125 bytes содержит 1024 открытых bytes `0x55`. Проверка ожидаемой конфиденциальности намеренно падает. Это primitive-level воспроизведение именно auxiliary storage path, а не заявление о восстановлении исходных columns или полном end-to-end SQL attack. Основной encrypted data stream не исследуется этим отрицательным probe.

**Негативный прогон с exit code=1 является успешным воспроизведением дефекта, но не зелёной проверкой реализации.** В patch сохранены обычные assertions корректного поведения, чтобы после исправления tests могли стать зелёными без инверсии oracle.

## Ошибки и ограничения тестовой обвязки

1. Первые fuzz-запуски ошибочно требовали точный порядок строк. Законный иной порядок equal-key/unmatched rows давал false failures. Oracle заменён сравнением мультимножеств; исходные helpers production regression tests не переписаны. Сохранены [differential-fuzz-order-assumption.log](evidence/differential-fuzz-order-assumption.log), [core-probes.log](evidence/core-probes.log).
2. Первая scalar matrix прошла шесть семейств, затем упала при materialization TIMESTAMP(12) `1969-12-31 23:59:59.999999999999`: SqlTimestamp.toLocalDateTime округляет до недопустимого NanoOfSecond=1000000000. Финальный probe отдельно воспроизвёл тот же отказ с **OFF и вообще без JOIN**; fixture matrix заменён значением без rounding carry. Это не SMJ finding. Первичный лог — [scalar-types.log](evidence/scalar-types.log).
3. При добавлении выбора codec в audit helper один compile остановился на несогласованном boolean/int параметре. Исправлена только тестовая обвязка. Первичные logs/JSON сохранены с suffix `.initial-compile-failure`; последний format и compile прошли отдельно. Это не ошибка production Java.
4. Не запускался полный monorepo CI, весь SQL corpus или FTE/multi-driver SMJ, которые исключены из scope. Экстремальная cardinality, произвольная concurrent skew и physical storage performance этими тестами не сертифицированы.

## Повторная оценка исправлений v02

| Пункт v02 | Свежая проверка / вывод v03 |
|---|---|
| 01 nullable file IDs | Iceberg compatibility suite прошёл; дополнительно OFF multi-file consumers прошли |
| 02 config | Core/Iceberg defaults/mappings прошли |
| 03 повторные join keys | TestMergeJoinRepeatedKeys и composite fuzz прошли |
| 04 indexed replay | Native spiller/append tests и девять scaling samples прошли |
| 05 yield | Scan/discovery/cancel/rejected-filter regressions прошли |
| 06 metrics | OperatorInfo JSON/merge и исходный distributed query stats test прошли |
| 07 memory | Output/replay reservations и пять 10M scenarios прошли в пределах своего контракта |
| 08 startup cleanup | FileSingleStreamSpillerFactory cleanup tests прошли; encryption bitmap — новый отдельный вопрос |
| 09 expression rewrite | Прежний expression-only regression прошёл; обнаружен другой post-selection projection path, V03-01 |
| 10 RIGHT dynamic filter | Distributed RIGHT test прошёл; новых зависаний DF не наблюдалось |

Исторические `verify -Pci`, main/test Error Prone, Sphinx и 206 TPC-DS fixture tests последнего кандидата v02 описаны в [IMPLEMENTATION_RESULTS.md](../dev-audit-v02/IMPLEMENTATION_RESULTS.md). В v03 они заново не запускались и не выдаются за новые успешные проверки HEAD. При исправлении найденных дефектов нужны свежие соответствующие gates. Состояние истории/гайдов — [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md).

Контрольные суммы исходного production snapshot — [source-SHA256SUMS](evidence/source-SHA256SUMS); итоговая целостность материалов — [SHA256SUMS](evidence/SHA256SUMS). `hs_err_pid*.log` не читались и не хешировались.
