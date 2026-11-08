# Соответствие CONTRIBUTING.md и DEVELOPMENT.md

> Историческое состояние до исправлений: `773afdee3bd`. Текущие изменения и результаты проверок: [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md), [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md).

Дополнение к аудиту v02 от 2026-11-08. Проверены локальные [CONTRIBUTING.md](../../.github/CONTRIBUTING.md), [DEVELOPMENT.md](../../.github/DEVELOPMENT.md), [PR template](../../.github/pull_request_template.md) и закреплённая в CI политика commit messages. Ревизия реализации: `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`, база `origin/master = e112ffdf7304333b50bfdff50dda4229c897896d`.

## Вывод

**Ветка пока не соответствует всем условиям подготовки upstream PR.** Существуют независимо проверяемые нарушения: отклоняемые CI сообщения коммитов, ошибки Error Prone в SMJ-коде, неполное оформление нового Iceberg config и падающие config tests. Не подготовлена публичная документация функциональности и свойств. При этом успешный обычный verify не означает прохождения отдельного Error Prone profile.

Ниже разделены обязательные проверки, рекомендации по читаемости и организационные сведения, которые нельзя установить из рабочего дерева. Правила не требуют переписывать каждый getter, запрещать все ternary expressions или добиваться зелёного SpotBugs вопреки отключающей его конфигурации проекта.

Это отдельное дополнение: функциональные V02-01–V02-10 в [REPORT.md](REPORT.md) не перенумерованы и не заменены замечаниями по оформлению.

## Подтверждённые несоответствия

### G-01. История коммитов не проходит закреплённую CI-проверку

На диапазоне `origin/master..HEAD` выполнен **тот же check.py**, что использует `.github/workflows/ci.yml:65`, из `airlift/github-actions@1652e24519f5c7cec59d841070bf9beffadabdf9`. Скрипт завершился с exit code 1.

Из 30 коммитов:

- 16 имеют subject длиннее обязательного предела 60 символов;
- один — `71cbe8ca3d9`, `codex some fixes` — начинается со строчной буквы;
- всего 17 коммитов отклонены скриптом;
- 21 subject длиннее рекомендуемых 50 символов; это более строгая рекомендация, её нельзя смешивать с пределом 60;
- последний `773afdee3bd`, `Complete bounded sorted merge join implementation`, укладывается в правила.

Примеры длинных subjects: `ef5574afbf4` и `508a623f202` — по 105 символов, `515dded0a3e` — 90. Полный результат, включая причины для каждого коммита: [guide-commit-check.log](evidence/guide-commit-check.log). Длины всех subjects: [guide-commit-subjects.json](evidence/guide-commit-subjects.json).

Исправление перед PR — подготовить reviewable историю с короткими imperative subjects и осмысленными descriptions. Это не повод автоматически выполнять force-push текущей пользовательской ветки. Кроме того, tool-centric subject `codex some fixes` не объясняет изменение и плохо соответствует правилам о содержании сообщений. AI-attribution не следует добавлять в переписанные сообщения. [Закреплённая политика CI](https://github.com/airlift/github-actions/blob/1652e24519f5c7cec59d841070bf9beffadabdf9/check-commit-messages/README.md), [правила Trino для commits](https://trino.io/development/process.html#pull-request-and-commit-guidelines).

Промежуточные коммиты по отдельности не собирались. Поэтому утверждение, что вся история buildable, также пока не подтверждено. Успешная сборка HEAD его не заменяет.

### G-02. Новое Iceberg-свойство оформлено не полностью

В [IcebergConfig.java](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergConfig.java):127 у `@Config("iceberg.report-sortedness")` отсутствует `@ConfigDescription`. Это прямое несоответствие [Adding a property](../../.github/DEVELOPMENT.md#adding-a-property).

В [TestIcebergConfig.java](../../plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestIcebergConfig.java) не обновлены recorded defaults и full mapping. В рамках v02 существующие tests воспроизвели `Untested attributes: [ReportSortedness]` и `Untested properties [iceberg.report-sortedness]`. Это нарушение [Testing a new config](../../.github/DEVELOPMENT.md#testing-a-new-config), уже отражённое как V02-02. [Лог](evidence/iceberg-config.log).

Положительная часть: config names используют dashes, session names — snake_case; description у `report_sortedness` присутствует. Пять новых core config setters имеют `@ConfigDescription`, их defaults/mappings включены в `TestOptimizerConfig`, validation annotations расположены на getters. Новых credentials нет; требований security-redaction к этим свойствам не возникает. Переименования/удаления старых properties не обнаружены, поэтому `@LegacyConfig`/`@DefunctConfig` здесь не требуются.

### G-03. Публичная документация и release-note preparation отсутствуют в проверяемом diff

Вся ветка относительно базы не меняет `docs/`. Материалы в `ai-docs/` не подключены к Sphinx reference documentation и не заменяют инструкции для пользователей/администраторов.

Нужны описание supported joins/types/retry modes, значения и default всех новых properties, настройка spill, смысл FORCE/PREFER/COST_BASED, ограничения single-driver и различие file/page order с task-wide order. Особенно важно явно описать влияние `report_sortedness=true` и общего SortingSpiller на обычные запросы при SMJ OFF.

| Config | Session property | Default в коде |
|---|---|---|
| `optimizer.prefer-merge-join` | `prefer_merge_join` | `OFF`; значения enum `OFF`, `PREFER`, `COST_BASED`, `FORCE` |
| `optimizer.merge-join-memory-pressure-threshold` | `merge_join_memory_pressure_threshold` | `0.7` |
| `optimizer.merge-join-small-build-threshold` | `merge_join_small_build_threshold` | `0.3` |
| `optimizer.merge-join-big-side-threshold` | `merge_join_big_side_threshold` | `1.0` |
| `optimizer.merge-join-cost-margin` | `merge_join_cost_margin` | `0.1` |
| `iceberg.report-sortedness` | `<catalog>.report_sortedness` | `true` |

Минимальные места для интеграции: [optimizer properties](../../docs/src/main/sphinx/admin/properties-optimizer.md), [Iceberg connector](../../docs/src/main/sphinx/connector/iceberg.md), разделы join/spill с перекрёстными ссылками. Новые страницы и разделы должны следовать принятой структуре и правилам [docs/README.md](../../docs/README.md), включая порядок разделов там, где применимо.

Для пользовательской возможности требуется предложить release-note text по [PR template](../../.github/pull_request_template.md). Не обязательно самостоятельно редактировать файл следующего релиза: предложенная запись может находиться в PR description. Из локальной ветки наличие такой GitHub PR description не установлено. [Trino release-note guidelines](https://trino.io/development/process.html#release-note-guidelines).

### G-04. Оформление tests требует ручной чистки сверх formatter

У всех 17 новых Java-файлов есть Apache header. Однако [TestJoinNodeAlgorithm.java](../../core/trino-main/src/test/java/io/trino/sql/planner/plan/TestJoinNodeAlgorithm.java):14 содержит второй, повреждённый лицензионный блок. Первый блок корректен, поэтому это не отсутствие лицензии; обычный license check проходит. Второй следует удалить как артефакт генерации.

Там же `testMergeRequiresInnerOrLeftJoin():114` теперь проверяет принятие RIGHT/FULL — имя противоречит содержимому. Javadoc [TestMergeJoin.java](../../core/trino-main/src/test/java/io/trino/sql/query/TestMergeJoin.java):30 продолжает описывать только INNER/LEFT. Это несоответствия consistency и production quality for test code, хотя formatter их не исправляет.

`testAlgorithmPropertyIsSerialized():188` проверяет только аннотацию reflection-ом, не сериализацию. Его нужно либо переименовать по действительной проверке, либо дополнить настоящим round-trip. Distributed e2e уже дают дополнительное свидетельство передачи плана; само наличие этого reflection test не означает провал всей сериализации JoinNode.

### G-07. Обязательная Error Prone compilation падает в SMJ-коде

Выполнена вторая команда из [Building](../../.github/DEVELOPMENT.md#building): `clean test-compile -Dair.check.skip-all=true -Perrorprone-compiler` для трёх изменённых модулей. Она остановилась на production compilation `trino-main`, до компиляции его tests. В первом reactor-запуске Iceberg и trino-tests помечены SKIPPED.

Компилятор сообщает 21 errors. Из лога выделены **20 уникальных source diagnostics**: 19 `UnnecessarilyFullyQualified` и один `RemoveUnusedImports`. Все их paths изменены SMJ-веткой; это не найденные в сторонних зависимостях ошибки.

| Файл | Диагностика |
|---|---|
| `MergeJoinBufferOperatorFactory.java:128` | Необязательный полный путь `io.trino.spiller.Spiller` |
| `MergeJoinOperatorFactory.java:202–241` | Полные имена Optional, OperatorInfo, AtomicLong; 14 отдельных source diagnostics |
| `SortingSpiller.java:135` | Необязательный полный путь IntStream |
| `PropertyDerivations.java:604` | Два необязательных полных пути JoinType |
| `DetermineJoinAlgorithm.java:70` | Неиспользуемый static import Math.max |
| `DetermineJoinAlgorithm.java:244` | Необязательный полный путь Optional |

Нужны обычные imports/простые имена и удаление неиспользуемого import, затем повторение Error Prone для main и test sources всех модулей. Исправления в рамках аудита не применялись. Перечень: [guide-errorprone-diagnostics.json](evidence/guide-errorprone-diagnostics.json), [лог](evidence/guide-errorprone.log). Наличие Airstyle/Checkstyle SUCCESS в обычном verify не отменяет этот blocker.

После восстановления core-артефактов независимый запуск Iceberg подтвердил ещё одну source diagnostic: `TestIcebergSortedWriting.java:177:48`, `UnnecessarilyFullyQualified` для `io.trino.testing.QueryFailedException`. Production compilation Iceberg прошла; test compilation завершилась с FAILURE. Итого в core и Iceberg непосредственно локализована **21 уникальная source diagnostic**. [Лог независимого запуска](evidence/guide-errorprone-remaining.log).

Отдельно выполненная main/test compilation `testing/trino-tests` с Error Prone прошла, с warnings. Это не устраняет ошибки двух остальных модулей. [Лог](evidence/guide-errorprone-testing.log).

## Рекомендации ревью, не автоматические CI-нарушения

### G-05. Слишком широкая граница текущего изменения

В ветке 172 изменённых файла: 93 software/test/fixture files и 79 файлов исследований/планов/аудита. Одновременно изменены физический join, общая сортировка spill, API page replay, Iceberg metadata и CBO. Последний commit содержит сразу многие из этих составляющих.

Это не нарушение выдуманного лимита строк. Но для независимого review и rollback полезно выделить логические границы: bounded external sort; replay primitives; JoinNode/operator integration; Iceberg sortedness; automatic selection. Исследовательские материалы можно сохранить в форке и предложить upstream короткий design document. Не следует считать, что все `ai-docs/` запрещены: речь о scope и затратах ревью. [Процесс Trino рекомендует небольшие логические изменения](https://trino.io/development/process.html#pull-request-and-commit-guidelines).

Деление нужно выполнять так, чтобы каждый промежуточный commit собирался. Отделять механические изменения конструкторов JoinNode от логики следует только там, где это сохраняет buildable последовательность; нельзя применить это правило ценой заведомо некомпилирующего промежуточного дерева.

### G-06. Читаемость и согласованность runtime-кода

В [MergeJoinOperatorFactory.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java) 1269 строк, transformation совмещает множество состояний group discovery, row replay, chunk replay, unmatched drain и input regions. Руководство не устанавливает максимальную длину файла, но эта структура усложняет проверку инвариантов; результаты v02 показывают практическую цену сложности. Полезны явно оформленные состояния/cursors и комментарии к переходам, а не механическое дробление методов ради числа строк.

Конкретные небольшие правки:

- Нетривиальные ternary expressions, например вычисление `hashPeak` в [MergeJoinCostModel.java](../../core/trino-main/src/main/java/io/trino/cost/MergeJoinCostModel.java):73 и выбор replay-reader в [MergeJoinBridge.java](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java):474, лучше выразить обычным ветвлением. Простые условные getters подпадают под разрешённое исключение.
- Новая IOException из `SortingSpiller.close():217` оборачивается в общий RuntimeException. Раздел Categorize errors рекомендует TrinoException с error code. Вместе с тем существующий GenericSpiller.close() использует аналогичный RuntimeException: нужно согласовать конкретный cleanup/error contract, а не объявлять любой такой wrapper безусловной ошибкой.

В горячих row loops новые streams не обнаружены: основные операции используют обычные циклы. Наличие streams при создании comparator/cost inputs само по себе не нарушает рекомендацию. Новых mocking-library imports не найдено; hand-written controlled spillers соответствуют подходу проекта. AssertJ используется, а специализированное сравнение результатов без порядка оправдано тестируемой семантикой.

## Матрица требований DEVELOPMENT.md

| Раздел | Результат проверки |
|---|---|
| Readability / Consistency | Частично: G-04/G-06; автоматические ошибки imports — G-07; formatter не доказывает читаемость state machine |
| Alphabetize | Public docs ещё не добавлены; G-03. Требование не означает алфавитный порядок полей всех Java-классов |
| Use streams | Замечаний к новым row loops по этому правилу не выявлено |
| Categorize errors | G-06: обсудить cleanup exceptions с учётом существующего precedent |
| Add license header | Есть у всех 17 новых Java files; один дубликат, G-04 |
| Prefer String formatting | Контекстная рекомендация; запрета любой конкатенации нет, hot path отдельно защищён от лишнего форматирования |
| Avoid ternary operator | Есть сложные случаи для упрощения, G-06; простые выражения допустимы |
| Avoid get in method names | Большинство новых get-методов — getters/API overrides; массовое переименование не требуется |
| Private inner class API | Blanket-проблема не установлена; private helpers не обязаны становиться public |
| Avoid mocks | Новых mocking-library imports в added diff не обнаружено |
| AssertJ / Airlift Assertions | В основном соблюдено; specialised result oracle допустим |
| Use var judiciously | Новые видимые примеры с builder/stats имеют понятный тип; отдельный blocker не установлен |
| Guava immutable collections | Основные публичные collections копируются через ImmutableList/ImmutableMap; новых Collections.unmodifiable wrappers не найдено |
| Production quality for tests | Частично: G-02/G-04 и функциональные пробелы v02 |
| Avoid abbreviations | Нет отдельного доказанного blocker; стандартные SMJ/NDV и локальные cursor positions оценивать по читаемости |
| Exhaustive enum switch | Добавленный default в calibration переключает String, не enum; запрет неприменим |
| Vector API | Нового vectorized implementation нет; специальные CPU/benchmark gates неприменимы |
| Config names / descriptions / tests | Имена соблюдены; Iceberg description/tests — G-02 |
| Legacy / defunct config | Старые свойства не переименовывались и не удалялись |
| Validation on getters / storing config | Для новых core properties соблюдено; новый config object в runtime fields не хранится |
| POM sorting | POM не менялись; автоматический sortpom входит в verify |
| Building / Error Prone | Результаты фактических запусков приведены ниже |
| Benchmarks | Для microbenchmarks применим JMH с annotation processing из Running benchmarks; distributed SQL calibration может оставаться отдельным workload test |
| IDE setup | Рекомендации по IDE не являются проверяемыми требованиями к branch contents |
| Docs / Web UI / releases | Docs/release preparation — G-03; Web UI code не менялся, Bun build не требуется только из-за SMJ operator type |

## Организационные условия CONTRIBUTING.md

CLA необходим для принятия вклада, но его статус нельзя вывести из наличия license header. Локальный аудит не подтверждает и не опровергает подписанный CLA; проверка остаётся перед merge. [CONTRIBUTING.md](../../.github/CONTRIBUTING.md#contributor-license-agreement-cla).

Для крупной функции руководства рекомендуют предварительное обсуждение и согласование разбиения. Наличие конкретного upstream design issue/обсуждения из проверяемых материалов не подтверждено; это не утверждение, что автор ни с кем не общался. Автоматические сообщения maintainers, создание issue или PR в рамках этой проверки не выполнялись. [Contribution process](https://trino.io/development/process.html#contribution-process).

Текущая локальная база закреплена для воспроизводимости; она не доказывает отсутствие конфликтов с актуальным upstream master. Проверка/перебазирование upstream и прохождение CI каждого предлагаемого commit остаются условиями submission.

## Проверки сборки

Использованы Oracle JDK `25.0.5.0.3+1`, `MAVEN_OPTS=-Xmx3g` и `JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'`. `./mvnw` — прямо разрешённая руководством замена `mvnd`.

```bash
./mvnw verify -DskipTests -Pci \
  -pl core/trino-main,plugin/trino-iceberg,testing/trino-tests

./mvnw clean test-compile -Dair.check.skip-all=true -Perrorprone-compiler \
  -pl core/trino-main,plugin/trino-iceberg,testing/trino-tests

# Независимые проверки после восстановления обычных core-артефактов:
./mvnw clean test-compile -Dair.check.skip-all=true -Perrorprone-compiler \
  -pl plugin/trino-iceberg,testing/trino-tests
./mvnw clean test-compile -Dair.check.skip-all=true -Perrorprone-compiler \
  -pl testing/trino-tests

python3 /tmp/smj-guide-check-commits.py origin/master..HEAD
```

| Проверка | Результат |
|---|---|
| `verify -DskipTests -Pci`, три модуля | **SUCCESS**, 6 мин 35 с |
| Airstyle внутри verify | 4284 core + 494 Iceberg + 137 testing Java files — отформатированы |
| Checkstyle внутри verify | 0 violations во всех трёх модулях |
| License / enforcer / sortpom / dependency / Modernizer | Выполненные проверки в verify прошли |
| Javadoc | Успешно упакован; предупреждения об incubator module не остановили сборку |
| SpotBugs / PMD | **SKIPPED** настройками проекта; не считаются выполненными проверками |
| JUnit tests в verify | **SKIPPED** по явному `-DskipTests`; config-test failures v02 остаются актуальными |
| Error Prone, исходная трёхмодульная команда | **FAILURE** в production core, G-07; следующие модули SKIPPED |
| Error Prone, независимый повтор Iceberg | Main compilation прошла; **FAILURE** в test compilation, ещё одна diagnostic G-07 |
| Error Prone, независимый запуск trino-tests | **SUCCESS**, main и test compilation; присутствуют warnings |
| Закреплённый check-commit-messages | **FAILURE**, 17 отклонённых сообщений |

Логи: [guide-ci-verify.log](evidence/guide-ci-verify.log), [guide-errorprone.log](evidence/guide-errorprone.log), [guide-commit-check.log](evidence/guide-commit-check.log). JSON с properties/header checks — [guide-static-review.json](evidence/guide-static-review.json); hash локальных guides и скрипта — [guide-policy-provenance.json](evidence/guide-policy-provenance.json).

Успешный verify показывает прохождение включённых в профиль проверок, а не полный GitHub CI и не отсутствие функциональных дефектов. Ничего не подавлялось новыми `@SuppressWarnings`, и исходники для получения SUCCESS не редактировались. Исходная неуспешная clean-команда временно оставила core targets неполными; повтор других модулей в таком состоянии дал missing-class ошибки, которые не классифицируются как новые SMJ defects. После обычного `package -DskipTests -Dair.check.skip-all=true -Dmaven.compiler.useIncrementalCompilation=false -pl core/trino-main` артефакты восстановлены, а изолированные проверки повторены с указанными выше результатами. [Лог восстановления](evidence/guide-restore-core.log), [неуспешный промежуточный запуск на неполных артефактах](evidence/guide-errorprone-remaining-incomplete.log).

Сводка команд и exit codes: [guide-check-results.json](evidence/guide-check-results.json). Core test sources с Error Prone не проверены: production compilation завершается раньше. Успешная обычная test compilation в verify этого ограничения не снимает.
