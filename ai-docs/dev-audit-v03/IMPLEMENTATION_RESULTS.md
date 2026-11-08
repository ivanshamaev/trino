# Результаты исправлений аудита v03

**Все технические пункты v03 закрыты и проверены.** Кандидат — локальная `smj-upstream-v03`, HEAD `45fe8c5d602c2973950d214573b42e8481971fb0`. Два P1 исправлены; final-plan validation, regressions, CI verify, Error Prone и public docs build прошли.

Исходный аудит относится к `smj-plan@0393e45fcf954473db34a33ecebe587c8cfff94d`. REPORT.md, CONCEPT_REVIEW.md и исходные evidence сохранены как история обнаружения дефектов. В рабочей `smj-plan` находятся те же изменения 23 software/docs files; её HEAD и опубликованная история не переписаны. Новые review-коммиты созданы отдельно. [Checklist](REMEDIATION_CHECKLIST.md) отражает приёмку, а не заменяет исходный отчёт.

## V03-01: физический контракт JOIN после rewrites

PushProjectionThroughJoin сохраняет algorithm: equi-keys проходят через projection как identities. PushPartialAggregationThroughJoin не опускает hash partial aggregation под выбранный MERGE, поскольку такой aggregation не сохраняет ordering, а rule выполняется после local exchanges. Оба варианта PushPartialAggregationThroughExchange отвергают ordered exchange. Hash join сохраняет прежний pushdown; небезопасного переключения MERGE в hash после physical planning нет.

ValidateJoinAlgorithms включён в final/adaptive sanity checks. Проверяет eligibility, ASC NULLS LAST на канонических ключах, single stream каждого входа, совместимое node partitioning и отсутствие replicated-null/any semantics. Для hash join при spill проверяет spillable и local streams. Existing DynamicFiltersChecker проверяет producers/consumers; distributed RIGHT regression также проходит. Ordered exchange проверяется по сортировке каждого источника: одной декларации ordering недостаточно.

PropertyDerivations публикует ordering ordered REMOTE REPARTITION и сохраняет node partitioning через single-source LOCAL exchange. Ранее часть гарантий передавалась planner-ом вручную в PlanWithProperties и терялась при повторной derivation. Исправление позволяет проверять конечный план, включая colocated joins.

SQL regressions используют два grouping keys и `sum(payload + 1)`, spill on/off. Тот же SQL сначала выполняется с OFF и FORCE/pushdown=false; результаты сравниваются независимо. Затем проверяются marker и результат FORCE/pushdown=true. Distributed варианты и отдельный ordered-exchange aggregation SQL также проходят, последний повторяется пять раз.

## V03-02: шифрование match-state

AppendingSpiller создаёт готовый bitmap с encryption policy native delegate. Operator больше не получает raw path и не выбирает самостоятельно, шифровать ли match-state. При encryption=true используется AES-GCM с отдельным временным AES-256 key на bitmap. 96-bit IV содержит монотонный счётчик записей; он не повторяется при возвращении к старому блоку, переполнение не допускается. Logical block offset передаётся как AAD.

До matching шифруются **все zero blocks**. Инициализация идёт порциями по 16KiB с cooperative yield; mark/read запрещены до её завершения. Untouched default state не остаётся открытым. Close/cancel удаляет и полностью, и частично инициализированный файл. Один cached block, record/cipher state и physical encrypted-file overhead учитываются в reservations. Длина, offsets и access patterns не скрываются. Managed reservations по-прежнему не являются абсолютным JVM heap cap.

Требование уникального IV для каждого GCM encryption сверено с [JDK 25 Cipher API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/javax/crypto/Cipher.html). Main encrypted data pages и bitmap используют независимые временные ключи. Ошибка encryption/decryption не переводит bitmap на plaintext.

Regressions проверяют zero-state до matching, отсутствие открытого шаблона `0x55` во всём файле, перезапись с новым IV, corruption, quota failure, long positions и cleanup. Native operator integration выполняет RIGHT/FULL с residual и 160 000 build rows при query limit 32KiB: boolean array заведомо не помещается, bitmap требует более одной initialization batch. Для NONE/LZ4/ZSTD проверяются normal completion и close с живым bitmap. Отдельный тест отменяет незавершённую initialization. Это end-to-end native operator path; не заявляется отдельный distributed SQL attack на encrypted spill.

## Окончательные проверки

| Проверка | Результат | Evidence |
|---|---|---|
| Core на уточнённом кандидате, `smj.large=true` | **309 tests, 0 failures/errors, 1 expected skip** | [initialization-core-large.log](implementation-evidence/initialization-core-large.log) |
| Iceberg config/sortedness, новые SQL regressions, opt-in distributed workloads | **26 tests, 0 failures/errors, 2 expected skips** | [initialization-iceberg-workloads.log](implementation-evidence/initialization-iceberg-workloads.log) |
| TPC-DS partitioned plan fixtures | **206 tests, 0 failures/errors/skips** | [review-tpcds-plans.log](implementation-evidence/review-tpcds-plans.log) |
| `verify -DskipTests -Pci`, три модуля | **SUCCESS**, 7 мин 25 с | [initialization-ci-verify.log](implementation-evidence/initialization-ci-verify.log) |
| `clean test-compile -Perrorprone-compiler`, три модуля | **SUCCESS**, main и test sources, 13 мин 57 с | [initialization-errorprone.log](implementation-evidence/initialization-errorprone.log) |
| Sphinx с `-E -W --keep-going` | **SUCCESS**, fresh environment | [docs-final-build.log](implementation-evidence/docs-final-build.log) |
| Commit-message policy | **6/6 проходят** | [review-commit-policy.json](implementation-evidence/review-commit-policy.json) |

Все argv, timestamps и exit codes финальной pipeline — [initialization-results.json](implementation-evidence/initialization-results.json); Sphinx — [docs-final-result.json](implementation-evidence/docs-final-result.json). Фактически запущенные JUnit classes/totals — [junit-summary.json](implementation-evidence/junit-summary.json).

TPC-DS выполнялся перед уточнением zero-state. Все planner sources, tests и fixtures между тем snapshot и окончательным кандидатом неизменны; это проверено в [final-source-integrity.json](implementation-evidence/final-source-integrity.json). После crypto refinement повторены runtime suites и profile checks; неизменённый planner corpus повторно не запускался. Эти fixtures не являются SF-1000 runtime benchmark или fingerprint всего physical plan.

Пять opt-in сценариев на 10 млн build rows действительно выполнены: INNER/LEFT/RIGHT/FULL и single build group с одной probe-строкой. Единственный core skip — extreme FULL output более двух миллиардов строк. Два Iceberg skips — опциональные calibration/planning-cost methods; `smj.validation=true` workload class выполнен явно. Полный monorepo suite не запускался.

Differential fuzz — 40 seeds × четыре join type × in-memory/native spill = **320 сравнений мультимножеств**. Есть scalar/composite keys, NULL, duplicates, residual и случайные page boundaries; codecs/encryption распределены по seeds, а не перемножены на все seeds. Для INNER/LEFT отдельно проверяется порядок ключей. Payload order внутри группы и unmatched RIGHT/FULL order не используются как oracle. Scalar SQL matrix — **40 сравнений** десяти семейств ключей и четырёх join types с OFF oracle и проверкой MERGE в EXPLAIN.

Свежие девять replay samples сохранили read/write 2.4053–2.8875: [replay-sweep-final.json](implementation-evidence/replay-sweep-final.json). Это serialized/reservation counters, а не physical disk I/O. Native encrypted integration и long-position bitmap tests проверяют конкретные границы; они не сертифицируют 15B×17B, любой размер строки или произвольную конкуренцию.

Distributed workload measurements — [distributed-workloads-final.json](implementation-evidence/distributed-workloads-final.json): 27 threshold executions, шесть concurrent chain/skew executions и wide FULL scenarios. На high-entropy wide input FORCE/COST_BASED проходят при 128MB/node, OFF получает ожидаемый memory-limit error. Документированная compressed-statistics limitation сохраняется: для сжимаемых строк FORCE проходит, COST_BASED может оставить hash и превысить память. Это ожидаемая проверка ограничения local heuristic, а не новый дефект merge loop. Времена — одиночные наблюдения без вывода об общем latency speedup; QueryStats reservations не равны JVM heap или per-worker peak.

CI verify намеренно запускается с skipped JUnit; runtime tests выполнены отдельно. SpotBugs/PMD пропущены настройками проекта и не выдаются за выполненные проверки. В логах есть deprecation warnings и сообщения cleanup/JMX тестовой среды; результат 0 failures/errors относится к JUnit, а не к отсутствию любого ERROR word в stdout.

Окружение: Oracle JDK `25.0.5.0.3+1`, wrapper, `MAVEN_OPTS=-Xmx3g`, `JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'`. У трёх изменённых модулей собственные build outputs; unchanged dependencies переиспользованы из существующих targets. Это clean compilation изменённых модулей для Error Prone, не clean build всего monorepo.

## Review-история и целостность

`smj-upstream-v03` сохраняет четыре коммита подготовленной v02 series поверх закреплённой `8ae1f91f853` и добавляет:

1. `60149fa87e8` — Preserve merge join contracts in late rewrites. Main/test build трёх модулей, 27 core tests и 4 distributed Iceberg tests прошли.
2. `45fe8c5d602` — Encrypt merge join match spill state. Уточнённый main/test build трёх модулей, 309 core tests, 26 Iceberg/workload tests, CI verify и Error Prone прошли.

[review-series.json](implementation-evidence/review-series.json) закрепляет boundaries и SHA. Private ai-docs и audit logs не включены в review branch. Main working tree содержит те же 23 изменённых software/docs files, побайтно равные кандидату; commit HEAD `smj-plan` остаётся исходным. Готовая review history локальна; push/PR не выполнялись. [UPSTREAM_PREPARATION.md](UPSTREAM_PREPARATION.md) содержит обновлённый draft и реальный scope.

## Промежуточные попытки

Первый core smoke выявил отсутствующую ordering property у valid ordered REMOTE REPARTITION. Validator сохранён строгим: исправлена derivation и добавлена source validation. Следующая test compilation остановилась на недоступном AssertJ helper; заменён Guava byte-pattern search. Эти logs сохранены отдельно.

Snapshot `fdf8ab5a24a` прошёл 307 core tests, 26 Iceberg/workload tests и 206 TPC-DS fixtures. При дополнительной проверке sparse zero-state признан недостаточно защищённым. Runtime commit локально уточнён полной encryption initialization, yield/cancel и соответствующими regressions. Первый CI verify намеренно остановлен SIGTERM и не считается успешным. Все окончательные runtime/profile результаты выше относятся к уточнённому snapshot; прежние попытки не подменяют их.

## Границы scope

Проект остаётся личным experimental MVP: PARTITIONED, retry NONE, single merge driver. Multi-driver/FTE/range splitting/joint CBO и масштабная certification исходного stage 0–7 не объявляются реализованными. Автор подтвердил, что CLA не подписан и upstream scope не согласован; эти внешние действия отложены до будущего submission и не выполняются от его имени.

`hs_err_pid*.log` не читались и не хешировались. Baseline manifest и изменяемые audit navigation/checklist files сохранены в implementation-evidence/audit-baseline-*. Контрольные суммы итоговых материалов и source snapshots фиксируются отдельно.
