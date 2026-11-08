# Воспроизведение audit probes

Patch [regression-probes.patch](regression-probes.patch) содержит только test sources: три расширенных существующих test-класса и три новых. Production fixes в нём нет. База — `0393e45fcf954473db34a33ecebe587c8cfff94d`; `git apply --check` на исходном дереве проходит.

Рекомендуется отдельный checkout/worktree, чтобы заведомо падающие regression probes не смешивались с рабочими изменениями. В выполненном аудите использовался `/tmp/trino-smj-audit-v03-check`, dependency build outputs переиспользовались как описано в [VALIDATION.md](../VALIDATION.md). Для нового чистого checkout сначала нужны обычные reactor/dependency builds по DEVELOPMENT.md: patch сам по себе не устанавливает зависимости.

```bash
# Выполнять из выбранного checkout на закреплённом HEAD:
git apply --check /home/ivan/opencode/trino/ai-docs/dev-audit-v03/repro/regression-probes.patch
git apply /home/ivan/opencode/trino/ai-docs/dev-audit-v03/repro/regression-probes.patch

export JAVA_HOME=/home/ivan/tools/smj-oracle-jdk25
export PATH="$JAVA_HOME/bin:$PATH"
export MAVEN_OPTS=-Xmx3g
export JAVA_TOOL_OPTIONS='-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops'

./mvnw test-compile -Dair.check.skip-all=true \
  -pl core/trino-main,plugin/trino-iceberg

# Положительные probes: scalar families, timestamp baseline control и fuzz:
./mvnw initialize surefire:test -Dair.check.skip-all=true \
  -pl core/trino-main \
  '-Dtest=TestMergeJoinTypeAudit,TestMergeJoinOperator#testAuditV03DifferentialFuzz'

# Негативные probes — на текущем HEAD ожидается ненулевой exit code:
./mvnw initialize surefire:test -Dair.check.skip-all=true \
  -pl core/trino-main \
  '-Dtest=TestAppendingSpiller#testAuditV03EncryptedMatchBitmap,TestMergeJoinPostSelectionAudit'

# Распределённое подтверждение V03-01 — также ожидается ненулевой exit code:
./mvnw initialize surefire:test -Dair.check.skip-all=true \
  -pl plugin/trino-iceberg \
  '-Dtest=TestIcebergSortedWriting#testAuditV03PostSelectionAggregation'

# Положительные дополнительные distributed probes:
./mvnw initialize surefire:test -Dair.check.skip-all=true \
  -pl plugin/trino-iceberg \
  '-Dtest=TestIcebergSortedWriting#testAuditV03MultipleFilesOffWindows,TestMergeJoinExchangeAudit'
```

Каждую команду с ожидаемым падением следует запускать отдельно, сохранять её exit code и читать причину. Отсутствие compile/dependency ошибок ещё не означает, что negative regression воспроизведён. В probes не используются `assertThrows` для двух найденных SMJ defects: tests проверяют правильное поведение, поэтому должны стать зелёными после production fixes. Исключение — самостоятельный timestamp baseline control, явно проверяющий существующий отказ без SMJ и без JOIN.

Штатный core suite, включая пять 10M-row tests, повторяется selector-ом из [baseline-selection.json](../evidence/baseline-selection.json) и `-Dsmj.large=true`. Полные реально выполненные команды уже записаны в [final-test-results.json](../evidence/final-test-results.json); этот набор шире показанных коротких probes. Не включайте во весь baseline новые отрицательные методы без намерения получить красный результат.

Изменённые Java probes отформатированы Airstyle. Patch не является предлагаемым upstream commit: при переносе в основную suite нужны обычные имена regression tests, согласованный lifecycle и исправленный production path. Технические детали stdout предназначены для аудита и не добавляются в пользовательский SQL interface.
