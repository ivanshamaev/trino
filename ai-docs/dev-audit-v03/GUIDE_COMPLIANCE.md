# Гайды и подготовка будущего submission

Повторная оценка относится к `smj-plan@0393e45fcf9`. Источники требований — локальные [CONTRIBUTING.md](../../.github/CONTRIBUTING.md), [DEVELOPMENT.md](../../.github/DEVELOPMENT.md), [CI workflow](../../.github/workflows/ci.yml), [PR template](../../.github/pull_request_template.md) и [PR description workflow](../../.github/workflows/pr-description-check.yml). Это дополнение к техническому [отчёту](REPORT.md).

## Что изменилось после второго аудита

- Исправлены descriptions и штатные config tests; текущие config regressions проходят.
- Добавлены публичные описания SMJ scope/properties/metrics/spill и Iceberg report_sortedness. Private ai-docs больше не являются единственным источником контракта.
- Удалены повреждённый второй header и reflection-only serialization test; JoinNode JSON round-trip входит в свежий core suite.
- Диагностики Error Prone устранены в v02. Последний verify/Error Prone/Sphinx success закреплён историческими логами кандидата; v03 не повторяет эти команды и не объявляет их свежим CI всего проекта.
- Bounded spill изолирован через признак SMJ SortNode; обычный ORDER BY и sorter regressions в v03 проходят.

При этом обещание сохранения физического контракта после поздних rewrites оказалось неполным (V03-01), а spill encryption не охватывает match-state (V03-02). Formatter и успешные обязательные compile checks такие функциональные контракты не доказывают. Их исправление требует SQL/regression tests, а не только code style cleanup.

## История коммитов

Свежий запуск закреплённого checker-а из `airlift/github-actions@1652e24519f5c7cec59d841070bf9beffadabdf9` выполнен для каждого non-merge commit через `--message-file`, без переписывания истории. Базы диапазонов указаны явно, чтобы не примешивать чужие upstream commits. Результаты и SHA256 checker-а — [commit-policy.json](evidence/commit-policy.json).

| История | Проверено сообщений | Отклонено |
|---|---:|---:|
| smj-plan, fork base e112ffdf730 | 31 | 17 |
| smj-upstream-v02, base 8ae1f91f853 | 4 | 0 |

Последний `Fix sorted merge join audit findings` проходит, но не исправляет предыдущие сообщения. Это известная причина использовать подготовленную отдельную review history для upstream. Все четыре её intermediate builds уже проверялись в v02; в v03 они заново не собирались. V03-01/V03-02 присутствуют и в этом software snapshot: хорошая история не устраняет runtime defects.

Наличие локального ref `audit-upstream/master@8ae1f91f853` не доказывает соответствие актуальному состоянию GitHub на дату будущего MR. Здесь не выполнялись rebase, force push или публикация PR.

## Оставшиеся условия

Нужно исправить два P1, повторить проверки затронутых модулей, обновить scope/release-note/PR description по фактическим ограничениям. Private docs references в Java comments перед submission полезно заменить ссылкой на включённый в PR design/public documentation; например, Javadoc DetermineJoinAlgorithm всё ещё ссылается на исследовательские ai-docs, отсутствующие в review history. Это замечание к самостоятельности документации, не отдельный runtime blocker.

Автор прямо сообщил: CLA не подписан, upstream scope не согласован, проект пока личный MVP. CONTRIBUTING.md требует CLA для принятия вклада. Это условие будущего submission; повторное подтверждение у автора и остановка личной разработки не требуются. Никаких сообщений maintainers или подписания от имени автора этот аудит не выполняет.

Release-note proposal и PR draft из v02 — подготовленные материалы, а не уже принятый upstream release note или открытый PR. Перед публикацией нужно актуализировать их после V03 fixes и проверить workflow требований к окончательному описанию.
