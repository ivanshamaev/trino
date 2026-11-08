# Проверочные tests аудита v02

`regression-probes.patch` содержит воспроизводящие tests, без production fixes. Они намеренно проверяют контракт, нарушенный в HEAD `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`. Ожидаемый текущий результат — указанные в отчёте failures, а не зелёный прогон.

Применять к отдельному чистому checkout этой ревизии из корня репозитория:

```bash
git apply --check ai-docs/dev-audit-v02/repro/regression-probes.patch
git apply ai-docs/dev-audit-v02/repro/regression-probes.patch
```

Если отчёт ещё не добавлен в checkout, используйте абсолютный путь к patch в исходном workspace. Требуются JDK 25 и обычные Maven-зависимости Trino; используемые команды и флаги указаны в [VALIDATION.md](../VALIDATION.md).

Patch добавляет отдельные audit-классы и методы в существующие test-классы. Его tests не следует коммитить в production sources как исправление: после устранения причины они могут стать обычными regression tests без reflection, где API это позволяет.

Отмена только этого patch в checkout, где его строки не менялись:

```bash
git apply --reverse --check ai-docs/dev-audit-v02/repro/regression-probes.patch
git apply --reverse ai-docs/dev-audit-v02/repro/regression-probes.patch
```

В рабочем дереве, из которого подготовлен аудит, patch уже снят. Исходный Java-код не изменён.
