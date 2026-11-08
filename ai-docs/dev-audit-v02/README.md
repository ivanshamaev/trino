# Аудит SMJ v02

Исходная ревизия: `773afdee3bd26dc1a59eb0524a3445657fc1b0d7`, дата 2026-11-08.

Технические доработки аудита завершены: [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). Финальный кандидат собран поверх обновлённого upstream; regressions, CI verify, main/test Error Prone и Sphinx прошли. По уточнению автора проект является личным MVP: CLA не подписан, scope с upstream не согласован. Это условия будущего submission, а не незавершённые исправления MVP.

**Вывод исходного аудита для `773afdee3bd`: концепция жизнеспособна, реализация ещё не готова к upstream MR.** Зафиксированы 10 замечаний: 6 P1 и 4 P2. Новая тихая ошибка результата не установлена; есть воспроизводимые ошибки планирования/исполнения, масштабирования и интеграционных контрактов.

- [REPORT.md](REPORT.md) — вердикт, конкретные замечания, исходники, воспроизведение и способы устранения.
- [CONCEPT_REVIEW.md](CONCEPT_REVIEW.md) — проверка концепции, invariants, физической схемы, sortedness, стоимости, памяти и scope.
- [VALIDATION.md](VALIDATION.md) — что реально запускалось, результаты и границы доказательств.
- [SOURCE_REVIEW.md](SOURCE_REVIEW.md) — область разбора и список 61 изменённого production Java-файла.
- [MR_READINESS_CHECKLIST.md](MR_READINESS_CHECKLIST.md) — критерии доработки и приёмки перед MR.
- [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md) — дополнительная проверка CONTRIBUTING/DEVELOPMENT, истории коммитов, оформления properties и pre-PR checks.
- [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md) — последующие исправления и фактические результаты проверок; исходный аудит выше относится к ревизии `773afdee3bd`.
- [UPSTREAM_PREPARATION.md](UPSTREAM_PREPARATION.md) — границы предлагаемой истории и черновик PR description/release notes.
- [repro/](repro/README.md) — patch с проверочными Java tests для исходной ревизии аудита.
- [evidence/](evidence/scope.json) — логи, assertion traces и измерения.

В ходе исходного аудита повторный штатный core regression: 134 tests, без failures/errors, 6 ожидаемых пропусков. Новые probes воспроизвели контрпримеры; также два существующих IcebergConfig tests упали без изменения их исходников. Production-код в ходе самого аудита не менялся, временные tests сняты с рабочего дерева. Файлы `hs_err_pid*.log` не читались.
