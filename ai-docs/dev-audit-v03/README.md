# Аудит SMJ v03

Аудит относится к `smj-plan@0393e45fcf9` после исправлений второго аудита. Основной результат — [REPORT.md](REPORT.md).

**Последующие технические исправления v03 завершены и проверены.** Текущее состояние — [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md); все пункты [checklist](REMEDIATION_CHECKLIST.md) закрыты в scope личного MVP. Локальный review-кандидат — `smj-upstream-v03@45fe8c5d602`. Отчёт ниже описывает состояние до этих исправлений.

Концепция узкого external merge join пригодна для дальнейшей разработки. До upstream MR требуется исправить два новых P1:

1. Поздний projection/partial aggregation rewrite снимает MERGE после построения физической topology; обычный grouped join падает.
2. Match bitmap для outer join пишет производный match-state открытым текстом при включённом spill encryption.

Материалы:

- [CONCEPT_REVIEW.md](CONCEPT_REVIEW.md) — архитектура, ordering, memory/storage и границы MVP;
- [SOURCE_REVIEW.md](SOURCE_REVIEW.md) — перечень и глубина прочтения реализации;
- [VALIDATION.md](VALIDATION.md) — новые проверки, controls и ограничения выводов;
- [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md) — состояние требований CONTRIBUTING/DEVELOPMENT и истории;
- [REMEDIATION_CHECKLIST.md](REMEDIATION_CHECKLIST.md) — конкретные исправления и критерии приёмки;
- [repro/README.md](repro/README.md) и [test patch](repro/regression-probes.patch) — воспроизведение;
- [evidence/](evidence/) — команды, логи и machine-readable результаты; [SHA256SUMS](evidence/SHA256SUMS).

В рамках самого аудита production Java и история веток не менялись. Последующая реализация исправлений и её проверки описаны отдельно. `hs_err_pid*.log` не читались. CLA и upstream scope по сообщению автора пока не оформлены; это известные условия будущего submission личного MVP.
