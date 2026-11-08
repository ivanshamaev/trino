# Evidence исправлений v03

Окончательный кандидат: `smj-upstream-v03@45fe8c5d602c2973950d214573b42e8481971fb0`. Итог — [IMPLEMENTATION_RESULTS.md](../IMPLEMENTATION_RESULTS.md).

| Файлы | Назначение |
|---|---|
| initialization-results.json, initialization-pipeline.log | Окончательная последовательность build, runtime suites, CI verify, Error Prone; все exit codes 0 |
| initialization-core-large.log | 309 tests, включая новые zero-state/init/yield/cancel cases и пять 10M-row scenarios |
| initialization-iceberg-workloads.log | 26 tests, в том числе opt-in workloads; 2 expected skips |
| initialization-ci-verify.log | Успешный verify трёх модулей; JUnit skipped по команде, runtime suites выполнялись отдельно |
| initialization-errorprone.log | Успешная clean main/test compilation трёх модулей с Error Prone |
| docs-final-result.json, docs-final-build.log | Полный Sphinx build с fresh environment, `-E -W --keep-going` |
| review-tpcds-plans.log | 206 fixtures на предшествующем snapshot; planner/test/fixture sources после него не менялись |
| junit-summary.json | Фактические per-class totals отдельных прогонов; не сумма уникальных tests между повторами |
| replay-sweep-final.json | Девять serialized/reservation I/O samples, не physical device I/O |
| distributed-workloads-final.json | 36 успешных измерений и 2 ожидаемых memory-limit failures; не latency benchmark |
| review-series.json, review-commit-policy.json | Два дополнительных review-коммита и проверка всех шести сообщений серии |
| final-source-integrity.json, modified-source-comparison.json | Состояние веток, 23 одинаковых изменённых файла, отсутствие private audit files в review branch |
| main-source-SHA256SUMS, review-source-SHA256SUMS | По 114 software/test/docs/fixture paths; пути относительно соответствующего checkout |
| final-changes.patch | Полный software/docs diff рабочей smj-plan относительно исходного HEAD |
| planner-changes.patch, runtime-changes.patch | Группы изменений для review; runtime-changes.initial.patch сохраняет вариант до уточнения zero-state |
| audit-baseline-* | Исходный manifest и прежние navigation/checklist до обновления статуса реализации |

First/second/third и review logs сохраняют промежуточные попытки. `review-results.json` содержит намеренно остановленный первый CI verify с exit 143: после дополнительной проверки был уточнён zero-state. Он не считается успешной проверкой. Все итоговые profile gates повторены и прошли в initialization pipeline. Прежний runtime commit `fdf8ab5a24a` уточнён локально, новый SHA указан выше.

Контрольные суммы материалов проверяются из каталога `dev-audit-v03`:

```bash
sha256sum -c evidence/SHA256SUMS
sha256sum -c implementation-evidence/SHA256SUMS
```

Global manifest включает актуальные материалы аудита и реализации. Первоначальный audit manifest сохранён в `audit-baseline-SHA256SUMS`; для его исторической проверки используются архивные README/checklist. `evidence/source-SHA256SUMS` относится к Git snapshot `0393e45fcf9`, а новые два source manifests — к рабочему дереву и review-кандидату после исправлений.

Source manifests проверяются из корня соответствующего checkout. Наличие двух manifests не означает, что все неизменённые файлы двух баз одинаковы: review series основана на upstream SHA на 53 коммита новее. Именно 23 изменённых файла побайтно совпадают.

`hs_err_pid*.log` не читались и в manifests не включены.
