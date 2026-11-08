# Навигация по evidence

Production HEAD: `0393e45fcf954473db34a33ecebe587c8cfff94d`. Окончательные результаты описаны в [VALIDATION.md](../VALIDATION.md); ранние неудачные попытки сохранены, чтобы не скрывать изменение audit oracle/fixtures.

| Материалы | Назначение |
|---|---|
| scope.json, source-SHA256SUMS | Software diff и контрольные суммы production/public-docs snapshot |
| build-environment.json, build.log | Окружение, reused dependencies, свежая сборка трёх модулей |
| core-regressions-large.log, iceberg-regressions.log | Окончательный штатный suite, включая пять 10M-row scenarios |
| positive-controls.log | Окончательные 320 fuzz comparisons, codecs/encryption; 40 scalar SQL comparisons; timestamp baseline |
| negative-controls.log | Окончательные локальные V03-01/V03-02 с независимыми успешными SQL controls |
| distributed-controls.log | Окончательный distributed V03-01, успешные controls и последующий runtime failure |
| sortedness-off.log, ordered-exchange.log | Положительные проверки двух интеграционных гипотез |
| controls-results.json, final-test-results.json, additional-test-results.json | argv, длительности и exit codes соответствующих этапов |
| junit-results.json | Разбор фактически выполненных классов из логов, без заявления о запуске всего модуля |
| replay-sweep.json | Девять samples serialized data/index I/O counters |
| commit-policy.json | Свежая проверка 31 исходного и 4 review commit messages |
| final-integrity.json | Отсутствие production edits, применимость patch, целостность ссылок и JSON |
| SHA256SUMS | Хеши файлов этого аудита; пути относительно dev-audit-v03 |

Первые `differential-fuzz-order-assumption.log` / `core-probes.log` содержат false failures из-за требования точного порядка строк. `scalar-types.log` — первый fixture с timestamp materialization failure, независимо воспроизведённым без JOIN и SMJ. Файлы `.initial-compile-failure` — промежуточная ошибка boolean/int в audit helper, исправленная только в test patch. Они не являются дополнительными найденными SMJ defects и не заменяют финальные logs.

`core-probes-final.log` и `distributed-aggregation.log` — уже корректные первые контрпримеры, после которых добавлены отдельные OFF/pushdown=false controls. Последние negative runs остаются красными намеренно: production fixes в этом аудите не делались. Exit code=1 сохранён в JSON, а не скрыт как SUCCESS.

`hs_err_pid*.log` не читались, в manifest не включены.
