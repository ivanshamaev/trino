# Доработки по третьему аудиту

Исходный аудит: `0393e45fcf9`. Результаты последующих исправлений — [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). Область личного MVP и будущего upstream submission разделена. Отметки ставятся по завершённым проверкам, а не по наличию кода.

## Обязательные технические исправления

- [x] V03-01: убрать некорректный сброс MERGE в post-selection projection/partial-aggregation path; fallback должен заново согласовывать local topology, driver count и spillability.
- [x] V03-01: проверить ordered exchange после pushdown с projection; сохранить сортировку обоих входов после всех поздних rewrites.
- [x] V03-01: SQL regressions с двумя grouping keys и sum(payload + 1), spill on/off; OFF и FORCE с выключенным aggregation pushdown — независимые controls.
- [x] V03-02: encryption-aware match bitmap либо явное безопасное ограничение неподдержанного сочетания; encrypted data stream не должен сопровождаться plaintext match-state.
- [x] V03-02: end-to-end RIGHT/FULL + residual + forced disk bitmap + encryption; cancel/cleanup/quota/serde/replay tests.

## Усиление защиты

- [x] Final physical-plan validator: MERGE ordering, partitioning, single stream, eligibility и согласованные DF; hash fallback имеет установленный spillable и корректные driver assumptions.
- [x] Oracle tests не предполагают порядок строк внутри одного equi-key; INNER/LEFT отдельно проверяют только обещанный порядок ключей.
- [x] Сохранить regression suite v02, compressed/encrypted fuzz, scalar families и opt-in 10M-row tests.
- [x] Перед фиксацией результата повторить verify и Error Prone для изменённых модулей и собрать public docs. Тесты аудита сами по себе не исправляют production sources.

## Подготовка будущего upstream submission

- [x] Перенести окончательные исправления в отдельную reviewable историю и проверить каждый commit; обычная smj-plan с private audit artifacts не является готовым upstream PR.
- [x] Подтвердить scope именно experimental PARTITIONED/retry NONE/single-driver и реальные validation gates; не представлять весь первоначальный stage 0–7 как выполненный.
- [x] Зафиксировать внешние условия будущего submission: CLA не подписан, scope с maintainers не согласован. По прямому сообщению автора это личный MVP; подписание и обсуждение отложены до решения отправлять работу upstream и не входят в завершение текущих технических доработок. См. [UPSTREAM_PREPARATION.md](UPSTREAM_PREPARATION.md).

## Выполненная проверка

Окончательный кандидат: `smj-upstream-v03@45fe8c5d602`. В `smj-plan` находятся те же 23 software/docs changes; HEAD и опубликованная история не переписаны.

- Core: 309 tests, 0 failures/errors, 1 expected skip. Пять opt-in 10M-row scenarios выполнены; 320 fuzz comparisons, 40 scalar comparisons и native encryption/yield/cancel regressions проходят.
- Iceberg и opt-in workloads: 26 tests, 0 failures/errors, 2 expected skips.
- TPC-DS: 206 planner fixtures прошли; planner sources после этого не менялись.
- CI verify: SUCCESS во всех трёх модулях.
- Clean main/test Error Prone: SUCCESS во всех трёх модулях.
- Полная пересборка Sphinx с `-E -W --keep-going`: SUCCESS.
- Оба новых review-коммита собираются, все шесть commit messages проходят policy checker. Внешние публикации не выполнялись.

Команды, логи, scope и ограничения — [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). Исходный checklist до исправлений сохранён в implementation-evidence/audit-baseline-checklist.md.
