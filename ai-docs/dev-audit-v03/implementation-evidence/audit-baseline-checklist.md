# Доработки по третьему аудиту

Ревизия `0393e45fcf9`. Это план приёмки, а не отметки выполненных исправлений. Область личного MVP и будущего upstream submission разделена.

## Обязательные технические исправления

- [ ] V03-01: убрать некорректный сброс MERGE в post-selection projection/partial-aggregation path; fallback должен заново согласовывать local topology, driver count и spillability.
- [ ] V03-01: проверить ordered exchange после pushdown с projection; сохранить сортировку обоих входов после всех поздних rewrites.
- [ ] V03-01: SQL regressions с двумя grouping keys и sum(payload + 1), spill on/off; OFF и FORCE с выключенным aggregation pushdown — независимые controls.
- [ ] V03-02: encryption-aware match bitmap либо явное безопасное ограничение неподдержанного сочетания; encrypted data stream не должен сопровождаться plaintext match-state.
- [ ] V03-02: end-to-end RIGHT/FULL + residual + forced disk bitmap + encryption; cancel/cleanup/quota/serde/replay tests.

## Усиление защиты

- [ ] Final physical-plan validator: MERGE ordering, partitioning, single stream, eligibility и согласованные DF; hash fallback имеет установленный spillable и корректные driver assumptions.
- [ ] Oracle tests не предполагают порядок строк внутри одного equi-key; INNER/LEFT отдельно проверяют только обещанный порядок ключей.
- [ ] Сохранить regression suite v02, compressed/encrypted fuzz, scalar families и opt-in 10M-row tests.
- [ ] Перед фиксацией результата повторить verify и Error Prone для изменённых модулей и собрать public docs. Тесты аудита сами по себе не исправляют production sources.

## Подготовка будущего upstream submission

- [ ] Перенести окончательные исправления в отдельную reviewable историю и проверить каждый commit; обычная smj-plan с private audit artifacts не является готовым upstream PR.
- [ ] Подтвердить scope именно experimental PARTITIONED/retry NONE/single-driver и реальные validation gates; не представлять весь первоначальный stage 0–7 как выполненный.
- [ ] При решении отправлять работу upstream подписать CLA и согласовать scope с maintainers. Автор подтвердил, что для текущего личного MVP это ещё не делалось; этот пункт не блокирует сам MVP.
