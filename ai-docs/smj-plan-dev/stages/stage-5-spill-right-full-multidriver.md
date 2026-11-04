# Stage 5 — Спилл групп + RIGHT/FULL + multi-driver

Фаза 2 плана [risks/04 §4.3](../../smj-research/risks/04-implementation-plan-with-risk-gates.md).
Зависит от MVP (stage 4). Целевой кейс: 15B×17B с гигантскими группами при bounded memory.

## Цель

Полная bounded-memory: спилл build-групп с re-iterate, RIGHT/FULL (zero-retention),
параллельные merge-драйверы, производительные доработки.

## Состав работ

| Компонент | Решение | Риск/якорь |
|---|---|---|
| SpillingMatchBuffer | порог → спилл группы (ран); повторные проходы — block-nested-loop **чанками** (не построчно); ленивые spill-читатели; контракт «ран ≡ immutable stream» (раны одноразовые — FileSingleStreamSpiller:145/189 — каскад без re-read) | I5, P4 (SPARK-30536); [07 §7.5](../../smj-research/07-brainstorm-java-optimization.md) |
| RIGHT/FULL outer-driver | **однопроходный стриминг**: null-padding O(1), zero-retention (пик == INNER); drain build-хвоста после probe; residual `anyMatchSurvived`-флаг; NULL-хвосты — C9-fast-path; re-iterate **обеих** сторон группы при спилле | risks/07 целиком; план §4.3 |
| Multi-driver merge | partitionCount = k×taskCount; реестр мостов по instance-index (JoinBridgeManager на driver-factory); eligibility stage 3 расширяется | P1; план §4.3 |
| Каскад-merge | fan-in ≤ 64 (не PriorityQueue на сотнях входов) — перенос в core | P2 |
| Async tail-spill | `OrderByOperator.finish()` — устранить синхронный доп. спилл (TODO «should be asynchronous») | I4; OrderByOperator:226 |
| Спилл-координация в задаче | локальный spiller-координатор (мелкие интерлив-раны → сериализация спиллов) | I11; risks/02 §2.11 |
| Словарные выходы | ownership-transfer/flatten при ревокании ИЛИ словарный вывод за флагом + телеметрия | I6; P3 (метрика copied-vs-referenced) |
| `spillThroughput` | runtime-метрика + warning < 500 МБ/с (сетевые тома) | P8; risks/03 §3.8 |

## Acceptance (гейты фазы 2)

- **Масштаб**: синтетика 15B×17B (экстраполяция 1B→10B) без OOM; max memory/задачу ≤ cap при
  гигантских группах; spill-bytes/строку ≤ 2 при fusion; **re-iterate 10M×10M при 64 МБ:
  bytes-read ≤ L + R·(L/C)** (P4-гейт);
- **FULL OUTER zero-retention**: пик памяти FULL == INNER на тех же данных; гига-NULL-хвост + FULL
  без OOM; probe-хвост 10⁹ без матчей — память плоская;
- **Изоляция**: 4 конкурентные SMJ-задачи на воркере с малым пулом — деградация ≤ 30%;
- **Правильность**: RIGHT/FULL oracle; фильтр-адресация на multi-page группах.

## Откат

Спилл-режим за флагом; RIGHT/FULL — правило не выбирает SMJ (eligibility в stage 3 возвращает
hash), если оператор не готов.

## Согласованность (проверено против корпуса)

- [x] **risks/04 §4.3**: состав/гейты/откат зеркалированы (включая zero-retention-acceptance —
  десятый проход §5.13.3);
- [x] **risks/07 §7.2-7.5**: merge-таблица FULL (drain, padding O(1), anyMatchSurvived) —
  основа outer-driver; OuterPositionIterator НЕ используется (SMJ проще hash-FULL);
- [x] **risks/06 §6.4**: NULL-fast-path распространён на FULL (обе стороны стримятся);
- [x] **risks/03 §3.1-3.4/§3.8** (P1-P4, P8) и **risks/02 §2.4-2.6/§2.11** (I4-I6, I11) —
  все митигации присутствуют в составе;
- [x] **13 §13.x (кейс 15B×17B)**: экстраполяционный acceptance и конфиг-ориентиры
  (partition count 200-400) — соответствуют;
- [x] **12 §12.x**: новых алгоритмов сортировки не вводится (каскад = MergeSortedPages),
  LSD-radix — отдельный perf-трек решения (07 §7.6), не входит в этап.
