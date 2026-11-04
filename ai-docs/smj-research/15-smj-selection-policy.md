# 15. Политика выбора SMJ: жёсткие рамки для планировщика

Документ фиксирует **решающие правила**, по которым `DetermineJoinAlgorithm` (фаза 1 — forced,
фаза 3 — COST_BASED) обязан/запрещён выбирать merge join вместо hash. Цель: SMJ покупает
bounded memory за CPU+IO — правила гарантируют, что мы **не платим CPU там, где RAM хватает**,
и получаем память там, где hash не помещается.

Опорные документы: сценарии [11](11-unsorted-inputs-scenarios.md) (S1–S8), применимость
[14](14-best-fit-scenarios.md), риски [P5/P7/P11](risks/README.md), план
[04, §4.2/§4.4](risks/04-implementation-plan-with-risk-gates.md).

## 15.1. Экономика выбора (почему это вообще вопрос)

| Алгоритм | Память (пик на задачу) | CPU | IO при нехватке памяти |
|---|---|---|---|
| Hash (partitioned) | **B/P** на задачу (build-партиция; B — build после фильтров, P — partition count) | O(L + B) — самый дешёвый | grace-hash: спилл build-партиций + **повторное чтение probe** (до L·(1+1/C) IO) |
| SMJ, входы НЕ готовы | O(сортировочные буферы — revocable, спилл) + текущая build-группа | O(L·log + B·log) + merge O(L+B) — **наценка ~log за сортировку** | линейный: 1 write + 1 read рана |
| SMJ, входы ГОТОВЫ (sorted+colocated) | та же bounded, групп меньше | O(L + B) — **сопоставимо с hash, без хэш-таблицы** | ноль (нет shuffle, нет спилла) |

Вывод: SMJ оправдан ⇔ **(а)** памяти для B/P заведомо мало (hash умрёт или уйдёт в дорогой
спилл), **или (б)** входы уже отсортированы/бакетированы так, что наценка log ≈ 0. Иначе hash.

## 15.2. MUST-гейты (структурные; при любом нарушении — только hash)

Безусловно обязательны (из плана фазы 1, все — с якорями рисков):

1. `joinType ∈ {INNER, LEFT}` (RIGHT/FULL — фаза 2);
2. equi-критерии непусты (cross join — никогда);
3. все ключи обеих сторон `isOrderable()`; **DOUBLE/REAL — исключены до C1-guards** (NaN:
   `compare(NaN,NaN)=0` при `equal(NaN,NaN)=false` — C1); MAP — навсегда (C2);
4. `distributionType == PARTITIONED` (REPLICATED-join — не для SMJ);
5. `retryPolicy == NONE` (merge-источники не ретраятся — I1);
6. `distributed_sort == true` (иначе visitSort сериализует входной пайплайн — P11;
   default true, OptimizerConfig:64);
7. `prefer_merge_join != OFF` (default OFF — ноль изменений поведения).

## 15.3. Состояния готовности входов (считываются из свойств плана)

- **sortedL / sortedR** — `LocalProperties.match(actual, canonical)` полностью удовлетворён:
  фактическая сортированность стороны покрывает **все** equi-ключи в каноническом порядке
  (ASC, NULLS_LAST — §4.9 плана). Префикс валиден: sorted by (k, dt) ⊇ join by (k).
- **colocated** — `getCommonPartitioning(left, right)` даёт handle, описывающий
  **bucket по столбцу, входящему в equi-ключи, с одинаковым N и трансформом на обеих сторонах,
  в пределах одного каталога** (S-N1). Mixed-спека `identity(dt)+bucket(k,N)` — работает
  (лишние функции отбрасываются, IcebergMetadata.applyPartitioning).
- Из них: **S1-GOLD = colocated ∧ sortedL ∧ sortedR** — «золотой» случай: ноль exchange,
  ноль сортировки, SMJ дешевле hash и по RAM, и по CPU (нет хэш-таблицы).

Нюанс пользовательского примера «одинаковый бакет»: если join по `(k, dt)`, а бакет только по
`k` — co-location **сохраняется** (bucket по подмножеству ключей), но sortedness требуется по
`(k, dt)`: файлы, отсортированные только по `k`, дают grouped-by-k вход → до-сортировка
локальна и дешёвая (почти-сортированные данные, quicksort 3-way — P10).

## 15.4. Ресурсные триггеры W (когда SMJ оправдан — «RAM минимальна»)

Обозначения: `U` — usable-память задачи (user+revocable pool на задачу при текущей конкурентности);
`Bp` — ожидаемый размер **наибольшей** build-партиции `max(B/P, skew-оценка)` (P ≤
`query.max-hash-partition-count`, default 100 — QueryManagerConfig:79; P уже определён
правилом `DetermineJoinDistributionType`, наш выбор бежит после него);
`L` — probe bytes; пороги α, β, γ — конфиг (см. 15.7).

| # | Условие | Почему SMJ | RAM-профиль |
|---|---|---|---|
| **W1** | `colocated ∧ sortedL ∧ sortedR` (S1-GOLD) | нет shuffle, нет sort, нет хэш-таблицы — SMJ доминирует hash по обеим осям | bounded, спилл почти не нужен |
| **W2** | `Bp > α·U` — build-партиция не влезает в память задачи | hash либо OOM (спилл off), либо grace-hash с ре-ридом probe; SMJ линеен по IO | bounded независимо от B |
| **W3** | `L > γ·U ∧ B > γ·U` (big-big, спилл неизбежен с обеих сторон) | SMJ: 1 write + 1 read на ран; grace-hash: повторные проходы probe | bounded |
| **W4** | `(sortedL ∨ sortedR) ∧ Bp > β·U` | одна сторона уже готова — наценка log только на второй; build велик | bounded |

**W1 безусловен** (в пределах MUST); W2/W3/W4 — сравнением с hash (в COST_BASED — через
`calculateSortedMergeJoinCostWithoutOutput` с memory-компонентой, фаза 3).

## 15.5. Жёсткие запреты N (когда планировщик обязан выбрать hash — защита CPU)

| # | Условие | Причина |
|---|---|---|
| **N1** | `Bp ≤ β·U ∧ спилл-hash не требуется` — build влезает в память **и** входы не готовы (не W1) | SMJ = чистая CPU-наценка (сортировка) без единицы выгоды |
| **N2** | рабочий набор hash влезает в LLC (~десятки MB/ядро) **и** не W1 | «тёплый» hash быстрее холодного merge (P5) |
| **N3** | build мал (< broadcast-порога) | дажe не предмет разговора: broadcast/hash |
| **N4** | обе стороны не отсортированы **и** `Bp ≤ α·U` | сортировать оба входа ради join, который и так влезает — регресс |
| **N5** | ключи DOUBLE/REAL (до C1-guards) или не-orderable | корректность (C1/C2) — не торгуется |

Правило-инвариант: **никогда не обменивать RAM на CPU, если RAM достаточно** (N1/N2/N4);
**никогда не обменивать корректность на что-либо** (N5/MUST).

## 15.6. Решающее правило (псевдокод `DetermineJoinAlgorithm`)

```text
selectAlgorithm(join, stats, session):
  if not MUST_1..7: return HASH                        # 15.2
  sortedL, sortedR = match(LocalProperties стороны, canonical(equiKeys))
  colocated = getCommonPartitioning(left, right) покрывает bucket по equi-ключу
  U  = usableTaskMemory(session);  Bp = max(B/P, skew);  L = probeBytes
  if colocated and sortedL and sortedR: return MERGE   # W1 — S1-GOLD
  if mode == FORCE:  return MERGE                      # тесты (нарушения W — warning в EXPLAIN)
  if mode == PREFER:                                    # MVP
      return MERGE if (Bp > α·U or (L > γ·U and B > γ·U) or ((sortedL or sortedR) and Bp > β·U))
             else HASH                                  # W2/W3/W4
  if mode == COST_BASED:                                # фаза 3
      return MERGE if cost_smj(with sortedness discount, memory relief) < cost_hash − δ
             else HASH
  return HASH                                           # OFF/default
```

Приоритет режимов: `OFF` (default) → `PREFER` (MVP, правила W) → `COST_BASED` (фаза 3) → `FORCE`
(только тесты; в проде не рекомендуется — имён W-нарушений не знает, может деградировать CPU).

## 15.7. Пороги (конфиг фазы 3; в PREFER — те же дефолты)

| Порог | Смысл | Default | Config (OptimizerConfig) |
|---|---|---|---|
| α | memory-pressure: build-партиция не влезает | **0.7** | `optimizer.join-merge-memory-pressure-threshold` |
| β | small-build: «влезает с запасом» | **0.3** | `optimizer.join-merge-small-build-threshold` |
| γ | big-big: обе стороны > γ·U | **1.0** | `optimizer.join-merge-big-side-threshold` |
| δ | CPU-допуск COST_BASED (SMJ выбирается только если дешевле на δ) | **10%** | `optimizer.join-merge-cost-margin` |

Дефолты — стартовые, калибруются JMH/макро-треком (07 §7.6) на фазе 3; α/β/γ ηе участвуют в
W1 (S1-GOLD безусловен).

## 15.8. Что нужно самому SMJ (минимальные ресурсы оператора)

- **RAM**: сортировочные буферы сторон (revocable → спилл при ревокации) + **текущая build-группа**
  (MVP: ограничение сверху на группу, гигантские группы — фаза 2 SpillingMatchBuffer) + O(k)
  курсоров merge. Не нужна build-хэштаблица O(B/P).
- **Disk**: раны сортировки ≤ (L+B)·(1+ε); фаза 2 — раны групп. Следить за P8 (сетевые тома).
- **CPU**: по 1 merge-драйверу на задачу на сторону (single-stream от merge-источника —
  `setDriverInstanceCount(1)`, LocalExecutionPlanner:918); параллелизм — выше по пайплайну
  (N сортировщиков + LOCAL merging exchange — P11-митигация).
- **Сетевой shuffle**: только если НЕ colocated (REPARTITION по equi-ключам обеих сторон) —
  как у hash; в S1-GOLD — ноль.

## 15.9. Разбор типовых ситуаций (включая пример пользователя)

| Ситуация | Классификация | Решение |
|---|---|---|
| `T1: bucket(user_id,256)+sorted(user_id)`, `T2: bucket(user_id,256)+sorted(user_id)`, один каталог, `JOIN ON user_id` | **S1-GOLD** (colocated ∧ sortedL ∧ sortedR) | **SMJ всегда** (W1): ноль shuffle/sort, RAM bounded, CPU ≤ hash |
| Тот же бакет, файлы НЕ отсортированы | colocated, не sorted | Локальная сортировка каждого бакета (группированные данные, дёшево) → SMJ оправдан при больших B (W2/W4), иначе hash |
| Бакет по `user_id`, join по `(user_id, dt)` | colocated (bucket ⊆ ключей), sortedness нужна по `(user_id,dt)` | До-сортировка внутри бакета; SMJ при Bp > β·U (W4) или big-big (W3) |
| Разные N бакетов (256 vs 128) | НЕ colocated (handle не равен; power-of-2 адаптация — TODO в Iceberg) | Полный shuffle + sort обеих → только W2/W3 |
| Sorted, но не бакетированы | не colocated — shuffle ломает порядок | SMJ только по памяти (W2/W3); ожидания — P7 |
| B = 2 ТБ, P = 100, U = 11 ГБ → Bp = 20 ГБ > 0.7·11 | W2 (память) | **SMJ**; hash уйдёт в спилл или OOM |
| B = 30 ГБ, P = 100, U = 11 ГБ → Bp = 0.3 ГБ, входы не готовы | N1/N4 | **hash** — сортировать 60+ ГБ ради join, влезающего в память, нельзя |
| Всё в LLC (десятки MB), любые входы | N2 (кроме W1) | **hash** |

## 15.10. Инвариант для CI

- Каждый W/N-переход покрыт план-тестом (`TestDetermineJoinAlgorithm`): матрица
  {colocated × sortedL/R × Bp/U × L/U} → ожидаемый алгоритм; OFF-режим — 0 диффов планов.
- Численные пороги α/β/γ/δ вынесены в OptimizerConfig — в тестах фиксируются явно,
  CI-независимы от калибровки.

## 15.11. Сценарии для memory-constrained 15B×17B (hash заведомо не пройдёт)

Числа (продолжение [13](13-case-study-15b-join-17b.md)): probe L = 15 млрд (~1.5 ТБ),
build B = 17 млрд (~1.7 ТБ) при ~100 Б/строка. Кластер 10 воркеров × 64 ГБ heap →
пул ≈ 44 ГБ/воркер (−30% headroom), U ≈ 11 ГБ/задачу при 4 конкурентных.
Hash partitioned при P=100: **B/P = 17 ГБ > 0.7·U = 7.7 ГБ** → W2 срабатывает: без спилла — OOM,
со спиллом — grace-hash с пере-чтением probe (~1.5 ТБ × раунды). SMJ bounded во всех сценариях.

| # | Сценарий (обе таблицы Iceberg) | Co-location | Sort-стоимость | Shuffle/IO | Вердикт |
|---|---|---|---|---|---|
| 1 | `bucket(k,256)` (одинаковые N и каталог, по одной спеке) **+ sorted by (k)**, join по k — **S1-GOLD** | да | **0** (готово) | **ноль** | идеал: SMJ доминирует hash по RAM и CPU (W1) |
| 2 | `bucket(k,256)` same N, файлы **не** sorted | да | локальная в каждом бакете; данные сгруппированы по k — почти-сортированные (P10), дёшево | ноль | отличная замена; SMJ при больших B (W2/W4) |
| 3 | `bucket(k,N)` + join по **(k, dt)** | да (bucket ⊆ ключей) | до-сорт (k,dt) внутри бакета — локальный | ноль | хороший (W4 при больших B) |
| 4 | sorted(k), **не** бакетированы | нет | сортировка теряется после shuffle: сортировать после exchange | shuffle 3.2 ТБ | SMJ только по памяти (W2/W3); ожидания — P7 |
| 5 | generic unsorted (15B×17B «как есть») | нет | обе стороны после shuffle | shuffle + spill-раны ≤ 3.2 ТБ, линейно | SMJ — единственный без-OOM вариант (W2: hash неприменим по условию) |
| 6 | разные N (256 vs 128) / разные каталоги | нет (handle не равен; S-N1) | как 4/5 | shuffle | деградация к 4/5 |
| 7 | identity-партиционирование по k (не bucket) | нет (value-based) | как 4/5 | shuffle | выгоды нет (S5) |

Рекомендация по подготовке данных (DDL из [14](14-best-fit-scenarios.md)): обе таблицы в
**одном каталоге**, `PARTITIONED BY (bucket(k, 256))`, `WRITE ORDERED BY k`, единый текущий
`sortOrderId` у всех файлов — сценарий 1. NULL-хвост при INNER дополнительно отрежется
план-фильтром `k IS NOT NULL` до shuffle (C9, [risks/06](risks/06-null-keys-risks.md)).
