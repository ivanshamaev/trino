# 8. Brainstorm: MPP-архитектор — распределённая стратегия SMJ

> Роль: архитектор MPP-движков. Проверенные факты: `ExchangeNode` (104-111: merging только GATHER;
  REMOTE ⇒ SINGLE_DISTRIBUTION, LOCAL ⇒ FIXED_PASSTHROUGH); `createMergeSource` (merging-источник ⇒
  `driverInstanceCount=1`, `retryPolicy==NONE`); `AddLocalExchanges.visitSort` (distributed sort =
  N парциальных сортировщиков + локальный merging exchange); `PartitioningHandleReassigner` +
  `Metadata.applyPartitioning` (co-location); `CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput`;
  `PhasedExecutionPolicy` → фазы из целых стадий, lifespans отсутствуют.

**Главный тезис**: SMJ в Trino стоит делать не как замену hash join, а как (а) специализированный
путь для уже отсортированных/co-located Iceberg-данных (zero-shuffle, почти бесплатный) и (б) fallback
для «big-big» join'ов в spill-режиме, где hash платит многопроходный спилл. CBO должен выбирать SMJ
почти только тогда, когда цена сортировки σ≈0 или build заведомо не лезет в память.

## 8.1. Распределённая стратегия — сравнение вариантов

**(А) Spark-подобный hash-repartition + локальная сортировка.** Сегодня невыразима без правки
планера (инвариант merging=GATHER). Два подварианта:
- **A1 (сортировка после exchange)**: `partitionedExchange(REMOTE, keys)` → спилл-сортировка в
  задаче → SMJ. Минус: exchange уже перемешал; sort — отдельный полный проход.
- **A2 (partitioned-merging exchange)**: на мапперах парциальная сортировка (как средняя стадия
  distributed sort), затем `REPARTITION(REMOTE)` с orderingScheme; потребитель мержит K отсортированных
  стримов **на партицию**. Это не «новый exchange», а ослабление инварианта + замена ExchangeOperator
  на per-partition heap-merge (по сути MergeOperator на hash-партицию, не на всю выборку). Кодовая цена
  умеренная: RemoteSourceNode уже несёт orderingScheme, компиляция в merging-источник уже есть —
  убрать `setDriverInstanceCount(1)` в пользу per-partition драйверов. **A2 строго лучше A1.**

**(Б) Co-located zero-shuffle.** Механика есть: bucket execution (`IcebergPartitioningHandle`) +
`PartitioningHandleReassigner` «вталкивает» партиционирование в TableScan через `applyPartitioning` —
обе стороны в одной стадии без exchange. Чего нет: (i) Iceberg не отдаёт sort order (TODO в коннекторе)
— критично: SMJ без сортировки возможен только если данные физически упорядочены по префиксу join-ключа;
(ii) co-location требует одинаковых bucket-функций (колонки, count, ровно одна спека) — при разных
bucket counts `getCommonPartitioning` пуст и PlanFragmenter откатывается на hash-repartition.
**Вердикт**: самый дешёвый по runtime и самый дорогой по требованиям к данным. Для ETL —
самоусиливающийся сценарий: пишем INSERT'ом в bucketed+sorted таблицы → будущие join'ы без shuffle вовсе.

**(В) PrestoDB grouped execution (lifespans).** В Trino нет lifespans/section executions;
`PhasedExecutionPolicy` строит фазы из целых стадий. Понести grouped execution — новый размерный
уровень в SqlStage/PipelinedStageExecution (задачи на lifespan), per-lifespan буферы, NodeScheduler,
back-pressure, UI — сравнимо с проектом PrestoDB (годы). **Для SMJ не нужно**: память SMJ и так
bounded (курсоры + окно текущей группы), spill решает остаток. Grouped execution был нужен PrestoDB
для *небаунди* hash-таблиц. **Вердикт: не тащить**; если появится fault-tolerant grouped mode — SMJ
встанет естественно (группа = lifespan).

**(Г) ClickHouse parallel_full_sorting_merge.** По сути это A2 (hash-sharding + независимые per-shard
merge join'ы). Что стоит заимствовать отдельно: (i) **range-партиционирование** (shard by PK ranges)
вместо hash — сохраняет глобальный порядок, включает read-in-order (пропуск сортировки) и убивает
hot-bucket skew, но требует семплинга/гистограмм (Iceberg min/max по партициям — частичная замена);
(ii) **пре-фильтрация по множеству ключей другой стороны** — это dynamic filtering, который уже есть;
для SMJ можно бесплатно получить min/max правой стороны как ранний range-фильтр левой.

## 8.2. Где мержить: один merger или M

Сегодня merging remote source — `driverInstanceCount=1`. Для SMJ правильный выбор **per партиция**,
не per задача: hash exchange обычно даёт partitionCount == taskCount → per-task один merge-драйвер
на сторону естественно. При partitionCount = k×taskCount (анти-skew) — по драйверу на партицию, каждый
со своим SMJ: изоляция skew, bounded memory, ноль shared-состояния.

Стыковка N сортировщиков → M merger'ов внутри задачи **уже решена**: паттерн
`AddLocalExchanges.visitSort` = «N fixed-parallel сортировщиков → mergingExchange(LOCAL, PASSTHROUGH)
→ LocalMergeSourceOperator». Для co-located пути (Б):

```
Скан (P драйверов) → partialSort → localExchange(PASSTHROUGH, hash по бакету)
      → LocalMergeSource(k) per бакет → MergeJoinDriver(бакет)
```

Join-драйвер получает **два** merge-источника (левый/правый) — фундаментальная проблема: у Trino-оператора
один input. Идиоматичное решение (PrestoDB MergeJoinSinkOperator / Velox CallbackSink): **правая сторона
— отдельный пайплайн, заканчивающийся sink'ом в мост** (аналог JoinBridgeManager), левая стримится.
Reuse инфраструктуры JoinBridgeManager — осознанный выбор, чтобы не трогать шедулер.

## 8.3. Планировка стадий и шедулинг

Границы фрагментов (A2):
- Стадии-источники: `Scan → Filter/Project → partialSort` (по одной на сторону);
- JOIN-стадия (FIXED_HASH): два `RemoteSourceNode(orderingScheme)` → merge → SMJ → остальной план.

Ключевое отличие от hash join: **у SMJ нет build-фазы как барьера**. Обе стороны потоковые, семантика —
«синхронизация по ключу»: левая строка с ключом k выдаётся, когда известно, что правая дошла до ключа > k
(watermark-wait). В bridge-модели (build материализуется) этот вопрос снимается полностью — probe просто
ждёт `whenBuildFinishes()`, как в hash join. В streaming-варианте MergeJoinOperator блокируется, когда
мост пуст и watermark не достигнут — так PrestoDB закрыл проблему двух блокирующихся сторон; deadlock
невозможен, если мост bounded (backpressure в exchange-буферы вверх по потоку — у Trino pull-based
exchange с `maxBufferedBytes`, backpressure бесплатный). Новых барьеров и изменений ExecutionPolicy
не требуется — фазы уже гарантируют старт upstream раньше.

Dynamic filtering **из** SMJ не работает (нужен полный набор ключей), но SMJ **потребляет** DF,
собранные join'ами выше по плану — сохраняется автоматически.

## 8.4. Skew

- **Гигантские группы одинаковых ключей**: SMJ держит текущую группу правой стороны целиком (мост) —
  100M строк × wide row = OOM. Митигации: (i) spill группы правой стороны (спилл на границе группы,
  стрим левой) — Velox-паттерн «память ~ макс. группа»; (ii) salting: planner-перепись ключа →
  (key, salt) для стороны-источника дублей + репликация второй стороны ×S (правило уровня
  OptimizeDuplicateInsensitiveJoins), только при низком NDV по статистикам; v2.
- **Hot buckets (partition skew)**: hash по ключу с низким NDV ⇒ весь вес в одном бакете. По возрастанию
  цены: partitionCount = k×taskCount + per-partition драйверы (лечит intra-node, не cross-node — бакет
  прибит к ноде NodePartitionMap); range-партиционирование (8.1Г); dynamic repartitioning — только в FTE
  (AdaptivePlanner). Честно зафиксировать: в pipelined-режиме cross-node skew для SMJ неизлечим без
  range — аргумент развивать пути (Б)/(Г).

## 8.5. Cost model

В терминах io.trino.cost (LocalCostEstimate.of(cpu, memory, network); свёртка CostComparator с весами):

```
Hash (PARTITIONED):  net = B_l + B_r
                    cpu = (B_l + B_r)·h + B_r·b        (h — hash/probe, b — build)
                    mem = B_r                            (resident build)

SMJ (A2), σ∈[0,1] — «sortness» (σ=0 вход уже упорядочен):
                    net = B_l + B_r
                    cpu = (B_l + B_r)·(1 + σ·log(r)·α + β)  (r — runway локальной сортировки)
                    mem = K·page + G_max                     (K курсоров + макс. группа; ≈0)
```

SMJ выигрывает, когда `w_mem·B_r > w_cpu·(B_l+B_r)·σ·α·log r` — два режима: **σ≈0** (отсортированные
входы — выигрывает почти всегда, кроме крошечных build'ов, где broadcast дешевле) и **spill-режим hash
join'а** (эффективная стоимость hash растёт кратно). Проигрывает при σ=1 и B_r ≪ B_l или при очень
низком NDV (квадратичный вывод).

Интеграция: **не** расширять `calculateJoinCostWithoutOutput`, а отдельное правило `DetermineJoinAlgorithm`
рядом с `DetermineJoinDistributionType` (после, до AddExchanges) — решение требует знания
упорядоченности: trait'ов в Trino нет, значит (i) явный SortNode/TopN над входом; (ii) ordering в
`ConnectorTableProperties` (заодно закрывает Iceberg TODO). Unknown-статы: консервативно — SMJ не
выбирается (fallback как сегодня); включение только при known row counts и (σ < порога ИЛИ build >
memory threshold). Это гарантирует «не ломать существующий CBO».

## 8.6. Failure modes и деградация

- **Spill кончился**: сегодня fail запроса; runtime-переключение hash→SMJ невозможно в pipelined-режиме
  (adaptive-правила только в FTE). В FTE возможно: AdaptivePlanner между re-execution'ами меняет
  join-стратегию — единственное место, куда стоит просить «runtime hash→SMJ».
- **Важно**: текущий merging remote source требует `retryPolicy == NONE` — partitioned-merging exchange
  сломает FTE, пока exchange-clients не научатся консистентно мержить с retry (упорядоченность после
  повторной доставки должна восстанавливаться — решаемо, retry идемпотентен по страницам, но это работа).
- **ETL-конфигурация**: spill_enabled + спиллеры на локальных SSD; для больших join'ов FTE
  (retry_policy=TASK); query_max_stage_count поднять; join_distribution_type=PARTITIONED для
  детерминизма; bucketed+sorted Iceberg как целевая схема хранения — включается путь (Б).

## 8.7. End-to-end рекомендация

```
Общий путь (A2):
 Stage 1(SOURCE)  Scan(L)→Filter→Proj→partialSort(k)   Stage 2(SOURCE)  Scan(R)→Filter→Proj→partialSort(k)
        └─ REPARTITION(remote,hash(k)) + merging(k) ───────┘ REPARTITION(remote,hash(k)) + merging(k)
                                   ┌────────────────────────┴────────────────────────┐
                                   ▼
 Stage 3 (FIXED_HASH, per задача/бакет):
   [merge(k) левого]→ MergeJoinOperator(k) → Project → output
   [merge(k) правого]→ MergeJoinSink → Bridge(watermarks) ──┘

Ускоренный путь (Б): Stage 1 = Stage 3: co-located bucketed сканы → localExchange(passthrough,hash)
   → LocalMergeSource(k)×2 → MergeJoinDriver. Ноль REMOTE exchange.
```

**Роадмап**: (1) Iceberg sort order в getTableProperties — дёшево, полезно всем; (2) оператор: inner+left,
мост на JoinBridgeManager-подобном lifecycle, spill группы; (3) ослабление инварианта ExchangeNode →
partitioned-merging exchange (+FTE retry); (4) CBO-правило + session-флаг; (5) FULL/RIGHT (drain через
outer-канал как LookupOuterOperator), skew (range-партиционирование, salting); (6) бонус: SMJ выдаёт
**отсортированный** поток — переиспользование для downstream window/sort бесплатно.

**Риски**: буферизация правой группы = O(G_max) (mitigate spill); коллация/NULL-семантика порядка vs
hash-семантика join; CBO-регрессии на unknown-статах; stage count; FTE-несовместимость merging exchange.

**Открытые вопросы**: (1) range-партиционирование без trait'ов — семплировать или верить Iceberg-статам;
(2) водяные знаки моста: гранулярность страница/строка и цена блокировки (для streaming-варианта);
(3) «частично упорядоченные» входы (σ между 0 и 1 — файлы отсортированы внутри, не снаружи);
(4) стоит ли правую группу сразу фильтровать по левому min/max (аналог CH max_rows_in_set_to_optimize_join)
— dynamic range filter из SMJ.
