# 11. Несортированные входы: сценарии бакетирования/партиционирования vs join-ключ

Контекст: SMJ проектируется для join 15 млрд × 17 млрд строк; входы **не отсортированы**, а
партиционирование может не совпадать с join-ключом. Документ отвечает на вопрос: **какой план
обмена (exchange) построит Trino в каждой конфигурации таблиц** и что это значит для SMJ.
Все факты проверены по коду этого репозитория.

## 11.1. Механика построения распределения join (AddExchanges.planPartitionedJoin)

`core/trino-main/.../sql/planner/optimizations/AddExchanges.java:970-1026` — три исхода:

```java
// 1) Левая сторона уже node-partitioned по leftSymbols (например, bucketed Iceberg-скан
//    с активированным connector partitioning):
right = node.getRight().accept(this, PreferredProperties.partitioned(rightPartitioning));
// rightPartitioning = партиционирование ЛЕВОЙ, транслированное на правые символы.
// Если правая активирует СОВПАДАЮЩЕЕ connector-партиционирование — exchange НЕТ (co-located).
// Если нет — right = partitionedExchange(REMOTE, rightPartitioning)
//   ⇨ ПРАВАЯ перемешивается в партиционирование ЛЕВОЙ (в т.ч. в её bucket-функцию!).

// 2) Иначе, если правая node-partitioned по rightSymbols:
//   ⇨ ЛЕВАЯ перемешивается в партиционирование ПРАВОЙ.

// 3) Иначе: partitionedExchange(REMOTE, left, leftSymbols) + partitionedExchange(REMOTE, right, rightSymbols)
//   ⇨ ПОЛНЫЙ shuffle обеих сторон (FIXED_HASH_DISTRIBUTION).
```

Плюс: при `colocated_join=false` (default **true**, `optimizer.colocated-joins-enabled=true`)
и нескольких источниках — принудительная перепартицификация правой стороны.

## 11.2. Когда Iceberg-скан даёт «бесплатное» партиционирование (bucket execution)

`plugin/trino-iceberg/.../IcebergMetadata.java`:

**`getTablePartitioning` (~823)** — необходимые условия:
- session `bucket_execution_enabled` (default **true**);
- **ровно одна партиционная спека** на таблицу: `icebergTable.specs().size() != 1 → empty`;
- таблица партиционирована.

**`applyPartitioning` (~1091)** — активация под конкретный join:
- запрошенные колонки ⊆ партиционных колонок таблицы;
- из всех partition-функций спеки **оставляются только функции по запрошенным колонкам**
  (функции по другим колонкам отбрасываются — `withTopLevelColumnIndex`/фильтр);
- среди оставшихся должна быть хотя бы одна **BUCKET**-функция
  (`noneMatch(BUCKET::equals) → empty` — чисто value-based партиционирование (identity/truncate)
  **отклоняется**: «can hurt performance if there is a filter on the partitioning columns»);
- если движок передал handle другой стороны (`partitioningHandle.isPresent()`) — он обязан
  **точно равняться** выведенному (`equals`, тот же набор трансформов и bucket count);
  адаптация кратных bucket counts — **TODO в коде, не реализована**;
- `MetadataManager.getCommonPartitioning` → default-реализация SPI: совместимы **только
  идентичные** handle (Iceberg свой `getCommonPartitioningHandle` не переопределяет — в отличие
  от Hive, комментарий в AddExchanges про «different bucket counts» к Iceberg не применим).

**Ключевой вывод**: спека `identity(dt) + bucket(user_id, 64)` при join **только по user_id**
**работает**: identity(dt) отбрасывается, handle = `[bucket(user_id,64)]`; каждая сплит-группа
(партиция `(dt, bucket)`) назначается в бакет по значению `bucket(user_id)` — все строки одного
user_id (из любых dt) попадают в один бакет. Co-location корректен.

## 11.3. Матрица сценариев (конфигурация таблиц → план → что делает SMJ)

Обозначения: L — левая (probe), R — правая (build); join по `k`; «shuffle» = объём
repartition-exchange; «sort» = локальная внешняя сортировка (обязательна, т.к. входы не отсортированы).

| # | Конфигурация | Exchange, который построит Trino | Для SMJ |
|---|---|---|---|
| **S1** | L: `bucket(k, N)`, R: `bucket(k, N)` (одинаковые N и трансформы, по одной спеке, **один каталог** — `getCommonPartitioning` требует равенства catalogHandle) | **Ноль exchange**: оба скана активируют одинаковый handle → co-located join (`colocated_join=true`) | Только локальная сортировка каждого бакета; лучший несортированный кейс |
| **S2** | L: `bucket(k, 64)`, R: `bucket(k, 128)` (разные N) | R активироваться не может (handle ≠) → **shuffle только R** в bucket-функцию L (64 бакета) | Сортировка обеих сторон; L сортируется «на месте» (без exchange), R — после shuffle |
| **S3** | L: `bucket(k, N)`, R: непартиционирована (илиspecs>1 / bucket_execution off) | Аналогично S2: **shuffle только R** в партиционирование L | L сортируется на скане, R — после shuffle |
| **S4** | L и R: `identity(dt) + bucket(k, N)` (или `bucket(a)+bucket(k)`), join по k | **Ноль exchange** (S1: лишние функции отбрасываются, handle `[bucket(k,N)]` с обеих сторон совпадает) | Как S1; внутри бакета строки из всех dt — сортировка всё равно локальная |
| **S5** | L и R: партиционированы **identity** по k (без bucket) | applyPartitioning → empty (value-based) → **полный shuffle обеих сторон** по k (FIXED_HASH) | Сортировка обеих сторон после shuffle |
| **S6** | Бакетированы по **другому** полю (например `bucket(region)`), join по k ∉ {region} | Запрошенные колонки ⊄ партиционных → **полный shuffle обеих сторон** | То же |
| **S7** | L: `bucket(k)+bucket(other)`, R: `bucket(k, N)` с совпадающим по k | L отбрасывает `bucket(other)` → handle `[bucket(k,N)]` = R's → **ноль exchange** | Как S1 |
| **S8** | Обе непартиционированы | **Полный shuffle обеих сторон** | Классический Spark-подобный путь |

Правила-выводы:
1. Co-location требует **bucket-функцию по (подмножеству) join-ключей с одинаковым bucket count и
   трансформом на обеих сторонах** **в пределах одного каталога** (`getCommonPartitioning` —
   равенство `catalogHandle`; разные каталоги → полный shuffle даже при одинаковых бакетах);
   identity-партиционирование само по себе никогда не даёт co-location
   (это фильтрация, не хэш-распределение).
2. Если хотя бы одна сторона бакетирована по join-ключу — shuffle только второй стороны
   (**в bucket-функцию первой**, не в generic hash!) — это вдвое дешевле полного shuffle.
3. Во **всех** сценариях несортированных входов SMJ обязан **строить сортировку**; различие только
   в объёме сети. Отсюда: сортировочная подсистема — главный объект проектирования (документы 12-13).

## 11.4. Число hash-партиций (parallelism join-стадии)

- `query.max-hash-partition-count` = **100** (default, `QueryManagerConfig`), min = 4 (50 для write).
- `DeterminePartitionCount` (`.../optimizations/DeterminePartitionCount.java:126-208`):
  ```
  partitionCount = max(
      outputSizeInBytes / max(minInputSizePerTask = 5 GB, queryMaxMemoryPerNode),
      outputRows / minInputRowsPerTask = 10M,
      minPartitionCount)
  ```
  ограничено `maxHashPartitionCount=100` и «2×maxPossiblePartitionCount»
  (`TaskCountEstimator.estimateHashedTaskCount` ≈ воркеры × task.concurrency); при близости к
  максимуму — не ставится вовсе (используется дефолт планирования: partitionCount = числу нод…).
- Для BIG join (15B×17B): rows-based даёт тысячи → упирается в **100**. На кластере 50 воркеров
  это 100 join-задач = **2 задачи/воркер**; merge-драйвер у нас один на задачу (MVP) ⇒
  **2 драйвера на 32-ядерной машине — узкое место**. Рекомендация: для больших SMJ-join'ов
  поднимать `query_max_hash_partition_count` до 2–4× числа воркеров (см. 13, §13.6).
- `task.concurrency` = `clamp(nextPowerOfTwo(cores), 2, 32)` — параллелизм **внутри** задачи
  (сканы/локальные exchange); join-стадия с `setDriverInstanceCount(1)` его не использует.

## 11.5. Свежесть статистик (нужно cost-модели)

Row counts для 15B/17B известны **до выполнения**: `TableStatisticsReader` суммирует
`recordCount` из манифестов Iceberg (с учётом enforced/unenforced предикатов) — см.
`ai-docs/10-iceberg-statistics-optimization.md`. `DeterminePartitionCount` и
`CostCalculatorUsingExchanges` уже потребляют эти числа ⇒ cost-выбор SMJ vs hash и подбор
partition count не упираются в unknown-статы (NDV из puffin-статистик — опциональное усиление
для skew-детекции).

## 11.6. Следствия для дизайна SMJ (сводка)

1. **SMJ-план всегда = (0/1/2) exchange + per-бакет внешняя сортировка + merge**. Сортировку нельзя
   «элитировать» — её надо планировать явно: в MVP — `SortNode` над каждым входом после exchange;
   co-located случаи (S1/S4/S7) — `SortNode` прямо над TableScan (частичная сортировка на сканах,
   затем локальный merging exchange по образцу distributed sort).
2. При S1/S4/S7 сортировку выгодно делать **на скан-стадии частично** (partial sort по сплитам) и
   сливать partitioned merging exchange'ом (фаза 3 из синтеза) — тогда join-задача только мержит.
3. В S2/S3 (односторонний shuffle) правая сторона приходит через exchange — её сортировка
   неизбежно в join-стадии (полная, со спиллом); левую можно отсортировать заранее.
4. Bucket count разный (S2) или bucket по чужому полю (S6) — SMJ не хуже hash join по сети
   (тот же exchange), проигрывает только ценой сортировки; при больших build-сторонах это
   оправдано (документ 13).
