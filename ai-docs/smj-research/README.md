# Sorted Merge Join: предварительные исследования

Раздел содержит подготовительные исследования для реализации **Sorted Merge Join (SMJ)** в Trino.

**Мотивация.** В Trino (этом форке) отсутствует sorted merge join — все equi-join выполняются
hash join (lookup join). Hash join требует материализации build-стороны; на десятках миллиардов
строк это многопроходный spilling и риск OOM (особенно при data skew). SMJ даёт bounded memory
(память ~ размер максимальной группы равных ключей, а не build-сторона целиком) и последовательный
I/O — стандартное решение Spark/ClickHouse/Velox для big-big join'ов.

## Карта документов

| # | Документ | Содержание |
|---|---|---|
| 1 | [01-trino-join-internals.md](01-trino-join-internals.md) | Глубокое погружение в текущую JOIN-подсистему: контракты интерфейсов, жизненный цикл hash join, что обязан соблюдать новый оператор |
| 2 | [02-trino-sort-merge-machinery.md](02-trino-sort-merge-machinery.md) | Существующие механизмы сортировки и слияния (OrderByOperator, MergeOperator, MergeSortedPages, компараторы, спилл) — строительные блоки SMJ |
| 3 | [03-smj-integration-points.md](03-smj-integration-points.md) | Полная карта точек интеграции SMJ в planner/optimizer/runtime + модель sortedness + Iceberg |
| 4 | [04-smj-in-other-engines.md](04-smj-in-other-engines.md) | SMJ в других движках: Spark, ClickHouse, DuckDB, Velox, PrestoDB (включая реальные PR), Pinot |
| 5 | [05-brainstorm-java-developer.md](05-brainstorm-java-developer.md) | Brainstorm: Java developer — дизайн оператора, конечный автомат merge, edge cases |
| 6 | [06-brainstorm-java-architect.md](06-brainstorm-java-architect.md) | Brainstorm: Java architect — границы слоёв, feature flags, поэтапный план, совместимость, риски |
| 7 | [07-brainstorm-java-optimization.md](07-brainstorm-java-optimization.md) | Brainstorm: perf-инженер — hot-path, flat-ключи, словарный вывод, спилл, бенчмарки |
| 8 | [08-brainstorm-mpp-architect.md](08-brainstorm-mpp-architect.md) | Brainstorm: MPP-архитектор — распределённые стратегии, шедулинг, skew, cost model |
| 9 | [09-brainstorm-trino-developer.md](09-brainstorm-trino-developer.md) | Brainstorm: Trino-коммиттер — дизайн план-модели, partitioned merging exchange, нарезка PR |
| 10 | [10-synthesis-and-recommendations.md](10-synthesis-and-recommendations.md) | **Синтез**: консенсус 5 ролей, варианты архитектуры, рекомендуемый roadmap, открытые вопросы |
| 11 | [11-unsorted-inputs-scenarios.md](11-unsorted-inputs-scenarios.md) | Несортированные входы: матрица сценариев бакетирования/партиционирования vs join-ключ (S1-S8), какой exchange построит Trino, число hash-партиций |
| 12 | [12-sorting-algorithms-landscape.md](12-sorting-algorithms-landscape.md) | Ландшафт алгоритмов сортировки (external merge sort, radix, replacement selection, PSRS, grace hash) и верифицированная инвентаризация: что есть/нет в Trino |
| 13 | [13-case-study-15b-join-17b.md](13-case-study-15b-join-17b.md) | Кейс-стади: join 15 млрд × 17 млрд строк — план по фазам, объёмы/память/CPU/I/O-математика, SMJ vs spilling hash join, чек-лист внедрения |
| 14 | [14-best-fit-scenarios.md](14-best-fit-scenarios.md) | **Сценарии применимости**: иерархия S1-GOLD..S5, сигналы выбора SMJ при малой памяти, SMJ vs grace hash, практические рецепты подготовки данных |
| 15 | [15-smj-selection-policy.md](15-smj-selection-policy.md) | **Политика выбора SMJ планировщиком (жёсткие рамки)**: MUST-гейты, ресурсные триггеры W1–W4 (когда SMJ оправдан при минимальной RAM), запреты N1–N5 (защита CPU), псевдокод DetermineJoinAlgorithm, пороги α/β/γ/δ, разбор типовых ситуаций (одинаковый бакет + join-ключ) |
| — | [risks/](risks/README.md) | **Углублённое исследование рисков** (поддиректория): корректность (NaN/MAP/NULL-порядки/per-file sortedness), интеграция с движком, производительность + **план реализации с риск-гейтами** и маппингом «риск → тест → фаза»; **[05-verification-report](risks/05-verification-report.md)** — повторная верификация всех фактов по коду (18/18 подтверждены, 2 исправления, 4 новых риска: I13/P11/C8/S-N1) |

## Главные выводы (executive summary)

1. **SMJ не нужен как новое значение `DistributionType`** — единодушно всех ролей. SMJ — это
   локальный алгоритм поверх PARTITIONED-распределения: в плане — новое поле
   `JoinNode.algorithm` (`Optional<JoinAlgorithm>`, единственное значение `MERGE`), а распределение
   остаётся PARTITIONED. Это сохраняет 8 семантических точек матчинга `DistributionType`
   нетронутыми и обратно совместимо при rolling upgrade.

2. **Оператор = два пайплайна + мост** (как у hash join): правая сторона — sink-оператор,
   сортирующий и публикующий отсортированный источник через `JoinBridgeManager`-подобный мост;
   левая — стриминговый merge-оператор. Потоковое слияние PrestoDB-стиля (обе стороны текут
   одновременно) в Trino-модели драйверов ведёт к дедлокам без grouped execution — не наш путь.

3. **Строительные блоки уже есть**: внешняя сортировка (`OrderByOperator` + спилл),
   k-way merge (`MergeSortedPages` + `WorkProcessor.mergeSorted`), кодогенерация компараторов
   (`OrderingCompiler::PageWithPositionComparator` — сравнение двух разных Page), модель
   sortedness (`SortingProperty`/`LocalProperties.match`/`PropertyDerivations`), spill-инфраструктура.

4. **Главная структурная дыра**: merging exchange существует только как GATHER/SINGLE
   (финальный merge ORDER BY — single-node). Для распределённого SMJ нужен **partitioned merging
   exchange** (REPARTITION + orderingScheme) — ослабление инварианта `ExchangeNode:104-111`;
   для MVP можно обойтись сортировкой после обычного REPARTITION.

5. **Iceberg — идеальный источник отсортированных данных**: `sort_order`/`sorted_by` уже есть
   в метаданных (используется write-путём), но `getTableProperties` не сообщает сортировку движку
   (TODO в коде). Закрытие этого TODO + bucketed таблицы (bucket execution, co-location) дают
   zero-shuffle SMJ: без exchange и без хэш-таблиц.

6. **Prior art подтверждает осуществимость**: PrestoDB имеет рабочую Java-реализацию
   (MergeJoinOperator/MergeJoinSinkOperator, PR #17829), Velox — векторизованный merge join
   (Match-окна, словарный вывод, ~10× к построчному), Spark SortMergeJoin — промышленный стандарт,
   ClickHouse — full_sorting_merge/parallel/partial_merge варианты.

Рекомендуемый путь — см. [10-synthesis-and-recommendations.md](10-synthesis-and-recommendations.md).

## Дополнение: несортированные входы и масштаб 15 млрд × 17 млрд строк (документы 11-13)

Исследован worst case — сортировку нужно **строить**, а не получать бесплатно:

- **Co-location работает и для смешанных спек**: `identity(dt) + bucket(user_id, N)` при join по
  user_id — identity-функции отбрасываются в `applyPartitioning`, остаётся `bucket(user_id)`;
  чисто identity-партиционирование (без bucket) co-location **не даёт никогда**; разные bucket
  counts на Iceberg несовместимы (адаптация — TODO в коде) → shuffle второй стороны в bucket-функцию
  первой. Полная матрица S1-S8 — в [11](11-unsorted-inputs-scenarios.md).
- **В Trino нет ни radix sort, ни parallel sort, ни replacement selection, ни order-preserving
  кодирования ключей** — единственная сортировка данных это quicksort по адресам
  (`PagesIndexOrdering`) + однопроходный k-way merge (`OrderByOperator`). Достаточно для MVP,
  radix по flat-префиксу (Spark-паттерн) — главное усиление фазы 2 — [12](12-sorting-algorithms-landscape.md).
- **15B×17B**: hash join с build 17×10⁹ строк практически нежизнеспособен без многократного
  спилл-прохода; SMJ даёт гарантированно 1×write+1×read на сторону (sort-join fusion) и bounded
  память; главное проектное ограничение — **параллелизм join-стадии** (default 100 hash-партиций =
  2 задачи/воркер — поднимать до 200-400) — [13](13-case-study-15b-join-17b.md).
- **Риски**: найден и верифицирован по коду критический риск корректности — **NaN в DOUBLE/REAL
  ключах** (`compare(NaN,NaN)=0`, но `equal(NaN,NaN)=false` → наивный SMJ даёт лишние строки);
  MAP-ключи orderable-нет; Iceberg «sorted» = per-file; **гигантский NULL-хвост идёт в один
  бакет уже в hash-мире — SMJ лечит стримингом**. Полный регистр (36 рисков с решениями) и
  risk-gated план — в [risks/](risks/README.md); сценарии применимости (S1-GOLD bucket+sorted —
  идеальный) — в [14-best-fit-scenarios.md](14-best-fit-scenarios.md).
