# 10. Синтез: консенсус, варианты архитектуры, рекомендуемый roadmap

Этот документ сводит результаты [01-03] (исследование Trino), [04] (другие движки) и brainstorm'а
пяти ролей [05-09] в целостную рекомендацию для реализации Sorted Merge Join в Trino.

## 10.1. Консенсус всех ролей (что решено)

Ниже — позиции, по которым все (или 4 из 5) ролевых агентов сошлись независимо:

1. **Не трогать `DistributionType`** — SMJ не третий способ распределения, а локальный алгоритм
   поверх PARTITIONED. В плане — новое поле **`JoinNode.algorithm: Optional<JoinAlgorithm>`**
   (значение `MERGE`), distribution остаётся PARTITIONED. Аргументы: 8 семантических точек матчинга
   остаются нетронутыми; инварианты RIGHT/FULL не дублируются; Jackson-совместимость при rolling
   upgrade (старый воркер игнорирует неизвестное Optional-поле → деградация до hash, не падение);
   нет новых PlanNode-типов → PlanFragmenter/ValidateDependenciesChecker/PlanPrinter не ломаются.
   *(Явно: architect — «аргументирую жёстко», trino dev — «моя рекомендация», mpp — отдельное правило,
   java dev — не спорит, строит оператора.)*

2. **Отдельное правило `DetermineJoinAlgorithm`**, а не расширение `DetermineJoinDistributionType`
   (SRP, kill-switch, отдельные тесты); выполняется **после** `DetermineJoinDistributions` и **до**
   `AddExchanges`; `ReorderJoins` в MVP не трогаем.

3. **Оператор = два пайплайна + мост** (bridge-модель hash join): правая сторона — sink-оператор
   (сортировка + публикация отсортированного источника; spill наследуется от внешней сортировки),
   левая — стриминговый merge-оператор; lifecycle через **`JoinBridgeManager`**-паттерн (рефкаунтинг,
   `whenBuildFinishes`, детерминированный destroy). Streaming обеих сторон PrestoDB-стиля — **нет**:
   в Trino-модели драйверов без grouped execution это дедлоки (урок PrestoDB PR #17829).

4. **RIGHT/FULL — отдельный outer-driver** по образцу `LookupOuterOperator` (не MVP); NULL-ключи
   не матчатся никогда; residual filter — переиспользование `JoinFilterFunction`.

5. **Iceberg sortedness — ключевой enabler и первая ценность**: закрыть TODO в
   `IcebergMetadata.getTableProperties` (консервативно: все файлы снапшота с одинаковым `sortOrderId`,
   только identity-трансформы) — это независимо полезно (sort elimination). Bucketed+sorted таблицы
   (bucket execution + `PartitioningHandleReassigner`) → co-located SMJ **без exchange и без хэш-таблиц**.
   При разном числе бакетов co-location ломает порядок — пересортировка.

6. **Partitioned merging exchange нужен** (REPARTITION + `OrderingScheme`, ослабление инварианта
   `ExchangeNode:104-111`; PlanFragmenter/RemoteSourceNode уже переносят orderingScheme — правок
   почти нет; `MergeOperator` уже существует), но **не в MVP**: MVP обходится обычным
   `partitionedExchange` + `SortNode` (внешняя сортировка со спиллом) в join-стадии.

7. **Feature flags**: config + session `prefer_merge_join` / `sorted_merge_join_strategy`
   (OFF | PREFER | FORCE | COST_BASED), default OFF; OFF = байт-в-байт текущий план (три линии
   обороны: rule no-op, пустое Optional → createLookupJoin, операторы не инстанцируются).

8. **Cost model**: перегрузка `calculateSortedMergeJoinCostWithoutOutput` (network как PARTITIONED;
   cpu = scan + n·log(n/runway) для несортированных; **memory = O(макс. группа) vs build резидентно** —
   без честной memory-компоненты CBO никогда не выберет SMJ); unknown-статы ⇒ SMJ не выбирается
   (не ломает существующий fallback).

9. **Гвард от «умных» оптимизаторов**: `AdaptiveReorderPartitionedJoin` skip при `algorithm.isPresent()`;
   `AddLocalExchanges.visitJoin` — singleStream для SMJ-сторон (локальный hash-exchange режет порядок);
   FTE (`RetryPolicy.TASK`) — жёсткий gate в MVP (merging source сегодня требует retryPolicy==NONE).

## 10.2. Расхождения и их разрешение

| Тема | Позиции | Разрешение синтеза |
|---|---|---|
| Где сортировать | java dev: SortNode в плане (V2); trino dev: partial SortNode + partitioned merging exchange; perf: сортировка внутри оператора с flat-ключами (radix) | **Эволюция**: MVP — SortNode после обычного exchange (готово сегодня); фаза 2 — partitioned merging exchange (сортировка на мапперах); фаза 3 — опционально внутренняя radix-сортировка build с flat-ключами |
| Контракт оператора | java dev: WorkProcessorOperator (pull); trino dev: классический Operator | **Operator для build (нужен memory revoke/спилл — для WorkProcessor это TODO в `WorkProcessorOperator.java:21`); merge probe — допустимы оба; MVP проще на классическом Operator по образцу LookupJoinOperator, миграция на WorkProcessor при спилле групп** |
| Dynamic filtering | architect: DF сохраняется (build-sink собирает домены как HashBuilderOperator); trino dev: MVP skip join'ы с DF | **Сохранять DF в bridge-модели** (архитектор прав: DynamicFilterSourceOperator стоит до sink и от реализации join не зависит); консервативный fallback — если DF-интеграция окажется сложной, MVP-skip как временная мера |
| Параллелизм | driverInstanceCount=1 на сторону (MVP) vs per-partition драйверы | **MVP: 1 merge-драйвер на задачу/бакет** (соответствует partitionCount==taskCount); фаза 2: per-partition драйверы (partitionCount = k×taskCount для skew) |
| Pairing драйверов | java dev поднимает как открытый вопрос (bridge[i] по instance-index) | При driverInstanceCount=1 проблема отсутствует (один мост); при multi-driver — реестр мостов по instance-index (прецедент `totalOperatorsCount`) — вынести в дизайн фазы 2 |

## 10.3. Варианты end-to-end архитектуры (сравнение из brainstorm'а)

| Критерий | **V1 «Минимально-инвазивный»** (поле+правило+bridge-операторы+SortNode) | **V2 «Exchange-first»** (partitioned merging exchange, тонкий join) | **V3 «Port PrestoDB»** (streaming + grouped execution) |
|---|---|---|---|
| Скорость до первой ценности | **высокая** (4 PR до рабочего MVP) | средняя | низкая |
| Blast radius | минимальный (1 Optional-поле, 1 правило, 2 оператора, 0 новых план-узлов, 0 SPI) | большой (инварианты ExchangeNode, StreamPropertyDerivations, spooling/FTE) | огромный (понести grouped execution/lifespans — «параллельная вселенная») |
| Эффективность рантайма | хорошая (build материализуется сортированным, probe стримится; спилл есть) | максимальная (сортировка на мапперах, merge без полной материализации) | хорошая, но дедлоки/шедулинг-риски |
| Совместимость/регресс | **нулевой при OFF** | средние риски | высокие |
| Приор art-совместимость | Velox (Match-окна) + Spark (буфер группы) | Spark-планировка + ClickHouse parallel_full_sorting_merge | PrestoDB PR #17829 |

**Рекомендация (единодушная де-факто): V1 → эволюция в V2.** Поле `JoinAlgorithm` делает оба варианта
совместимыми: partitioned merging exchange фазы 2-3 — чисто аддитивная оптимизация под тем же
план-контрактом. V3 отклонён.

## 10.4. Целевая архитектура (после всех фаз)

```mermaid
flowchart TB
    subgraph PLAN["Планировщик"]
        DJA["DetermineJoinAlgorithm<br/>(после DetermineJoinDistributionType)<br/>eligibility: equi-keys + sortedness<br/>(PropertyDerivations / Iceberg)"]
        AE["AddExchanges.visitJoin (MERGE):<br/>partial SortNode + mergingPartitionedExchange<br/>(REPARTITION + orderingScheme)"]
        ALE["AddLocalExchanges: singleStream<br/>+ LOCAL merging exchange"]
    end
    subgraph STAGE_SRC_L["Стадия L (SOURCE)"]
        SL["Scan(Iceberg sorted) → Filter/Project<br/>→ partialSort (пропуск при read-in-order)"]
    end
    subgraph STAGE_SRC_R["Стадия R (SOURCE)"]
        SR["Scan → Filter/Project → partialSort"]
    end
    subgraph STAGE_JOIN["JOIN-стадия (FIXED_HASH, per бакет)"]
        ML["merge(k) левого<br/>(RemoteSourceNode+ordering → MergeOperator)"]
        MR["merge(k) правого"]
        MJ["MergeJoinOperator<br/>(Match-окна, flat-prefix ключ,<br/>словарный вывод, residual filter)"]
        BR["JoinBridgeManager&lt;MergeJoinBridge&gt;<br/>(lifecycle, refcounts)"]
        OD["OuterDriver (RIGHT/FULL)"]
        ML --> MJ
        MR --> BR --> MJ
        MJ --> OD
    end
    SL -->|REPARTITION(hash keys) + merging| STAGE_JOIN
    SR -->|REPARTITION(hash keys) + merging| STAGE_JOIN
    ICE["Iceberg bucketed+sorted<br/>(sort order reported, co-location)"] -.->|zero-shuffle путь<br/>(Б) без exchange| STAGE_JOIN
```

Ключевые характеристики:
- **Память: O(максимальная группа равных ключей)** на merge-драйвер (не build-сторона!); build
  спиллится отсортированными ранами (паттерн OrderByOperator), группы — MatchBuffer с порогами
  (память → spill по образцу Spark ExternalAppendOnlyUnsafeRowArray, re-iterate через block nested loop).
- **CPU**: flat 64-bit order-preserving prefix ключа (реюз `FlatHashStrategy`-энкодингов) — merge =
  sequential scan двух long[]; radix sort входов; galloping для skew; словарная сборка выхода
  (DictionaryBlock поверх входных страниц, ~2-5× на широких payload — урок Velox ~10×).
- **Co-located путь (Б)**: bucketed+sorted Iceberg → SMJ вообще без exchange и хэш-таблиц —
  «самоусиливающийся» ETL-сценарий (пишем сортированным — читаем merge'ом).

## 10.5. Рекомендуемый roadmap

| Фаза | Состав | Критерии готовности | PR-ы |
|---|---|---|---|
| **0. Iceberg sortedness** (enabler) | `getTableProperties` → SortingProperty (консервативно: единый sortOrderId, identity); гарантии порядка сплитов | PropertyDerivations выводит сортировку; потребители sortedness работают (`AddExchanges.visitTopN` PARTIAL→Limit для pre-sorted входа, `PropertyDerivations.visitWindow` pre-sorted окна); опционально новое правило элиминации SortNode над отсортированным сканом (правила `RemoveRedundantSorts` в Trino не существует — risks/05 D1, пишем с нуля); fuzz-тесты корректности | отдельный PR (самостоятельно ценен) |
| **1. MVP (forced SMJ)** | JoinNode.algorithm; DetermineJoinAlgorithm (PREFER/FORCE, INNER/LEFT); операторы bridge-модели (пакет `io.trino.operator.join.merge`); SortNode-обмен; driverInstanceCount=1; гварды (adaptive, FTE, AddLocalExchanges); PlanPrinter; session/config флаги | TestMergeJoinOperator зелёный (все типы, NULL, дубликаты, пустые, RLE/dictionary, oracle-сравнение с hash); FORCE на TPC-H/DS = diffs 0; bounded memory под skew | PR1 план-модель; PR2 оператор; PR3 флаг+правило+гварды; PR4 e2e-тесты |
| **2. Спилл + FULL/RIGHT + multi-driver** | SpillingMatchBuffer (пороги, block nested loop re-iterate); RIGHT/FULL через outer-driver; LOCAL merging exchange под join / per-partition драйверы; pairing мостов | Десятки млрд строк без OOM (синтетика 10-100 млрд); ревокации проходят; product-tests спилла | PR5-7 |
| **3. Partitioned merging exchange + CBO** | Ослабление инварианта ExchangeNode; partial SortNode на мапперах; StreamPropertyDerivations; calculateSortedMergeJoinCostWithoutOutput; режим COST_BASED; телеметрия algorithm | Stage count не растёт vs hash-план; cost-режим не выбирает SMJ там, где hash быстрее (бенчмарк-гейты) | PR8-10 |
| **4. Полная интеграция** | ReorderJoins (SMJ-aware JoinGraph); SemiJoin; FTE (ordered spooling partitions, retryPolicy!=NONE для merge-source); опционально streaming-вариант (упрётся в TODO спилла WorkProcessor) | по факту спроса | отложено |

**Параллельно (perf-трек)**: JMH-микро (компараторы MH/generated/flat-long; 2-way vs PQ; gallop;
dictionary vs PageBuilder; спилл с раздельными ключами); макро TPC-H/DS + синтетика 1/10/100 млрд +
zipf-skew; метрики: cpu-time/row, LLC-miss/row, alloc B/row, re-read factor. Пороги бенчмарков →
константы cost-модели.

## 10.6. Сводный риск-регистр (топ-10, из всех ролей)

| # | Риск | Вероятность/эффект | Митигация |
|---|---|---|---|
| 1 | Коннектор сообщил ложную sortedness → **неверные результаты** | низкая/критический | консервативный критерий (единый sortOrderId, identity, порядок в сплите), PlanSanityChecker, fuzz vs hash-join oracle, kill-switch |
| 2 | OOM на гигантской группе дублей | средняя/высокий | cap group size + SpillingMatchBuffer; в MVP fail-fast с внятной ошибкой; cost-модель учитывает NDV |
| 3 | CBO-регресс при COST_BASED | средняя/средний | default OFF; unknown-статы ⇒ не выбирается; телеметрия + порог min-sorted-input-bytes |
| 4 | Rolling upgrade / сериализация | низкая/средний | аддитивное Optional-поле, 0 новых план-узлов, mixed-version product-test |
| 5 | FTE-несовместимость (merging source требует retryPolicy==NONE) | средняя/средний | жёсткий gate в MVP; ordered spooling в фазе 4 |
| 6 | ReorderJoins разрушает eligible-планы | высокая/низкий | SMJ-выбор пост-reorder (осознанный компромисс); интеграция фаза 4 |
| 7 | Потеря/усложнение dynamic filtering | средняя/средний | bridge-модель сохраняет DF (DynamicFilterSourceOperator до sink); product-test DF+SMJ |
| 8 | Memory-учёт словарных выходов (DictionaryBlock держит source-страницы) | средняя/средний | перенос учёта в downstream-контекст / flatten при ревокации |
| 9 | Деградация на узких выходах/всё-в-кэше данных | высокая/низкий | cost-модель (hash непобедим в LLC); FORCE только для тестов |
| 10 | Skew-партиции cross-node (hash NDV≪N) | средняя/средний | partitionCount=k×taskCount (intra-node), range-партиционирование (фаза 4, Iceberg min/max), salting |

## 10.7. Открытые вопросы (консолидировано, к проработке на фазе дизайна)

1. **Pairing драйверов** при multi-driver merge: надёжная раздача bridge[i] (instance-index в
   OperatorFactory/DriverContext; прецедент `totalOperatorsCount`).
2. **Частично упорядоченные входы** (σ∈(0,1)): файлы отсортированы внутри, но не между собой —
   учитывать ли в cost (Iceberg знает sort order per file)?
3. **Канонический OrderingScheme**: требовать совпадения ASC/DESC обеих сторон или вставлять
   выравнивающий Sort; NULLS FIRST/LAST семантика vs NULL-не-матчится.
4. **Sortedness при row-level deletes** (порядок выживших строк сохраняется, но цена применения
   deletes до merge) — сообщать ли.
5. **Range-партиционирование** (ClickHouse `query_plan_join_shard_by_pk_ranges`): семплировать или
   верить Iceberg min/max — лечение cross-node skew.
6. **Runtime hash→SMJ fallback**: только через FTE AdaptivePlanner — стоит ли просить.
7. **Semi/anti join на SMJ**: SemiJoinNode имеет свой DistributionType — отдельная точка интеграции.
8. **Dynamic range filter из SMJ** (аналог CH `max_rows_in_set_to_optimize_join`): min/max правой
   стороны как ранний фильтр левой при streaming-варианте.
9. **Стоимость revocable vs user memory** в CostComparator (сейчас одна ось memory).
10. **Спилл гигантских групп с обеих сторон**: рекурсивный block nested loop или диагностика-fail.

## 10.8. Метрики успеха проекта

- **Функциональные**: все join-типы (INNER/LEFT на фазе 1, RIGHT/FULL на фазе 2) дают результаты,
  идентичные hash join (oracle-тесты); выключенная фича меняет 0 планов (plan-compare CI).
- **Масштаб**: запрос с join'ом 2×10 млрд строк на кластере N воркеров завершается без OOM с
  bounded peak memory/воркер (целевая граница: max(размер макс. группы, окно merge)); hash join на
  том же запросе либо падает, либо k× медленнее из-за spill-проходов.
- **Производительность**: на pre-sorted входах (Iceberg sorted_by) SMJ ≥ hash по latency при ≤ памяти;
  cpu-time/row на >LLC данных не хуже hash при несортированных входах до порога σ.
- **Стабильность ETL**: TPC-DS/синтетика со skew (zipf) без OOM-фейлов; ревокации памяти корректны.

## 10.9. Следующие шаги

1. Ревью этой документации → фиксация решений раздела 10.1-10.2 (особенно: план-модель `JoinAlgorithm`,
   bridge-модель оператора, фазировка).
2. Детальный тех-дизайн оператора (state machine из [05, §4] + MatchBuffer из [05, §6] + flat-key
   из [07, §1]) — отдельный документ.
3. Старт фазы 0 (Iceberg sortedness PR) и фазы 1 (PR1 план-модель) — они независимы и параллелизуются.
4. Настройка perf-треков (JMH-микро из [07, §6]) до начала фазы 2 — данные нужны для cost-констант.

## 10.10. Дополнение: несортированные входы и масштаб 15B×17B (документы 11-13)

Дополнительное исследование кейса «данные не отсортированы, join 15 млрд × 17 млрд строк»
уточняет и усиливает roadmap (полностью — [11](11-unsorted-inputs-scenarios.md),
[12](12-sorting-algorithms-landscape.md), [13](13-case-study-15b-join-17b.md)):

1. **Сортировка — центральный объект проектирования**: во всех сценариях несортированных входов
   SMJ = (0/1/2) exchange + per-бакет внешняя сортировка + merge. Co-location по bucket-функции
   (в т.ч. смешанные спеки `identity+bucket`) убирает exchange, но не сортировку.
2. **Существующей внешней сортировки Trino достаточно для MVP** (quicksort-раны + k-way merge,
   1 проход при раны ≤ fan-in), но в кодовой базе **отсутствуют**: radix sort, parallel sort,
   replacement selection, sample sort, order-preserving кодирование ключей, каскадный merge
   (есть только в Hive `SortingFileWriter`). План усиления R1-R6 — в [12, §12.6].
3. **Ключевая оптимизация — sort-join fusion**: раны k-way мержатся прямо в merge-join без
   материализации отсортированного вывода → ровно 1×write + 1×read каждой стороны.
4. **Параллелизм join-стадии — главное ограничение масштаба**: default 100 hash-партиций
   (2 задачи/воркер на кластере 50 нод) при одном merge-драйвере на задачу — поднимать
   `query_max_hash_partition_count` до 200-400 и/или множить merge-драйверы (фаза 2).
5. **Cost-порог SMJ vs spilling hash**: при полностью несортированных входах SMJ включать когда
   build заведомо не лезет в память (build > ~1.5-2× доступной) или входы уже отсортированы;
   при сортировке «в холостую» на кэш-помещающихся данных hash остаётся лучше (подтвердить синтетикой).
6. Обязательные конфиг-изменения для ETL такого масштаба: spill включён + NVMe-пути + LZ4,
   `max-spill-per-node` ≥ 1.5 ТБ, `query.max-memory` кластерно ≥ 2-3 ТБ (default 20 ГБ!),
   компрессия exchange.
