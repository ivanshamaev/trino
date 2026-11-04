# План исследования: Trino Engine + Iceberg

Цель: глубокое исследование внутреннего устройства Trino и его интеграции с Iceberg
с документированием в `ai-docs/`. Статус: **выполнено**.

## Этап 2: Sorted Merge Join (подготовительные исследования)

- [x] S1. JOIN-интерфейсы ядра детально (контракты, lifecycle, память) → `smj-research/01-trino-join-internals.md`
- [x] S2. Существующие механизмы сортировки и слияния (строительные блоки SMJ) → `smj-research/02-trino-sort-merge-machinery.md`
- [x] S3. Карта точек интеграции SMJ (планер, sortedness, Iceberg, cost) → `smj-research/03-smj-integration-points.md`
- [x] S4. SMJ в других движках (Spark, ClickHouse, DuckDB, Velox, PrestoDB, Pinot) → `smj-research/04-smj-in-other-engines.md`
- [x] S5. Brainstorm 5 ролей (java dev / java architect / perf / MPP architect / trino dev) → `smj-research/05..09-brainstorm-*.md`
- [x] S6. Синтез: консенсус, варианты архитектуры, roadmap, риски → `smj-research/10-synthesis-and-recommendations.md`

## Этап 3: SMJ на несортированных входах, кейс 15 млрд × 17 млрд строк

- [x] S7. Матрица сценариев «бакетирование/партиционирование vs join-ключ»: какой exchange построит Trino (S1-S8), bucket execution для смешанных спек, число hash-партиций → `smj-research/11-unsorted-inputs-scenarios.md`
- [x] S8. Ландшафт алгоритмов сортировки (external merge sort, radix, replacement selection, PSRS, grace hash) + верифицированная инвентаризация наличия в Trino (radix/parallel/timsort/replacement selection — отсутствуют) → `smj-research/12-sorting-algorithms-landscape.md`
- [x] S9. Кейс-стади join 15B×17B: план по фазам, память/I/O/CPU-математика, sort-join fusion, параллелизм join-стадии, SMJ vs spilling hash join, чек-лист внедрения → `smj-research/13-case-study-15b-join-17b.md`

## Этап 4: риски SMJ и сценарии применимости

- [x] S10. Риски корректности (верифицировано по коду: NaN-дивергенция compare/equal для DOUBLE/REAL, не-orderable MAP, NULL-порядки, per-file sortedness Iceberg, префиксная сортированность, cast'ы) с решениями → `smj-research/risks/01-correctness-risks.md`
- [x] S11. Риски интеграции с движком (FTE/adaptive, dynamic filtering, memory accounting словарных выходов, lifecycle спилл-ранов, сериализация, наблюдаемость) → `smj-research/risks/02-engine-integration-risks.md`
- [x] S12. Риски производительности (параллелизм join-стадии, k-way merge, копирование вывода, re-iterate групп, конфиг-дефолты) → `smj-research/risks/03-performance-risks.md`
- [x] S13. План реализации с риск-гейтами: фазы 0-4 с acceptance-критериями из рисков, маппинг «риск → тест → фаза», критерии отката → `smj-research/risks/04-implementation-plan-with-risk-gates.md`
- [x] S14. Сценарии применимости: иерархия S1-GOLD (bucket+sorted same N) .. S5, сигналы выбора при малой памяти, SMJ vs grace hash, рецепты подготовки данных → `smj-research/14-best-fit-scenarios.md`

## Этап 5: повторная верификация (аудит собственных документов)

- [x] V1. Повторный проход по репозиторию: верификация 18 фактов risks-документов (NaN-дивергенция, MAP/comparable, LocalProperties.match, per-file sort, TODO async-spill, одноразовые раны, сигнатура JoinFilterFunctionFactory, partition count defaults, точка вставки правила PlanOptimizers:886 и др.) → `smj-research/risks/05-verification-report.md`
- [x] V2. Найдены и исправлены расхождения: `RemoveRedundantSorts` не существует (фаза 0 acceptance переписана под реальные потребители: TopN-partial→Limit, Window pre-sorted + опциональное новое правило элиминации); streaming-агрегации в Trino нет (Рецепт D исправлен: выгода для GROUP BY — future, для Window/sorted-записи — реальна); JoinOperatorType — класс, не enum
- [x] V3. Новые риски добавлены в регистр и план: I13 (45 мест `new JoinNode(` — канонический конструктор), P11 (singleStream сериализует probe-pipeline → LOCAL merging exchange в MVP), C8 (file-level sortOrderId и префиксная отчётность), S-N1 (co-location только same-catalog); speculative execution проверен — риска нет (FTE-internal)
- [x] V4. Третий проход (риски/05 §5.6): верификация плана 04 по всему `ai-docs/` (включая `smj-research-openai/`) и коду; внешние референсы #12618 и SPARK-30536 подтверждены первоисточниками; исправлены D4-D9 (C1-C9→C1-C8; I5→I8 адреса фильтра; маппинг §4.6 дополнен I7/I8/I10/I11/I12/P8/P9/P10/S-N1; регистр README дополнен I7/I11/P8/P9/P10 + исправлены секции P5/P7; остаточные RemoveRedundantSorts в 10-synthesis/06; enum-формулировка и опечатка в risks/02/03); якоря добавлены (outputSingleMatch=maySkipOutputDuplicates:2911, calculateJoinCostWithoutOutput в CostCalculatorWithEstimatedExchanges, mergingExchange= GATHER+FIXED_PASSTHROUGH)
- [x] V5. Четвёртый проход (риски/05 §5.7): автопроверка ссылок всего `ai-docs/` (0 битых; 43/43 секционных); повторно подтверждены ExchangeNode:104-111 (4 checkArgument), AddExchanges.visitJoin:945, `// todo support sorting properties` в IcebergMetadata getTableProperties + applyPartitioning (mixed-спека, power-of-2 TODO), 45 мест JoinNode; **новый риск I14** (`UseNonPartitionedJoinLookupSource` после AddLocalExchanges — PlanOptimizers:1004/1012) добавлен в risks/02 §2.13 + регистр + план (гварт фазы 1); регистр = 34 риска; кросс-валидация NaN/radix/ADR с smj-research-openai — противоречий нет
- [x] V6. Пятый проход — readiness-аудит (риски/05 §5.8 + план §4.9 «Definition of Ready»): подтверждены visitJoin:2539/createLookupJoin:2879 (единая точка выбора), WorkProcessor-модель оператора (spilling.LookupJoinOperatorFactory implements JoinOperatorFactory+WorkProcessorOperatorFactory:50; WorkProcessorOperator.java:21 `// TODO: support spill`), нейминговый прецедент prefer_partial_aggregation:131, SQL-дефолт NULLS_LAST (OrderingTranslator:25-29); закрыты R1-R4: план теперь ссылается на опорные дизайн-доки (03 §3.10/§3.9, 05 §5.4/§5.6/§5.1, 09, 12), нейминг унифицирован (`join.merge`/`MergeJoin*`; 05 аннотирован), канон OrderingScheme = ASC NULLS_LAST закреплён, I14-правило добавлено в карту 03 §3.10. **Вердикт: блокеров старта MVP нет**
- [x] V7. Шестой проход — реализуемость механик (риски/05 §5.9): подтверждён конструкторный паттерн JoinNode (@JsonCreator публичный :78 → приватный полный :105; все 45 мест через публичный), прецедент внутреннего enum (DistributionType :52), outer-operator lifecycle в JoinBridgeManager (`getOuterPositionsFuture` — хук RIGHT/FULL фазы 2), `LocalCostEstimate.of(cpu, maxMemory, network)`, Iceberg-структуры фазы 0 (getSupportedSortFields:1740, TrinoSortField, table.sortOrder()); исправлены U1 («канонический конструктор» → «@JsonCreator без дефолт-перегрузок») и U2 (config-дом FeaturesConfig → OptimizerConfig:76/517). **Готовность к старту планирования MVP подтверждена повторно**
- [x] V8. Седьмой проход — точность тест-якорей (риски/05 §5.10): уточнены SMALL_MEMORY_POOL_BYTES (TestHashJoinOperator:127), flipChildren (:170), прецедент golden-JSON `TestPatternRecognitionNodeSerialization`, пути TestDetermineJoinDistributionType/QueryAssertions/BaseCostBasedPlanTest точны, mixed-version infra `testing/trino-product-tests` существует, `query.max-memory=20GB` подтверждён (MemoryManagerConfig:38); автопроверка 57 md: регистр 34 = §4.6 (32 строки, P5/P6 объединены) = «Закрывает» (0 дыр), битых ссылок 0. **Новых рисков и расхождений по существу нет — план готов к старту планирования MVP**
- [x] V9. Восьмой проход — adversarial (риски/05 §5.11): **найден скрытый пробел — `distributed_sort`** (при `false` visitSort сам загоняет вход в singleStream → возврат P11; default true, OptimizerConfig:64) — закрыт в eligibility плана §4.2/§4.9 + регистр P11; полностью размечена merge-механика: REMOTE — `createMergeSource:909` (MergeOperatorFactory, требует retryPolicy==NONE + driverInstanceCount=1 — дословно I1-гейт), LOCAL — `createLocalMerge` → `LocalMergeSourceOperatorFactory` (подпакет `operator/exchange/`, :38 — имена в доках 02/06/08/12 верны), OrderByOperator — `MergeSortedPages:42` (claim 12 подтверждён); DF — обёртка `createDynamicFilterSourceOperatorFactory:2959` над build-sink (якорь в план §4.2 п.2); PlanFragmenter:545 + CostCalculatorUsingExchanges.visitJoin:180 (03 §3.10) ✓. **Регистр 34 без изменений; готовность подтверждена**
- [x] V10. Девятый проход + политика выбора (риски/05 §5.12): целостность зелёная (0 битых ссылок, 58 md, регистр=§4.6=«Закрывает»); создан **[15-smj-selection-policy.md](smj-research/15-smj-selection-policy.md)** — жёсткие рамки выбора SMJ: 7 MUST-гейтов, W1–W4 (S1-GOLD безусловен; Bp>α·U memory-pressure; big-big γ·U; скидка за sorted-сторону), запреты N1–N5 (не менять RAM на CPU где RAM хватает; LLC; broadcast; корректность), псевдокод DetermineJoinAlgorithm, пороги α=0.7/β=0.3/γ=1.0/δ=10% (OptimizerConfig, фаза 3), разбор кейса «одинаковый бакет + join-ключ» (sorted/не-sorted/join по (k,dt)/разные N); встроено в план §4.2/§4.4/§4.9 и индекс
- [x] V11. Десятый проход — NULL/FULL-OUTER/DF (риски/05 §5.13): три новых исследования — **risks/06 (C9)**: NULL хэшируется константой (BlockTypeOperators:101-107 → весь NULL-хвост в одном бакете уже в hash/bucketing), fast-path компаратора (INNER O(1)-skip, LEFT стриминг) + `k IS NOT NULL` для INNER (авто-добавления в Trino нет — проверено; прунинг Iceberg-файлов по null_count); **risks/07**: FULL OUTER — null-padding стримится O(1), zero-retention (пик == INNER), без OuterPositionIterator (SMJ проще hash-FULL); **risks/08**: DF — сбор на build над sink, INNER-only, split-pruning уже есть (`getNextBatch(DynamicFilterSnapshot)`, IcebergSplitSource:268+); курсорный range-skip = P12 (фаза 4, только S1-GOLD); **15 §15.11** — матрица 7 сценариев 15B×17B (hash неприменим: B/P=17ГБ > 0.7·U=7.7ГБ); план 04 обновлён (§4.2/4.3/4.5/4.6/4.9); регистр 34→36 (C9, P12)

## Этап 7: план разработки (smj-plan-dev)

- [x] P1. Единый план реализации — `smj-plan-dev/README.md`: зафиксированные архитектурные решения A1-A10 (поле JoinNode.algorithm/@JsonCreator-механика; bridge build-push/probe-WorkProcessor; канон ASC NULLS_LAST; нейминг join.merge; правило после DetermineJoinDistributionType:886; политика 15 MUST/W/N; guard-набор; NaN/NULL-guards; MVP-forced), карта этапов 0-7 с зависимостями и рисками, сквозные инварианты (OFF=0 диффов, oracle, airstyle/commit-конвенции AGENTS.md), опорные точки интеграции (верифицированные якоря)
- [x] P2. Страницы этапов `smj-plan-dev/stages/` (8 файлов): stage-0 Iceberg sortedness; stage-1 план-модель; stage-2 оператор; stage-3 правило+гварды; stage-4 обмены+e2e (MVP); stage-5 спилл+RIGHT/FULL+multi-driver; stage-6 partitioned merging+CBO; stage-7 полная интеграция — каждый: цель, зависимости, состав «файл→изменение→якорь», тесты/acceptance из рисков, откат, **секция «Согласованность» с кросс-проверкой против корпуса**
- [x] P3. Кросс-проверка против всех документов: скрипт согласованности — 0 битых ссылок (70 md), все «NN §N.N»-ссылки резолвятся (1 ошибка «05 §7.5»→«07 §7.5» исправлена), все ID рисков существуют в регистре (36), якоря кода сверены с отчётом верификации §5.1-5.13; ai-docs/README дополнен подразделом smj-plan-dev
- [x] P4. Аудит smj-plan-dev (smj-plan-dev/AUDIT.md, 2026-11-04): реализуемость каждого пункта по коду (DDT-паттерн `isEmpty()` — same-batch безопасен; `WorkProcessorOperatorAdapter.Factory implements JoinOperatorFactory`:46-47; JoinBridge = 3 метода (outer-заглушка в MVP); **1 сплит = 1 целый файл** (toIcebergSplit:911+ — stage-0 формулировка исправлена); PlanFragmenter переносит ordering безусловно — stage-6 «проверить»→«подтверждено»; PropertyDerivations.visitJoin:554; SortFieldInfo:1766); соответствие корпусу (0 битых ссылок, ID/секции ок); формулировки адекватны (acceptance измеримы); **внутренних противоречий нет** (ключевые связки A2/I13, A4/матчинг, DF INNER-only, NULL-fast-path согласованы); 3 уточнения + 2 усиления внесены в stage-файлы

## Этапы

- [x] 1. Ориентация: структура репозитория, модули, версия Iceberg (1.12.0)
- [x] 2. Архитектура Trino: coordinator/worker, discovery, session, транзакции → `01-trino-architecture.md`
- [x] 3. Жизненный цикл запроса: parser → analyzer → planner → optimizer → fragmenter → scheduler → execution → `02-query-lifecycle.md`
- [x] 4. Выполнение JOIN: алгоритмы, распределённое выполнение, dynamic filtering, spilling → `04-joins.md`
- [x] 5. Обработка данных: Pages, Blocks, types, operators, exchange, memory management → `03-execution-runtime.md`
- [x] 6. Connector SPI: как коннекторы подключаются к движку → `05-connector-spi.md`
- [x] 7. Iceberg connector: модули, каталоги (HMS, Nessie, REST, JDBC, Glue, Snowflake), конфигурация → `06-iceberg-connector-overview.md`
- [x] 8. Метаданные Iceberg: таблицы, схема, партиции, снапшоты, транзакции, commit protocol → `07-iceberg-metadata-transactions.md`
- [x] 9. Чтение: split planning, page sources, ORC/Parquet/Avro, delete-файлы, deletion vectors, кэширование → `08-iceberg-read-path.md`
- [x] 10. Запись: page sink, file writers, commit, row-level operations (UPDATE/DELETE/MERGE), materialized views, OPTIMIZE → `09-iceberg-write-path.md`
- [x] 11. Статистики и оптимизация: partition pruning, predicate pushdown, dynamic filtering, table statistics, bucket execution → `10-iceberg-statistics-optimization.md`
- [x] 12. Обслуживание: procedures, ALTER TABLE, system tables → `11-iceberg-maintenance.md`
- [x] 13. Хранилище: TrinoFileSystem, S3/HDFS, object store layout, кэши, FileIO → `12-storage-filesystem.md`
- [x] 14. Финальная сборка: README-индекс, перекрёстные ссылки → `README.md`

## Метод

Исследование проведено параллельными агентами по 8 областям (архитектура сервера, компиляция SQL,
JOIN, рантайм, структура коннектора/каталоги, чтение, запись, статистики/обслуживание/ФС) с
последующей перекрёстной проверкой фактов по исходникам и сверкой дублирующихся выводов
(например, enforced/unenforced-предикаты и commit-протокол подтверждены независимо из трёх областей).

## Результаты

Итоговые документы — см. `README.md` (индекс). Документация основана исключительно на коде
этого форка; упомянутые классы и методы существуют в репозитории.
