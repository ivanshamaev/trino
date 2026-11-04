# 05. Отчёт верификации рисков и плана (второй проход по репозиторию)

Дата: второй аудит `ai-docs/smj-research/risks/01..04` + `14-best-fit-scenarios.md`.
Каждое утверждение документов проверено повторно по коду; найденные расхождения исправлены
(правки внесены в соответствующие документы); обнаружены новые риски (I13, P11, C8, S-N1).
Третий проход — верификация плана 04 по **всему** `ai-docs/` и коду — в §5.6.
Четвёртый проход (§5.7) — целостность ссылок, повтор ключевых инвариантов, новый риск I14.

## 5.1. Матрица верификации: подтверждённые факты

| # | Утверждение (документ) | Проверка в коде | Статус |
|---|---|---|---|
| 1 | C1: `compare(NaN,NaN)=0`, `equal(NaN,NaN)=false` для DOUBLE | `DoubleType.java:192-273`: equal = `left == right`; compare = `if (left==right) return 0; return Double.compare(...)` → `Double.compare(NaN,NaN)=0`; `COMPARISON_UNORDERED_FIRST` (237-252) тоже возвращает 0 для NaN==NaN | ✅ подтверждено |
| 2 | C1: та же дивергенция для REAL | `RealType.java`: equal = `intBitsToFloat(l) == intBitsToFloat(r)`; compare = тот же guard + `Float.compare` (строки 223-231) | ✅ |
| 3 | C1: `-0.0/+0.0` консистентны | `if (left == right) return 0` (комментарий «Double.compare considers 0.0 and -0.0 different») + нормализация в hash (`if (value == 0) value = 0`) | ✅ |
| 4 | C2: MAP hash-join'ится, но не orderable | `MapType.isComparable()` переопределён (282-284, от value-типа) → analyzer `StatementAnalyzer:3298` (`commonSuperType.isComparable()`) разрешает equi-join; `isOrderable` НЕ переопределён → `AbstractType:68` false | ✅ подтверждено |
| 5 | C3: SortingProperty несёт SortOrder (ASC/DESC × NULLS FIRST/LAST) | `io.trino.spi.connector.SortOrder`: `ASC_NULLS_LAST(true,false)`, `DESC_NULLS_FIRST(false,true)` и др. | ✅ |
| 6 | C4: префиксная семантика матчинга сортировок | `LocalProperties.match` (95-112) — позиционный алгоритм; `SortingProperty.isSimplifiedBy` (SortingProperty.java:80-84) требует **equality**; desired (a) удовлетворяется actual (a,b) позиционно; desired (a,b) НЕ удовлетворяется actual (a) | ✅ оба направления верны |
| 7 | C5: Iceberg sorted = per-file | `IcebergPageSink.java:536`: каждый data-writer оборачивается в `IcebergSortingFileWriter` → сортировка внутри каждого writer'а = каждого файла | ✅ |
| 8 | C6: property-трансляция только через identity | `PropertyDerivations.visitProject:783-787` — `computeIdentityTranslations` | ✅ |
| 9 | I1: merging-источник требует `retryPolicy==NONE` | `LocalExecutionPlanner.createMergeSource` (899-939) | ✅ |
| 10 | I1: AdaptiveReorderPartitionedJoin — только FTE | импортирует `getFaultTolerantExecutionAdaptiveJoinReordering*` (44-45) | ✅ |
| 11 | I2: PhasedExecutionSchedule special-case только REPLICATED | `PhasedExecutionSchedule.java:385-393` (`== REPLICATED` в processJoin) | ✅ |
| 12 | I2: SchedulingOrderVisitor собирает только сканы | `SchedulingOrderVisitor.java:28-64` — только `visitTableScan`/`visitTableFunctionProcessor` | ✅ |
| 13 | I4: синхронный tail-spill с TODO | `OrderByOperator.java:226` — «TODO: this should be asynchronous» | ✅ |
| 14 | I5: spill-раны одноразовые | `FileSingleStreamSpiller.java:145, 189` — `writable.getAndSet(false)`, «Repeated reads are disallowed» (**два** сайта: `getSpilledPages` и `getAllSpilledPages`) | ✅ |
| 15 | I8: сигнатура JoinFilterFunctionFactory | `JoinFilterFunctionCompiler.java:257-259`: `create(ConnectorSession, LongArrayList addresses, List<Page> pages)` | ✅ дословно |
| 16 | P1: partition count defaults | `QueryManagerConfig:79-81`: max=100, min=4, minForWrite=50; `OptimizerConfig:101-102`: 5 GB / 10M строк на задачу; формула `DeterminePartitionCount:126-208` | ✅ |
| 17 | План §4.2: точка вставки правила | `PlanOptimizers.java:886` — `DetermineJoinDistributionType ... // Must run before AddExchanges` в одном IterativeOptimizer с `DetermineSemiJoinDistributionType` (889) | ✅ |
| 18 | План: `AddLocalExchanges.visitJoin` — singleStream/exactlyPartitionedOn для build | `AddLocalExchanges.java:~900-931`: `buildPreference = exactlyPartitionedOn(buildHashSymbols)` / `singleStream()` | ✅ |

Дополнительно проверено и **не является риском**:
- **Speculative execution**: существует только как внутренняя эвристика FTE-планировщика
  (`QueryManagerConfig:1189` «speculative stage scheduling»; `speculative`-параметры в FTE-сchedулерах)
  — в pipelined-режиме пользовательских спекулятивных задач нет; SMJ и так FTE-gated →
  самостоятельного риска нет (дублирования задач/спилла не возникает).
- `DetermineTableScanNodePartitioning` существует (`iterative/rule/DetermineTableScanNodePartitioning.java`) — ссылки плана корректны.

## 5.2. Найденные расхождения (исправлены в документах)

### D1 ❌→✅ `RemoveRedundantSorts` не существует

План (фаза 0, acceptance) ссылался на «RemoveRedundantSorts убирает лишние SortNode» — **в этом
репозитории такого оптимизатора нет** (0 совпадений grep). Реальные, проверенные механизмы-потребители
sortedness, которые заработают от фазы 0:
1. `AddExchanges.visitTopN` PARTIAL — замена partial TopN на Limit для pre-sorted input
   (через `LocalProperties.match`, строки ~577-590);
2. `PropertyDerivations.visitWindow` (~261-296) — pre-partitioned/pre-sorted окна не пересортируются
   (`preSortedChannelPrefix`);
3. само правило `DetermineJoinAlgorithm` (главный потребитель).

Плюс в фазу 0 добавлена опциональная deliverable: **новое простое правило элиминации SortNode над
сканом с удовлетворяющей SortingProperty** (в духе несуществующего RemoveRedundantSorts — написать
с нуля, ~50 строк на pattern `SortNode(Scan)` + `LocalProperties.match`). Исправлено в
[04, §4.1](04-implementation-plan-with-risk-gates.md).

### D2 ❌→✅ streaming-агрегации в Trino нет

Документ [14, Рецепт D](../14-best-fit-scenarios.md) утверждал «FlatGroupByHash имеет fast-path
для отсортированных входов (pre-grouped)» — **неверно**: grep по `operator/aggregation` и
`HashAggregationOperator` не находит streaming/sorted-input агрегации; `preGroupedChannels`
существует только в `WindowOperator` (это PrestoDB имел `streaming_aggregation_enabled` /
order-based execution, не Trino). Исправлено: выгода для GROUP BY — **потенциальная будущая
оптимизация**; реальная выгода сегодня — Window (pre-sorted) и sorted-запись downstream.

### D3 ⚠️ I10: JoinOperatorType — класс, а не enum

`JoinOperatorType` (`io.trino.operator.JoinOperatorType:24`) — класс со static-фабриками
(`ofJoinNodeType`, `innerJoin(...)`), не enum. Формулировка «добавить значение enum» в I10/плане
заменена на «добавить фабрику `mergeJoin(...)` + operatorType-строку для WebUI».

## 5.3. Новые риски (добавлены в документы)

### I13 (интеграция): неполный перенос нового поля `JoinNode.algorithm` по местам пересборки

**Факт**: `grep -rn "new JoinNode(" core/trino-main/src/main/` — **45 мест** пересборки JoinNode
(`UnaliasSymbolReferences`, `PredicatePushDown`, `PruneJoinColumns`, `PushProjectionThroughJoin`,
`RemoveUnsupportedDynamicFilters`, `PlanCopier`, `ReorderJoins`, `JoinNode.flipChildren/with*`, ...).
Если добавить поле через перегруженный конструктор с дефолтом — компилятор **молча** примет все 45
мест, и любое забытое место будет тихо ронять маркер → план деградирует до hash (safe, но silent miss)
или, хуже, маркер без перерасчёта условий после трансформаций.

**Митигация** (внесена в план §4.2, PR1): поле добавляется **только в канонический конструктор**
(все 45 мест обязаны явно пробросить или задать empty → компилятор ловит каждое); golden-тест
«план с algorithm=MERGE проходит UnaliasSymbolReferences/PredicatePushDown/flipChildren без потери
поля» (unit на каждое трансформирующее правило); plan-compare CI.

### P11 (производительность): singleStream для SMJ-сторон сериализует весь probe-pipeline

**Факт**: `AddLocalExchanges.visitJoin` управляет параллелизмом **всего пайплайна над join**.
Hash join: probe = `defaultParallelism` (task.concurrency до 32), build = `exactlyPartitionedOn`.
Для SMJ обе стороны обязаны быть single-stream → **весь пайплайн от скана до join = 1 драйвер на
задачу** — в co-located сценарии (S1/S2) это превращает «золотой» случай в однопоточный скан!

**Митигация** (перенесена в MVP, фаза 1): под каждой SMJ-стороной строить **частичную сортировку
по сплитам + LOCAL merging exchange** — готовый паттерн `AddLocalExchanges.visitSort`
(N parallel sorters → `mergingExchange(LOCAL, PASSTHROUGH)` → `LocalMergeSourceOperator`):
скан остаётся параллельным, single-stream начинается только от merge-источника. Это же закрывает
часть P1 (сортировка распараллелена внутри задачи).

### C8 (корректность/Iceberg): file-level `sortOrderId` vs table-level и префиксная отчётность

Уточнение к C5/фазе 0: сортированность файла определяется **sortOrderId файла в манифесте**, а не
табличным `table.sortOrder()` (внешние писатели Spark/Flink пишут по объявленному порядку; Trino
при неподдерживаемых трансформах пишет unsorted с `sortOrderId=0`). Правила фазы 0 уточнены:
- сообщать SortingProperty только если **все data-файлы снапшота** (после прунинга) имеют
  sortOrderId == текущему табличному **и** все поля этого порядка — identity (поддерживаемые);
- **префиксная валидность**: если порядок `(a, truncate(b))`, а join по `a` — файлы отсортированы
  по `(a, ...)` ⇒ свойство `(a)` валидно (сортировка по большему множеству имплицирует префикс);
  сообщать можно валидный identity-префикс, не только полный порядок.

### S-N1 (сценарии): co-location только в пределах одного каталога

**Факт**: `MetadataManager.getCommonPartitioning` (492-508) требует `leftCatalogHandle.equals(rightCatalogHandle)`.
Две одинаково бакетированные Iceberg-таблицы в **разных каталогах** никогда не ко-локуются →
S1-GOLD деградирует до full shuffle. Добавлено в условия [11, §11.3](../11-unsorted-inputs-scenarios.md)
и [14, §14.1](../14-best-fit-scenarios.md).

## 5.4. Верификация плана (`04-implementation-plan-with-risk-gates.md`)

| Пункт плана | Проверка | Результат |
|---|---|---|
| Фаза 0 acceptance: «RemoveRedundantSorts убирает SortNode» | правило не существует | ❌ **исправлено** → реальные потребители (TopN-partial→Limit, Window pre-sorted) + опционально новое правило элиминации |
| Фаза 1: eligibility-список | все пункты сверены с кодом (isOrderable, DOUBLE/REAL, DF, retryPolicy, PARTITIONED) | ✅ |
| Фаза 1: точка вставки правила «тот же IterativeOptimizer, до AddExchanges» | `PlanOptimizers:886-889` | ✅ подтверждено |
| Фаза 1 состав: PR-нарезка | совместима с нарезкой из 09/§9.7; дополнена: LOCAL merging exchange под SMJ-сторонами (P11) и канонический конструктор (I13) | ✅ (с правками) |
| Фаза 2: SpillingMatchBuffer против одноразовых ранов | `FileSingleStreamSpiller:145/189` — оба метода чтения одноразовые | ✅ совместимо |
| Фаза 3: stage-count == hash-плану | SortNode внутри join-стадии; merging REPARTITION не добавляет фрагментов (PlanFragmenter.visitExchange переносит ordering безусловно) | ✅ обоснование корректно |
| Фаза 4: FTE ordered spooling | спекулятивные механизмы — тоже FTE-internal, конфликтов с гейтом нет | ✅ |
| Маппинг §4.6 | все ID существуют; добавлены I13, P11, C8, S-N1 | ✅ (дополнено) |

## 5.5. Итог верификации

- 18/18 перепроверенных фактических утверждений о коде — **подтверждены** (включая критический NaN).
- 2 расхождения найдены и исправлены (несуществующий `RemoveRedundantSorts`; отсутствие
  streaming-агрегации) — оба меняли acceptance-критерии/рекламу выгод, не архитектуру.
- 4 новых риска добавлены (I13 «45 мест пересборки», P11 «сериализация probe-pipeline»,
  C8 «file-level sortOrderId», S-N1 «same-catalog»); два из них (P11, I13) — существенные для MVP,
  их митигации включены в план фазы 1.
- Архитектурные решения синтеза (10) верификацией **не опровергнуты** — усилены.

## 5.6. Третий проход: верификация плана 04 по всему `ai-docs/` (2026-11-04)

Метод: план [04](04-implementation-plan-with-risk-gates.md) сверен с каждым документом `ai-docs/`
(базовые 01–12, smj-research 01–14, risks 01–05, **включая появившуюся после второго прохода
директорию `smj-research-openai/`** — независимое параллельное исследование), каждая кодовая
ссылка плана — с репозиторием, две внешние ссылки — с первоисточниками.

### 5.6.1. Подтверждено по коду (новые якоря, не проверявшиеся ранее)

| Утверждение плана | Проверка | Статус |
|---|---|---|
| Фаза 0 (а): `visitTopN` PARTIAL→Limit для pre-sorted входа | `AddExchanges.visitTopN` PARTIAL-ветка: комментарий «If source is pre-sorted, partial topN can be replaced with partial limit N», `LocalProperties.match(...)` → `LimitNode` c preSortedInputs | ✅ дословно |
| Фаза 0 (б): `visitWindow` pre-sorted окна | `PropertyDerivations.visitWindow`: при полностью pre-partitioned + pre-sorted входе свойства пробрасываются без изменений (`getPreSortedOrderPrefix`) | ✅ |
| Фаза 1 (P11): паттерн `AddLocalExchanges.visitSort` | `visitSort` = `fixedParallelism()` + `mergingExchange(id, LOCAL, node, orderingScheme)`; `ExchangeNode.mergingExchange` (ExchangeNode.java:220-231) = `Type.GATHER` + `FIXED_PASSTHROUGH_DISTRIBUTION` + orderingScheme | ✅ |
| Фаза 1: `outputSingleMatch` достижим для INNER/LEFT | `LocalExecutionPlanner:2911`: `outputSingleMatch = node.isMaySkipOutputDuplicates() && <правые выходы ⊆ правых equi-символов>`; флаг ставит правило `OptimizeDuplicateInsensitiveJoins` (см. ai-docs/04-joins.md) | ✅ якорь добавлен в acceptance |
| Фаза 1: пакет оператора `io.trino.operator.join` | существует; внутри — `spilling/` и `nonspilling/` (см. #12618 ниже), `JoinBridgeManager`, `JoinFilterFunction`, `JoinStatisticsCounter` | ✅ |
| Фаза 3: `calculateSortedMergeJoinCostWithoutOutput` | предлагаемый **новый** метод; образец существует: static `CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput`, статически импортируется `DetermineJoinDistributionType:230` | ✅ якорь добавлен |
| Фаза 3: `StreamPropertyDerivations`, `JoinOperatorInfo` | оба файла существуют (`planner/optimizations/StreamPropertyDerivations.java`, `operator/join/JoinOperatorInfo.java`) | ✅ |
| I7: `query_max_stage_count` | реальное свойство (`SystemSessionProperties:87`) | ✅ |
| I8: `SyntheticAddress` | существует (`operator/PagesIndex.java`) | ✅ |
| Acceptance: тест с SMALL_MEMORY_POOL | `TestHashJoinOperator` (main test scope) | ✅ |

### 5.6.2. Внешние ссылки плана — подтверждены первоисточниками

- **Trino #12618** «Hard-fork join operator and remove spilling from it» (PR, merged 2022-07-06,
  release 389): форк join-оператора на spilling/nonspilling, фактический выигрыш ~1-1.5%,
  добавлен session `force_spilling_join`. След в репо: пакеты `operator/join/{spilling,nonspilling}`
  + `SystemSessionProperties:222 FORCE_SPILLING_JOIN`. Принцип №4 плана («нефорк с старта»)
  корректно опирается на этот прецедент — цитата в плане усилена.
- **SPARK-30536** «Sort-merge join operator spilling performance improvements» (ASF Jira):
  описание в наших документах точное — итератор по спиленным right-matches создавался на каждую
  левую строку; фикс = lazy `UnsafeSorterSpillReader` + буфер 1МБ→1КБ; ~500× на TPC-DS Q14.
  Подтверждает P4 (re-iterate = I/O×L) и митигацию «ленивые spill-читатели».

### 5.6.3. Найденные расхождения плана — исправлены (D4–D9)

| # | Расхождение | Исправление |
|---|---|---|
| D4 | §4.0: «Oracle-тестирование … (C1-C9)» — риска C9 не существует | → «C1-C8» |
| D5 | §4.2 «Закрывает»: «I5 (адреса фильтра)» — по регистру адресация фильтра = **I8**; I5 = lifecycle спилл-ранов (корректно в §4.3/§4.6) | → «I8 (адреса фильтра)»; добавлен I12 (fail-during-merge есть в acceptance) |
| D6 | §4.6 маппинг неполон относительно собственных списков «Закрывает»: отсутствовали I7, I8, I10, I11, I12, P8, P9, P10, S-N1; «фаза 1.5» в строке C1 нигде не определена | строки добавлены; C1 → «exclusion → guards+DOUBLE той же фазой» |
| D7 | Регистр README неполон: отсутствовали I7, I11, P8, P9, P10 (план ссылается на I7/I11/P9 при «идентификаторы — из README»); ссылки P5/P7 указывали на §3.7/§3.8 вместо §3.5/§3.7 | 5 строк добавлены; секционные ссылки исправлены |
| D8 | Остаточные упоминания несуществующего `RemoveRedundantSorts` вне risks/04: `10-synthesis §10.5` (roadmap!) и `06-brainstorm:80` — roadmap противоречил исправленному acceptance фазы 0 | §10.5 переписан под реальные потребители; в 06 добавлена аннотация-поправка |
| D9 | risks/02 §2.10 называл `JoinOperatorType` enum'ом (противоречит поправке D3 второго прохода); опечатка «станет瓶颈» (китайский глитч) в risks/03 §3.9 | исправлено («класс со static-фабриками»; «узким местом») |

### 5.6.4. Кросс-сверка с `smj-research-openai/` (независимое исследование)

Появилось в `ai-docs/` после второго прохода; проверено на противоречия с планом 04:

- **Противоречий по существу нет**: их ADR3 «manifests вместо streaming queues на первом этапе»
  соответствует нашему bridge-решению (buffer-модель PrestoDB-стиля, не streaming PrestoDB);
  «новый binary Operator/Driver API для MVP не требуется» совпадает; их критика тезиса
  «без SMJ миллиарды строк невозможны» к нашим документам не применима — grep по
  13/14/README overstated-формулировок не находит (мы всегда оговариваем grace-hash-spill).
- Их самостоятельная оценка этапности (0–5, kernel/oracle → external storage primitives → …)
  совместима с нашими фазами 0–4 (у нас сильнее акцент на план-модель PR1 и Iceberg-enabler).
- Примечание: README openai упоминает `ai-docs/smj-research-kimi/` — такой директории в
  `ai-docs/` нет (dangling reference в чужом артефакте; не правил).

### 5.6.5. Итог третьего прохода

- 10 новых кодовых якорей плана подтверждены; 2 внешних референса (#12618, SPARK-30536) —
  подтверждены первоисточниками и подкреплены следами в репозитории.
- 6 расхождений (D4–D9) найдены и исправлены; все они — точность ссылок/полнота регистров,
  **ни одно не затрагивает архитектурные решения или sequencing фаз**.
- План 04 теперь полностью согласован с регистром рисков (каждый ID из «Закрывает»/§4.6
  резолвится в README-таблицу и в документ 01–03) и с roadmap 10-synthesis.

## 5.7. Четвёртый проход: ссылки, инварианты, I14 (2026-11-04)

### 5.7.1. Целостность `ai-docs/` (автоматическая проверка)

- **Битые относительные ссылки: 0** (скрипт по всем 56 `.md` в `ai-docs/`, включая
  `smj-research-openai/`).
- **Ссылки вида «NN §N.N»: 43/43 резолвятся** в реальные заголовки целевых документов.

### 5.7.2. Повторно подтверждены ключевые инварианты (свежими проверками)

| Факт | Проверка |
|---|---|
| Инвариант merging-exchange (цель ослабления фазы 3) | ExchangeNode.java:104-111 — четыре `checkArgument`: remote merging ⇒ SINGLE_DISTRIBUTION; local merging ⇒ FIXED_PASSTHROUGH_DISTRIBUTION; type == GATHER; один вход | ✅ |
| `AddExchanges.visitJoin` (точка MERGE-ветки фазы 1) | строка 945, сигнатура `PlanWithProperties visitJoin(JoinNode, PreferredProperties)` | ✅ |
| Phase-0 enabler | `IcebergMetadata.getTableProperties`: `// todo support sorting properties` + пустой `ImmutableList.of()` (localProperties); `applyPartitioning` — отбрасывание функций вне запрошенных колонок (mixed-спека `identity(dt)+bucket(k,N)` ⇒ handle `[bucket(k,N)]`) + `// todo if bucketing is a power of two, we can adapt the bucketing` | ✅ дословно |
| «45 мест `new JoinNode(`» (I13) | повторный подсчёт: 45 (main) | ✅ стабильно |
| `JoinNode.distributionType` (eligibility PARTITIONED) | поле `Optional<DistributionType>` (JoinNode.java:66) | ✅ |

### 5.7.3. Новый риск I14 — `UseNonPartitionedJoinLookupSource`

Правило трансформирует `join{build{localExchange(partitioned)}}` → `gather` и выполняется
**после** `AddLocalExchanges` (комментарий PlanOptimizers:1004, само правило :1012); решение
принимается по экономике hash-lookup — для MERGE неприменимо. В наших документах не
упоминалось (найдено независимо в `smj-research-openai/06` ADR 1 — их список правил для
исключения до SMJ-aware реализации). Добавлено: risks/02 §2.13, строка в регистре, гвард в
плане §4.2 (состав фазы 1, п.4) + строка в маппинге §4.6.

### 5.7.4. Кросс-валидация фактов с `smj-research-openai/` (глубже, чем в §5.6.4)

- **NaN**: их таблица (07: «NaN/NaN → compare 0, equal false, candidate group ≠ match»)
  совпадает с нашей C1 — независимое подтверждение критического риска.
- **Radix**: их аудит (13: «Готовый generalized PagesIndex radix path не найден»;
  PagesIndexOrdering = Bentley–McIlroy quicksort) совпадает с нашим 12 — независимое
  подтверждение отсутствия radix в Trino.
- **ADR 1** (представление алгоритма): рекомендуют то же, что наш план — поле `JoinNode.algorithm`
  с обязательным аудитом copy/flip-правил (= наш I13) и финальным validator'ом; отдельный узел —
  долгосрочная альтернатива (= наш консенсус 10 §10.2).
- **ADR 2** (распределение): расхождение подхода, не факта — они встраивают сортировку в
  «собственный producer pipeline», не через SQL `SortNode` («может вызвать gather»). Наш вариант
  SortNode + LOCAL merging exchange этот риск закрывает явно (`visitSort` с distributed-sort
  даёт fixedParallelism + merging GATHER, не remote gather); их поддержка prefix-composite
  ordering совпадает с нашим C4/C8 (префиксная валидность).
- **ADR 3**: manifests vs streaming — эквивалент нашего bridge-решения.

### 5.7.5. Итог четвёртого прохода

- 0 битых ссылок; 43/43 секционных ссылок; 5 ключевых инвариантов подтверждены заново.
- 1 новый риск (I14) с тривиальной митигацией (гвард) включён во все три артефакта
  (02-док, регистр, план) — регистр теперь 34 риска.
- Счётчик в smj-research/README исправлен (20 → 34).
- Архитектура и sequencing плана 04 вновь без изменений; для реализации всё готово.

## 5.8. Пятый проход: readiness-аудит плана для старта реализации MVP (2026-11-04)

Фокус — готовность плана как базы планирования реализации (не только фактчек).

### 5.8.1. Подтверждено (новые якоря)

- `LocalExecutionPlanner.visitJoin:2539` — единственная точка выбора оператора; все
  INNER/LEFT/RIGHT/FULL → `createLookupJoin` (:2558), сам метод — :2879 ⇒ скелет
  `createMergeJoin` по его образцу валиден.
- Модель оператора (05 §5.1): `WorkProcessorOperatorAdapter` существует;
  `spilling.LookupJoinOperatorFactory` — `implements JoinOperatorFactory,
  WorkProcessorOperatorFactory` (:50) ⇒ probe-pull паттерн готов к реюзу;
  `// TODO: support spill` — WorkProcessorOperator.java:21 (обосновывает «тяжёлая память на
  build-стороне» в MVP).
- Нейминг-прецедент: `prefer_partial_aggregation` (SystemSessionProperties:131) ⇒
  `prefer_merge_join` консистентен конвенции.
- SQL-дефолт null-order: `OrderingTranslator.sortItemToSortOrder` (:25-29) —
  UNDEFINED → NULLS_LAST (для ASC **и** DESC) ⇒ канонический OrderingScheme «ASC NULLS_LAST»
  совпадает с дефолтом языка.
- `io.trino.operator.MergeOperator` существует — коллизии с `join.merge.MergeJoinOperator`
  нет (другой пакет и имя класса).

### 5.8.2. Найдено и закрыто

| # | Проблема | Решение |
|---|---|---|
| R1 | План не ссылался на опорные дизайн-документы (03 §3.10 карта правок, 05 §5.4 конечный автомат, 05 §5.6 MatchBuffer) — инженеру пришлось бы искать их самому | добавлен §4.9 «Definition of Ready» с маршрутом первого чтения |
| R2 | Нейминговая вилка: 05 — `join.sortedmerge`/`SortedMerge*` vs план/09/10 — `join.merge`/`MergeJoin*` | канон = `join.merge`/`MergeJoin*` (консенсус 3:1, коллизий нет); 05 §5.1 аннотирован |
| R3 | Канонический null-order в C3 был «например ASC NULLS_LAST» | закреплён в §4.9 + якорь на SQL-дефолт (OrderingTranslator) |
| R4 | Карта 03 §3.10 не содержала правило из I14 | `UseNonPartitionedJoinLookupSource` добавлен в гварды карты |

### 5.8.3. Вердикт по готовности

Блокеров старта MVP нет: карта правок полная (03 §3.10 + I14), модель оператора подтверждена
кодом (WorkProcessor-паттерн, TODO-spill, JoinBridgeManager), микро-решения закреплены
(канонический OrderingScheme, нейминг, property, guard-набор — план §4.9), тест-инфраструктура
расписана (03 §3.9), все якоря верифицированы (§5.1–5.7). Остаточные вопросы — внутрифазовые
(счётчики статистик, EXPLAIN-формат, пороги фазы 2, fuzz-генератор).

## 5.9. Шестой проход: реализуемость механик PR1/фазы 0 (2026-11-04)

Фокус — реализуемость заявленных механик по коду (готовность к детальному дизайну PR).

### 5.9.1. Подтверждено

| Механика плана | Проверка |
|---|---|
| I13: конструкторный паттерн JoinNode | единственный публичный конструктор — `@JsonCreator` (JoinNode:78; делегирует приватному полному :105 с дефолтом последнего boolean). Все 45 внешних мест идут через публичный ⇒ добавление параметра без перегрузок даёт компиляторную проверку каждого места; прецеденты: `spillable`, `maySkipOutputDuplicates` |
| §4.9: enum JoinAlgorithm | прецедент внутреннего enum: `JoinNode.DistributionType {PARTITIONED, REPLICATED}` (JoinNode:52) |
| §4.3: RIGHT/FULL через bridge | `JoinBridgeManager` уже несёт outer-operator lifecycle: `outerOperatorFactoryClosed/Created/Closed`, `getOuterPositionsFuture()` — механика full-outer hash join переиспользуется напрямую |
| §4.4: memory-компонента cost | `LocalCostEstimate.of(cpuCost, maxMemory, networkCost)` (:57) — поле maxMemory существует |
| Фаза 0: структуры Iceberg | `getSupportedSortFields` (IcebergMetadata:1740; вызовы :1726/:1940), `TrinoSortField.java`, `table.sortOrder()` (:1725/:1944) — всё на месте |
| §4.9: рецепт property (03 §3.3) | `DISTRIBUTED_SORT` — SystemSessionProperties:125; default `preferPartialAggregation` — **OptimizerConfig**:76 (`optimizer.prefer-partial-aggregation` :517) |

### 5.9.2. Уточнено (неточности формулировок, не архитектуры)

| # | Было | Стало |
|---|---|---|
| U1 | «поле только в каноническом конструкторе» — но в JoinNode полный конструктор приватный (:105), внешние 45 мест строят через публичный @JsonCreator | «в единственном публичном (@JsonCreator) конструкторе без дефолт-перегрузок» — план §4.2 п.1 + регистр I13 |
| U2 | §4.9: «config-пара в FeaturesConfig» | «в OptimizerConfig (прецедент optimizer.prefer-partial-aggregation :76/:517)» — 03 §3.3 был прав |

Дополнено: §4.3 — реюз outerOperator-lifecycle для RIGHT/FULL; §4.9 — прецедент enum (JoinNode:52).

### 5.9.3. Вердикт

Все механики PR1 (план-модель), PR2 (оператор/bridge) и фазы 0 (Iceberg sortedness) реализуемы
дословно по заявленным якорям; две неточности формулировок исправлены (U1-U2). Готовность к
старту планирования реализации MVP подтверждена повторно.

## 5.10. Седьмой проход: точность тест-якорей и сквозная согласованность (2026-11-04)

### 5.10.1. Подтверждено (уточнение тест-инфраструктуры плана)

| Якорь плана/документов | Проверка |
|---|---|
| Ревокация в середине merge | точное имя константы — `SMALL_MEMORY_POOL_BYTES` (TestHashJoinOperator:127, `DataSize.of(1, MEGABYTE).toBytes()`); план уточнён |
| Golden-тест трансформаций | `JoinNode.flipChildren` — :170 |
| Golden-JSON тест нового поля | найден in-repo прецедент: `TestPatternRecognitionNodeSerialization` (planner/plan тесты) — добавлен в план §4.2 п.1 |
| Rule-тесты | `TestDetermineJoinDistributionType.java` существует (путь из 03 §3.9 точен) |
| Query-level тесты | `core/trino-main/src/test/java/io/trino/sql/query/` — `QueryAssertions.java` и Test* файлы ✓ |
| CBO-фикстуры | `testing/trino-tests/.../BaseCostBasedPlanTest.java` — путь точен |
| Mixed-version (I9) | `testing/trino-product-tests` существует — product-test реализуем |
| P6/13: «query.max-memory = 20 ГБ кластерно» | `MemoryManagerConfig.maxQueryMemory = DataSize.of(20, GIGABYTE)` (:38, `@Config("query.max-memory")` :57); документы значение приписывают верно |

### 5.10.2. Сквозная согласованность (автопроверка, 57 md)

- Регистр: **34 риска**; §4.6: 32 строк (P5/P6 — объединённая строка);
- все ID из «Закрывает»-списков плана резолвятся и в §4.6, и в регистре (0 дыр);
- в регистре нет ID, отсутствующих в §4.6 (кроме объединённой P5/P6-строки);
- битых ссылок: 0.

### 5.10.3. Исправлено

- U3: «тест с SMALL_MEMORY_POOL» → `SMALL_MEMORY_POOL_BYTES` (TestHashJoinOperator:127) + infra
  mixed-version (`testing/trino-product-tests`) — план §4.2 acceptance;
- усилено: golden-JSON прецедент `TestPatternRecognitionNodeSerialization` — план §4.2 п.1.

### 5.10.4. Вердикт

Все тест-якоря плана точны и реализуемы in-repo; согласованность регистр↔маппинг↔«Закрывает»
полная. **План готов к старту планирования реализации MVP SMJ** — седьмой проход не нашёл
ни одного нового риска или расхождения по существу.

## 5.11. Восьмой проход: adversarial-проверка рабочих допущений (2026-11-04)

Метод: попытка **опровергнуть** допущения плана (не подтвердить якоря).

### 5.11.1. Найден реальный пробел — `distributed_sort` (исправлен)

Допущение P11-митигации: «частичная сортировка по сплитам + LOCAL merging exchange» опирается
на путь `AddLocalExchanges.visitSort` с `isDistributedSortEnabled(session)`. Проверка:
- default `distributedSort = true` (OptimizerConfig:64; session `distributed_sort`,
  SystemSessionProperties:125) — по умолчанию работает;
- **но при `distributed_sort=false`** visitSort уходит в else-ветку: `planAndEnvechildren(node,
  singleStream(), defaultParallelism(session))` — весь входной пайплайн (включая скан) загоняется
  в single-stream, т.е. P11-сериализация возвращается через пользовательский флаг.
Исправление: eligibility фазы 1 требует `distributed_sort=true` (план §4.2 п.3 + §4.9 п.4;
расширенная митигация P11 в регистре).

### 5.11.2. Полностью размечена merge-механика (якоря strengthened)

| Уровень | Механика по коду |
|---|---|
| REMOTE merging | `visitRemoteSource → createMergeSource` (LocalExecutionPlanner:909-937): требует `orderingScheme.isPresent()` + `retryPolicy==NONE` (**дословно наш I1-гейт**) + `setDriverInstanceCount(1)` (наш single merge-driver); инстанцирует `MergeOperatorFactory` (io.trino.operator.MergeOperator) |
| LOCAL merging | `visitExchange` (:3658, «Only local exchanges are supported») → `createLocalMerge`: `LocalExchange` + `LocalExchangeSinkOperatorFactory` (N драйверов) + **`LocalMergeSourceOperatorFactory`** (io.trino.operator.**exchange**.LocalMergeSourceOperator:38 — подпакет!) — k-way merge через orderingCompiler |
| Спилл-раны | `OrderByOperator` импортирует `MergeSortedPages.mergeSortedPages` (:42) + Spiller/SpillerFactory — claim документа 12 «quicksort-раны + k-way merge» подтверждён |

Примечание: имя `LocalMergeSourceOperator` в документах 02/06/08/12 **верно**; при проверке
класс ищется в `operator/exchange/`, а не в корне `operator/`.

### 5.11.3. Прочее подтверждено

- Сбор DF: `createDynamicFilterSourceOperatorFactory` оборачивает build-пайплайн над sink
  (:2959 в createLookupJoin; :2724 в nested-loop) — claim 02 §2.3/06 верен; для SMJ PR2 —
  та же обёртка, build-оператору интерфейс не нужен (якорь добавлен в план §4.2 п.2).
- 03 §3.10: `PlanFragmenter` переносит `exchange.getOrderingScheme()` (:545) ✓;
  `CostCalculatorUsingExchanges.visitJoin` (:180) ✓.

### 5.11.4. Вердикт

Adversarial-проход нашёл один скрытый пробел (distributed_sort — пользовательский флаг мог
тихо вернуть сериализацию пайплайна) — закрыт в eligibility; вся merge-механика (REMOTE/LOCAL
merging, спилл-раны, DF-обёртка) подтверждена дословно с точными классами и строками.
Готовность плана к старту планирования MVP подтверждена; регистр рисков без изменений (34),
пробел отнесён к P11.

## 5.12. Девятый проход: целостность + политика выбора SMJ (2026-11-04)

- Контроль целостности: **0 битых ссылок** (58 md), регистр 34 = §4.6 (32 строки, P5/P6
  объединены) = «Закрывает» — 0 дыр; `maxHashPartitionCount = 100` перепроверен
  (QueryManagerConfig:79).
- **Новая deliverable по запросу**: документ
  [15-smj-selection-policy.md](../15-smj-selection-policy.md) — жёсткие рамки выбора SMJ
  планировщиком: MUST-гейты (7 структурных), ресурсные триггеры W1–W4 (S1-GOLD безусловен;
  Bp > α·U; big-big; скидка за готовую сторону), запреты N1–N5 («не платить CPU, где хватает
  RAM»; LLC; broadcast; не-готовые входы при влезающем build; корректность), псевдокод
  `DetermineJoinAlgorithm`, конфиг-пороги α/β/γ/δ, разбор типовых ситуаций (включая
  пользовательский кейс «одинаковый бакет + join-ключ» и его варианты: не-sorted файлы,
  join по (k,dt), разные N).
- Встроено: план §4.2 п.3 (eligibility → 15 §15.2-15.6), §4.4 (COST_BASED → 15 §15.6-15.7),
  §4.9 (опорные документы + [15]); индекс smj-research/README (строка 15).
- Готовность плана подтверждена; критерии выбора формализованы и готовы к превращению в
  план-тесты `TestDetermineJoinAlgorithm` (матрица W/N-переходов — 15 §15.10).

## 5.13. Десятый проход: NULL/FULL-OUTER/DF — три новых исследования (2026-11-04)

По запросу: глубокое исследование трёх новых направлений + сценарии 15B×17B + ревизия плана.
Новые документы [06](06-null-keys-risks.md), [07](07-full-outer-streaming.md),
[08](08-dynamic-filtering-split-skip.md); регистр 34 → **36** (C9, P12).

### 5.13.1. Новые кодовые факты (все проверены)

| Факт | Якорь |
|---|---|
| NULL хэшируется константой → весь NULL-хвост в ОДНОЙ партиции/бакете (и в hash join сегодня, и в bucketing) | `BlockTypeOperators.hashCodeNullSafe`: `isNull → NULL_HASH_CODE` (:101-107) |
| Авто-добавления `IS NOT NULL` к equi-join в Trino НЕТ (можем добавить сами для INNER — семантически эквивалентно) | grep planner/rules — правил `*Null*` нет |
| DF собирается обёрткой над build-sink; применяется page-level + **split-pruning** | `createDynamicFilterSourceOperatorFactory` (LEP:2722-2731); SPI `ConnectorSplitSource.getNextBatch(int, DynamicFilterSnapshot)`; Iceberg: `getNextBatchInternal` строит `dynamicFilterPredicate` из снапшота (IcebergSplitSource:268+) |
| Session-флаг DF | `enable_dynamic_filtering` (SystemSessionProperties:146) |
| FULL OUTER hash требует OuterPositionIterator (буфер позиций + пере-итерация) | потребители `getOuterPositionsFuture`: `PartitionedLookupSourceFactory`, `OuterLookupSource` |

### 5.13.2. Ответы на вопросы пользователя

1. **NULL-хвост**: да, сегодня весь NULL идёт в один бакет/воркер (NULL_HASH_CODE). SMJ решает
   стримингом: fast-path компаратора (INNER — O(1)-skip хвостов, LEFT — стриминг с padding,
   NULL-группа никогда не буферизуется) + planner-фильтр `k IS NOT NULL` для INNER (отсечение
   до shuffle + Iceberg-прунинг файлов по null_count). Распределять NULL нельзя — семантика.
2. **FULL OUTER**: да — null-padding стримится O(1), zero-retention (пик == INNER на тех же
   данных), без OuterPositionIterator (SMJ проще hash-FULL); тонкости — residual
   `anyMatchSurvised`-флаг и re-iterate обеих сторон при спилле групп (фаза 2, risks/07).
3. **Dynamic File Filtering**: поддерживаем — сбор на build над sink (как hash), **только для
   INNER**; пропуск целых групп файлов **уже существует** в Trino домен-основанно
   (`getNextBatch(DynamicFilterSnapshot)` → Iceberg прунит по min/max). Курсорный
   «прошёл диапазон → не читать файл» — позиционного канала нет; оформлен как P12 (фаза 4,
   только S1-GOLD, против DF-бейзлайна; после REPARTITION порядок по hash(k) — не работает).

### 5.13.3. Изменения плана 04

- §4.2: NULL fast-path в операторе (п.2); `IS NOT NULL` для INNER + DF INNER-only (п.3);
  C9 и I3-уточнение в «Закрывает»;
- §4.3: FULL OUTER — однопроходный стриминг, zero-retention, drain build-хвоста; новый
  acceptance «FULL zero-retention»;
- §4.5: P12 range-split-skip (фаза 4, S1-GOLD-only);
- §4.6: строки C9 (фазы 1/2) и P12 (фаза 4);
- §4.9: guard-набор дополнен «DF — только INNER».
- Дополнительно [15 §15.11](../15-smj-selection-policy.md): матрица 7 сценариев для
  memory-constrained 15B×17B (от S1-GOLD «ноль shuffle/sort» до generic unsorted «единственный
  без-OOM»), с рекомендацией DDL.

### 5.13.4. Вердикт

Три новых риска/возможности исследованы по коду и встроены в план; блокеров не добавилось
(C9 закрывается fast-path'ом + фильтром, FULL — архитектурно проще hash, DF уже есть в движке).
План готов к старту планирования MVP.
