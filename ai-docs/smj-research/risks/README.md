# Риски реализации Sorted Merge Join: сводный регистр

Поддиректория содержит углублённое исследование подводных камней реализации SMJ в Trino
(дополнение к документам 01-13, которые уже закрывали «первый эшелон» рисков — см.
[10-synthesis §10.6](../10-synthesis-and-recommendations.md)). Каждый риск здесь:
верифицирован по коду или обоснован, имеет анализ и конкретные решения.

## Документы

| # | Документ | Класс рисков |
|---|---|---|
| 1 | [01-correctness-risks.md](01-correctness-risks.md) | **Корректность** — расхождение семантики равенства и упорядочивания (NaN!), NULL-порядки, не-orderable типы (MAP), префиксная сортированность, per-file sortedness Iceberg, cast'ы в equi-клаузах |
| 2 | [02-engine-integration-risks.md](02-engine-integration-risks.md) | **Интеграция с движком** — FTE/adaptive, шедулинг, memory accounting, lifecycle спилла, сериализация плана, WebUI/EXPLAIN, контракт residual-фильтра |
| 3 | [03-performance-risks.md](03-performance-risks.md) | **Производительность** — параллелизм join-стадии, деградация k-way merge, копирование вывода, I/O-амплитфикация групп, конфиг-дефолты, ложные ожидания |
| 4 | [04-implementation-plan-with-risk-gates.md](04-implementation-plan-with-risk-gates.md) | **План реализации с учётом рисков** — фазы с risk-гейтами, маппинг «риск → тест → фаза», критерии go/no-go, стратегии отката |
| 5 | [05-verification-report.md](05-verification-report.md) | **Отчёт верификации**: второй проход — 18/18 фактов, 2 расхождения, риски I13/P11/C8/S-N1; третий (§5.6) — план 04 по всему `ai-docs/` (#12618/SPARK-30536 подтверждены, D4-D9 исправлены); четвёртый (§5.7) — 0 битых ссылок, инварианты подтверждены, новый риск I14 |
| 6 | [06-null-keys-risks.md](06-null-keys-risks.md) | **C9: гигантский NULL-хвост** — NULL хэшируется константой (один бакет уже в hash/bucketing); fast-path компаратора (INNER O(1)-skip, LEFT стриминг) + planner `IS NOT NULL` для INNER с прунингом файлов по null_count |
| 7 | [07-full-outer-streaming.md](07-full-outer-streaming.md) | **FULL OUTER в SMJ**: null-padding стримится O(1), zero-retention (пик == INNER), без OuterPositionIterator; residual `anyMatchSurvived`-флаг; drain build-хвоста; acceptance фазы 2 |
| 8 | [08-dynamic-filtering-split-skip.md](08-dynamic-filtering-split-skip.md) | **DF и пропуск файлов**: сбор на build над sink (INNER-only!); split-pruning уже есть — `getNextBatch(DynamicFilterSnapshot)` → Iceberg прунит файлы по доменам; курсорный range-skip — P12, фаза 4, только S1-GOLD |

## Регистр рисков (top-20, полные описания в документах)

| # | Риск | Severity | Вероятность | Ключевая митигация | Документ |
|---|---|---|---|---|---|
| C1 | **NaN в DOUBLE/REAL ключах**: `compare(NaN,NaN)=0`, но `equal(NaN,NaN)=false` — SMJ смержит NaN-ключи, hash join нет → **неверные результаты** | 🔴 критический | средняя (NaN в ключах редок, но легален) | NaN-guard в merge-цикле (как NULL); MVP: исключить DOUBLE/REAL ключи или обязательный guard + fuzz-тесты | 01 §1.1 |
| C2 | MAP в join-ключах: equi-join работает по хэшу (MapType.isComparable=true — analyzer разрешает), но `isOrderable()=false` → SMJ неприменим | 🔴 | низкая | eligibility-проверка `isOrderable()` всех ключей | 01 §1.2 |
| C3 | NULL-порядки сортировок сторон не совпадают (NULLS FIRST vs LAST) | 🟠 | средняя | требование идентичного OrderingScheme обеих сторон; NULL-guard до сравнения | 01 §1.3 |
| C4 | Sortedness только по префиксу ключа (sorted by (a), join on (a,b)) | 🟠 | высокая | полный матчинг scheme (не префикс); обратное (sorted (a,b), join (a)) — валидно | 01 §1.4 |
| C5 | **Iceberg «sorted» = per-file**: конкатенация файлов в бакете не отсортирована | 🟠 | определённо | k-way merge сплитов (каждый сплит = отсортированный диапазон); mixed sortOrderId-файлы → refuse | 01 §1.5 |
| C6 | Cast в equi-клаузе ломает вывод sortedness (property через identity-only) | 🟡 | высокая | принять как missed opportunity; не «чинить» кастами | 01 §1.6 |
| C7 | Коннектор сообщил ложную sortedness | 🔴 | низкая | консервативный критерий; PlanSanityChecker; fuzz vs hash-oracle | 01 §1.7 |
| C8 | **File-level `sortOrderId` vs table-level** + префиксная отчётность sortedness (Spark/Flink-писатели, unsorted legacy-файлы) | 🟠 | средняя | отчёт по sortOrderId каждого файла из манифеста; валидный identity-префикс | 05 §5.3 |
| **C9** | **Гигантский NULL-хвост ключа**: все NULL хэшируются константой (`NULL_HASH_CODE`, BlockTypeOperators:101-107) → один бакет/партиция и в hash, и в bucketing; наивный MatchBuffer в SMJ даст OOM (NULL-группа не матчится, но буферизуется) | 🔴 | высокая при NULL-хвостах в больших таблицах | fast-path компаратора: NULL-группа никогда не буферизуется (INNER — O(1)-skip хвостов обеих сторон; LEFT — стриминг с padding); planner-фильтр `k IS NOT NULL` для INNER (отсечение до shuffle + прунинг файлов по null_count) | 06 |
| I1 | FTE (`RetryPolicy.TASK`): merge-источники требуют `retryPolicy==NONE`; AdaptivePlanner может потерять маркер | 🟠 | средняя | жёсткий gate; ordered spooling в фазе 4 | 02 §2.1 |
| I2 | Шедулирование: phased/порядок источников — **проверено, риска нет** (требуется тест) | 🟢 | — | e2e equivalence phased/all-at-once | 02 §2.2 |
| I3 | Dynamic filtering: сохраняется через build-sink — требуется product-test | 🟡 | низкая | product-test DF+SMJ | 02 §2.3 |
| I4 | Синхронный доп. спилл в `OrderByOperator.finish()` (TODO :226) блокирует драйвер | 🟡 | высокая | асинхронный спилл — обязательный пункт фазы 1/2 | 02 §2.4 |
| I5 | Одноразовые spill-раны (два сайта :145/:189) + конкуренция задач | 🟡 | средняя | контракт «ран ≡ immutable stream»; каскад-merge; SpillingMatchBuffer без re-read | 02 §2.5 |
| I6 | Memory-учёт словарных выходов (DictionaryBlock держит исходные страницы) | 🟡 | высокая | ownership-transfer/flatten при ревокации; MVP — копирование build-каналов | 02 §2.6 |
| I7 | `query_max_stage_count`: рост числа стадий от SortNode/merge-exchange | 🟢 | низкая (SortNode внутри join-стадии — стадии не растут) | тест stage-count(SMJ) == stage-count(hash) | 02 §2.7 |
| I8 | Residual-фильтр адресуется в build через `LongArrayList` адресов — SMJ-build обязан давать SyntheticAddress | 🟡 | определённо | build на PagesIndex (адреса есть) | 02 §2.8 |
| I9 | Сериализация/mixed-кластер (Optional-поле) | 🟡 | низкая | @JsonCreator default; product-test | 02 §2.9 |
| I10 | WebUI/EXPLAIN: `JoinOperatorType` — класс со static-фабриками (не enum — поправка верификации) | 🟡 | низкая | фабрика mergeJoin(...) + operatorType-строка; печать algorithm только при наличии | 02 §2.10 |
| I11 | Конкуренция операторов за пул в одной задаче (интерлив-ревоки → мелкие многочисленные раны) | 🟡 | средняя | сериализация спиллов в задаче (локальный координатор); каскад-merge; тест 4 конкурентных SMJ-задач | 02 §2.11 |
| I12 | Утечки spill-файлов при фейле во время merge | 🟡 | низкая | тест fail-during-merge; LocalSpillManager резерв/освобождение | 02 §2.12 |
| **I13** | **45 мест `new JoinNode(`**: непроброшенное поле `algorithm` — silent miss маркера | 🟠 | высокая (забыть легко) | поле в единственном публичном @JsonCreator-конструкторе без дефолт-перегрузок (компилятор ловит все 45; прецеденты `spillable`/`maySkipOutputDuplicates`); golden-тесты правил; plan-compare CI | 05 §5.3 |
| **I14** | **`UseNonPartitionedJoinLookupSource`** (выполняется после AddLocalExchanges — PlanOptimizers:1004/1012) может заменить partitioned local exchange над build на gather — молча ломает SMJ-контракт топологии | 🟠 | низкая (в корректном SMJ-плане паттерн не возникает — но при баге гвардов сработает тихо) | гвард в правиле (MERGE исключается) + golden-тест | 02 §2.13 |
| P1 | Параллелизм join-стадии: 100 партиций default = 2 задачи/воркер | 🟠 | определённо | `query_max_hash_partition_count` 200-400; multi-driver фаза 2 | 03 §3.1 |
| P11 | **singleStream SMJ-сторон сериализует весь probe-pipeline (включая скан) — 1 драйвер/задачу** | 🟠 | определённо | частичная сортировка по сплитам + LOCAL merging exchange (паттерн visitSort; потребитель — `LocalMergeSourceOperator`, exchange-пакет) в MVP; eligibility требует `distributed_sort=true` (default; при `false` visitSort сам загоняет вход в singleStream) | 05 §5.3 |
| **P12** | **Cursor-driven range-split-skip** (пользовательский сценарий «merge прошёл диапазон → не читать файлы с max(k) < курсора»): позиционного канала курсор→коннектор в Trino нет; после REPARTITION порядок по hash(k) — не работает | 🟢 (opportunity) | — | MVP: доменный DF уже прунит файлы (`IcebergSplitSource.getNextBatch(DynamicFilterSnapshot)`, :268+); курсорный вариант — фаза-4 spike только для S1-GOLD, против DF-бейзлайна | 08 |
| P2 | PriorityQueue на сотнях входов (boxing, log k) | 🟡 | средняя | каскад fan-in ≤ 64; прямой 2-way цикл | 03 §3.2 |
| P3 | Полное копирование вывода через PageBuilder | 🟡 | определённо | словарная сборка / sort-join fusion | 03 §3.3 |
| P4 | Re-iterate спиленной группы = I/O×L (урок Spark SPARK-30536) | 🟠 | средняя | block nested loop чанками; ленивые spill-читатели | 03 §3.4 |
| P5 | «Всё в LLC» — SMJ проигрывает hash по CPU | 🟡 | высокая | cost-порог; FORCE только для тестов | 03 §3.5 |
| P6 | Конфиг-дефолты непригодны для BIG join (spill off/100GB/NONE; query.max-memory 20GB) | 🟠 | определённо | ETL-пресет конфигурации в документации | 03 §3.6 |
| P7 | Sorted-but-not-bucketed не даёт выгоды (shuffle ломает порядок) — ложные ожидания | 🟡 | высокая | документация; range-разбиение как future | 03 §3.7 |
| P8 | Спилл на сетевых томах (EBS/NFS): 100-200 МБ/с вместо 2 ГБ/с → фаза спилла ×10 | 🟡 | средняя (облачные кластеры) | документация + runtime-метрика `spillThroughput` с warning при < 500 МБ/с | 03 §3.8 |
| P9 | Merge-драйвер однопоточен — bottleneck на single-hot-partition группах | 🟡 | средняя | MergePath-декомпозиция merge (фаза 4); метрика queue-time драйвера | 03 §3.9 |
| P10 | Деградация сортировки на «почти-сгруппированном» порядке после shuffle | 🟢 | низкая (quicksort 3-way не деградирует на повторах) | зафиксировать бенчмарком; Timsort не нужен | 03 §3.10 |
| S-N1 | Co-location только в пределах **одного каталога** (`getCommonPartitioning` требует равенство catalogHandle) | 🟡 | средняя | документация сценариев; same-catalog для S1-GOLD | 05 §5.3 |

Верификация всех фактов (второй проход по репозиторию) — в [05-verification-report.md](05-verification-report.md):
18/18 утверждений о коде подтверждены; 2 расхождения исправлены (`RemoveRedundantSorts` не существует,
streaming-агрегации в Trino нет); speculative execution проверен и риском не является (FTE-internal).
Третий проход (§5.6 отчёта) — верификация плана 04 по всему `ai-docs/` и коду: #12618 и SPARK-30536
подтверждены внешними источниками; исправлены ID-ссылки (I5→I8), неполный маппинг §4.6, «C1-C9»,
регистр дополнен I7/I11/P8/P9/P10. Четвёртый проход (§5.7) — 0 битых ссылок (56 md), повтор ключевых
инвариантов, новый риск **I14** (`UseNonPartitionedJoinLookupSource`), регистр = 34 риска.
Десятый проход (§5.13) — три новых исследования: **C9** NULL-хвост (risks/06), FULL OUTER
стриминг (risks/07), DF/split-skip **P12** (risks/08); регистр = **36 рисков**.
План реализации с учётом рисков —
[04-implementation-plan-with-risk-gates.md](04-implementation-plan-with-risk-gates.md).

Легенда: 🔴 блокер корректности, 🟠 существенно влияет на дизайн/производительность, 🟡 управляемо.

## Главный вывод

Новых **блокеров архитектуры не обнаружено** — найден один новый критический риск корректности
(NaN-семантика, §01.1), который решается локальным guard'ом в merge-цикле, и подтверждён набор
инженерных обязательств (per-file sortedness Iceberg, параллелизм, конфиг-пресет). План реализации
с учётом всех рисков — в [04-implementation-plan-with-risk-gates.md](04-implementation-plan-with-risk-gates.md).

Все факты этого раздела повторно верифицированы по коду — см.
[05-verification-report.md](05-verification-report.md): критический NaN-риск и все опорные
факты подтверждены дословно; два маркетинговых утверждения скорректированы; добавлены риски
I13 (45 мест пересборки JoinNode), P11 (сериализация probe-pipeline при singleStream),
C8 (file-level sortOrderId), S-N1 (co-location в пределах одного каталога).
