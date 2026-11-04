# 02. Риски интеграции SMJ с движком Trino

Класс рисков: SMJ корректен, но **не вписывается** в подсистемы движка (шедулирование, память,
спилл, сериализация, наблюдаемость) — дедлоки, зависания, утечки, несовместимость.

## 2.1. 🟠 I1: Fault-tolerant execution и adaptive-планировка

Факты (код):
- `LocalExecutionPlanner.createMergeSource` (899-939) требует `retryPolicy == NONE` для
  merging-источников — merge-оператор сегодня не переживает retry задач (упорядоченность после
  повторной доставки страниц не восстанавливается автоматически; `DeduplicatingDirectExchangeBuffer`
  дедуплицирует, но порядок страниц при повторе не гарантирует порядок слияния).
- `AdaptiveReorderPartitionedJoin` (FTE-only) молча переворачивает PARTITIONED-join'ы при
  перепланировке → потеряет sortedness-согласованность, если SMJ-маркер переживёт перепланировку.
- `AdaptivePlanner` в FTE перепланирует стадии между попытками — план-маркер `algorithm` может
  «уехать» с реальностью (входы стали другими).

**Решение**: жёсткий gate в MVP: правило `DetermineJoinAlgorithm` возвращает empty при
`getRetryPolicy(session) == TASK`; гвард в `AdaptiveReorderPartitionedJoin` (skip при
`algorithm.isPresent()`); спулинг-exchange с ordering — отдельная задача фазы 4
(«ordered spooling partitions»: писать/читать партиции спула с сохранением порядка, retry
идемпотентен по страницам — восстановимо).

## 2.2. I2: Шедулирование: фазы и порядок источников (проверено — риска нет)

- `PhasedExecutionSchedule.visitJoin` (383-393): special-case только для REPLICATED (build-фаза
  первой); SMJ всегда PARTITIONED → обе стороны в одном фрагменте группы, стандартный порядок.
- `SchedulingOrderVisitor` (28-64) собирает только листовые `TableScanNode`/`TableFunctionProcessorNode`
  (build-side-first через `BuildSideJoinPlanVisitor`); REMOTE-источники join-фрагмента не
  участвуют — конфликтов нет.
- Буферизация моста: как у hash join — build до конца (bridge), probe блокируется на
  `whenBuildFinishes()`. Дедлоков класса PrestoDB (два потоковых драйвера) нет.

**Обязательство**: тест «SMJ + execution_policy=phased/all_at_once» на эквивалентность.

## 2.3. I3: Dynamic filtering — сохраняется, но проверить правила

Мост-модель сохраняет DF: `DynamicFilterSourceOperator` стоит на build-пайплайне **до**
build-sink и от реализации join не зависит; probe ждёт (`waitForBuild`) — для SMJ это и так
естественно. Риски:
- `PredicatePushDown.createDynamicFilters` добавляет `df(...)`-предикаты в probe-скан — фильтрация
  row groups сохраняет порядок (вырезание диапазонов) → sortedness не ломается. Но `df(...)` на
  probe **до** SMJ-стороны: если SMJ сам является probe другого join'а — никакой разницы с hash.
- `RemoveUnsupportedDynamicFilters` матчит TableScan на probe — не зависит от algorithm.

**Обязательство**: product-test «DF + SMJ» (сравнение отфильтрованных сплитов с hash-вариантом);
уточнить в `PredicatePushDown`, что SortNode над probe **сохраняет** df-выражения (SortNode —
пассивный по предикатам; свойства: `PropertyDerivations.visitSort` задаёт локальную
сортировку, DF-домены не трогает).

## 2.4. 🟡 I4: Синхронный доп. спилл в `OrderByOperator.finish()` — TODO в коде

`OrderByOperator.finish()` (204-244): при неудаче конвертации revocable→user памяти выполняет
**синхронный** `getFutureValue(spillToDisk())` с комментарием «TODO: should be asynchronous».
На BIG-партициях (десятки ГБ) хвост без ревока может не влезть в user-память → блокировка
драйвера на весь спилл. Для SMJ (где SortNode — обязательная часть плана) это горячий путь.

**Решение**: обязательный пункт фазы 1 — асинхронный доп. спилл (возвращать future, блокироваться
через `isBlocked()`); тест на «tail-spill» с малым пулом.

## 2.5. 🟡 I5: Lifecycle спилл-ранов

Факты: `FileSingleStreamSpiller.getSpilledPages()` — **одноразовое чтение** (`writable.getAndSet(false)`);
`GenericSpiller` создаёт новый spiller на каждый spill → ранов сколько вызовов; удаление файлов —
в `close()`; `SpillSpaceTracker` резервирует байты.

Риски SMJ:
- merge-join потребляет раны **лениво и долго** (пока идёт join) — spill-файлы висят занятыми
  весь запрос; конкуренция за `max-spill-per-node` с соседними задачами → возможен
  `OUT_OF_SPILL_SPACE` посреди join. Митигация: учитывать длительность владения; лимит per-node
  поднят под ETL; каскад-merge для сокращения числа открытых ранов.
- Если merge-join сам ревоцируется (его буферы) — раны трогать нельзя (они на диске и не в
  revocable-памяти — ок).
- Повторное чтение рана (re-iterate правой группы) — **запрещено** API → SpillingMatchBuffer обязан
  работать через чанки с собственной буферизацией (block nested loop), не через повторный
  `getSpilledPages()`. Задокументировать как контракт.

## 2.6. 🟡 I6: Memory-учёт словарных выходов

Выход SMJ через `DictionaryBlock` поверх исходных страниц (оптимизация из 07/12): словарь держит
ссылки на build/probe страницы → их retained-size живёт после `finish()` оператора, пока страницы
не собраны downstream. `MemoryRevokingScheduler` может пытаться ревоцировать закрытый оператор
(запросов нет — учёт уже не у него).

**Решения**: (а) перенос учёта: при выдаче словарной страницы оператор списывает долю
retained-size исходных страниц в downstream-контекст (паттерн «ownership transfer»); (б) flatten
при ревокации (радикально); (в) MVP: отказаться от словарного вывода для build-каналов (копия
через PageBuilder как у hash join), включить по флагу с телеметрией. Рекомендация: (в) в MVP → (а) в фазе 2.

## 2.7. I7: `query_max_stage_count` и форма плана

MVP вставляет `SortNode` **внутри join-стадии** (после RemoteSourceNode) — новых фрагментов нет,
число стадий не растёт. Phase-3 (partialSort на скан-стадии + partitioned merging exchange) — тоже
не добавляет стадий (те же два exchange на join, что у hash). Риск возникает только если выбрать
путь «отдельные sort-стадии» — не выбираем. Фиксируем тестом на количество стадий SMJ-плана == hash-плану.

## 2.8. 🟡 I8: Контракт residual-фильтра и адресация build

`JoinFilterFunctionFactory.create(session, LongArrayList addresses, List<Page> pages)` — фильтр
адресует build-строки через `SyntheticAddress` в `List<Page>`. SMJ-build обязан предоставлять
эту структуру. Если build — PagesIndex (сортированный), адреса уже есть (`valueAddresses`) —
но **после сортировки адреса переупорядочены**: фильтру всё равно (адресует (slice, position),
порядок не важен). Если build — streaming из спилл-ранов без материализации, придётся собирать
`List<Page>` для текущего окна (окно и так буферизуется — MatchBuffer хранит страницы). ✓ совместимо.

**Решение**: MatchBuffer выставляет `addresses+pages` для фильтра; тест residual-filter на
multi-page группе.

## 2.9. I9: Сериализация плана и mixed-кластер

`Optional<JoinAlgorithm>` с `@JsonCreator`-дефолтом: старый воркер игнорирует поле → hash join
(валидный результат). Новый воркер на старом координаторе — поля нет → empty → hash. Тест
mixed-version (coordinator↔worker обоих направлений) в product-tests.

## 2.10. I10: Наблюдаемость

- `JoinOperatorType` для WebUI (это класс со static-фабриками, не enum — поправка risks/05 D3):
  добавить фабрику `mergeJoin(...)` + operatorType-строку (+ маппинг статистик);
  `PlanPrinter` печатает algorithm; JSON EXPLAIN — additive.
- Статистики оператора: `JoinStatisticsCounter` + гистограмма групп + счётчики ранов/спилла —
  без них диагностика «почему SMJ медленный» невозможна.
- Golden-тесты EXPLAIN: algorithm печатается **только при наличии** → существующие планы не меняются.

## 2.11. I11: Конкуренция операторов за пул в одной задаче

Join-стадия MVP: 2 SortNode (revocable) + merge-join (мало) + возможная агрегация выше. Ревоки
интерливятся между сторонами сортировки — раны получаются мелкими и многочисленными. Митигации:
(а) сериализовать спиллы внутри задачи (общий spiller-координатор — в Trino его нет, добавить
локальный в контексте задачи несложно); (б) каскад-merge; (в) тест на 4 конкурентных SMJ-задачи
в одной воркере с малым пулом.

## 2.12. I12: Спекулятивное исполнение / повтор запроса

Pipelined-режим не ретраит задачи (QUERY-restart при падении ноды — весь запрос заново, spill
очищается при abort задачи через close операторов — проверить в тесте «fail during merge»:
утечки файлов спилла недопустимы; `LocalSpillManager` резервирует/освобождает — тест на инвалидацию).

## 2.13. 🟠 I14: `UseNonPartitionedJoinLookupSource` переписывает локальный exchange над build

Правило (`iterative/rule/UseNonPartitionedJoinLookupSource.java`) трансформирует
`join{probe, build{localExchange(partitioned)}}` → `localExchange(gather)` и выполняется
**после** `AddLocalExchanges` — это зафиксировано комментарием в `PlanOptimizers:1004`
(«needs to run after AddLocalExchanges since it operates on ExchangeNodes added by this
optimizer»; само правило — `PlanOptimizers:1012`). Решение правило принимает из экономики
hash-lookup-источника — для MERGE-join оно неприменимо: перестройка топологии над build-стороной
ломит контракт SMJ «ровно один сортированный поток на сторону». В корректном SMJ-плане
partitioned-local-exchange над build возникать не должен (наши гварды AddLocalExchanges дают
single-stream + merging-GATHER), но при баге в гвардах правило сработает **молча** — план
останется формально валидным, а контракт сортированности будет нарушен.

**Решение**: гвард в правиле (не применять при `JoinNode.algorithm=MERGE`); golden-тест
«SMJ-план не изменяется этим правилом». Независимо обнаружено в `smj-research-openai/06` ADR 1.
