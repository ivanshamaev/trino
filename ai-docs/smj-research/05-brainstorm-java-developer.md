# 5. Brainstorm: Java developer — дизайн оператора SMJ

> Роль: core Java developer. Задача — конкретный дизайн оператора sorted merge join.

## 5.1. Компоновка оператора, пакеты, контракты

Пакет `io.trino.operator.join.sortedmerge` (рабочее имя этой записки; **канонический нейминг
финализирован** — risks/04 §4.9, консенсус с 09/10: пакет `io.trino.operator.join.merge`, классы
`MergeJoinOperator`/`MergeJoinBufferOperator`/`MergeJoinSource`; коллизии с существующим
`io.trino.operator.MergeOperator` нет — другой пакет и имя класса):

| Класс | Роль | Контракт |
|---|---|---|
| `SortedMergeJoinBridgeFactory` | создаёт bridge + `JoinBridgeManager` в планировщике | — |
| `SortedMergeJoinBridge implements JoinBridge` | мост: `ListenableFuture<SortedMergeSource>` + `whenBuildFinishes()` | `JoinBridge` |
| `SortedMergeJoinBuildOperator(Factory)` | правая сторона: копит pages, сортирует, опционально спиллит, публикует `SortedMergeSource` | **`Operator`** (push) |
| `SortedMergeJoinOperator(Factory)` | левая сторона: конечный автомат merge | **`WorkProcessorOperatorFactory`** + `JoinOperatorFactory` (через `WorkProcessorOperatorAdapter.Factory`, который уже реализует `JoinOperatorFactory` — важно для `createOuterOperatorFactory()`) |
| `SortedMergeSource` | аналог `LookupSource`: курсор по отсортированным build-строкам | новый интерфейс |
| `SortedMergeJoinPageBuilder` | сборка вывода | аналог `LookupJoinPageBuilder` |
| `MatchBuffer` / `InMemoryMatchBuffer` / `SpillingMatchBuffer` | буфер текущей правой группы | новый интерфейс |
| статистика | — | reuse `JoinStatisticsCounter` как есть |

**Почему probe = WorkProcessorOperator**: весь join-код Trino мигрировал на pull-модель
(`spilling.LookupJoinOperatorFactory` через адаптер). Merge — принципиально pull-алгоритм;
push-контракт `getOutput()` заставил бы дробить стейт-машину. Pull даёт естественный
`WorkProcessor.yielding(driverYieldSignal::isSet)` вокруг горячего цикла.

**Почему build = обычный Operator**: `HashBuilderOperator` — push, `OrderByOperator` — push, и оба
полноценно поддерживают `startMemoryRevoke/finishMemoryRevoke`. **Спилл для WorkProcessor-операторов —
TODO в Trino** (`WorkProcessorOperator.java:21`), поэтому всю «тяжёлую» память держим на push-стороне,
где спилл уже работает. Probe-сторона у SMJ стриминговая, её память = текущая группа — TODO не кусает.

**Точка выбора**: `LocalExecutionPlanner.visitJoin` (~2538-2560) — ветка `createSortedMergeJoin(node, ...)`
по образцу `createLookupJoin` (те же два пайплайна: probe поверх `probeSource` +
`context.addDriverFactory(false, PhysicalOperation(buildFactory, ...), buildContext)`). Копируем
механику `outputSingleMatch`, `compileJoinFilterFunction`, dynamic-filter блока.

## 5.2. Мост: JoinBridgeManager vs PrestoDB MergeJoinSource

- **PrestoDB** (MergeJoinSinkOperator → MergeJoinSource): правая пушит страницы в общий буфер,
  левая читает по мере merge; работало **только в grouped execution** — при обычном исполнении нет
  гарантий прогресса двух драйверов (build-драйвер может не быть запланирован, probe блокируется на
  заполненном буфере → дедлок на ThreadPerDriverTaskExecutor).
- **`JoinBridgeManager`**: build материализуется до конца, probe блокируется на future,
  lifecycle через ReferenceCount, детерминированный `destroy()`.

**Рекомендация: `JoinBridgeManager`** — уже интегрирован с `AddLocalExchanges`, FTE и учётом памяти;
семантика «probe ждёт build» отработана (`waitForBuild`); `destroy()` бесплатно. Отличие от hash join:
`JoinBridge.getOuterPositionIterator()` для RIGHT/FULL не нужен — merge-оператор сам проходит build
в порядке и эмитит непревзойдённые строки по ходу.

**Критический нюанс, который PrestoDB не решал**: в hash join N probe-операторов работают с одним
глобальным `PartitionedLookupSource`; merge join требует **паринга driver-к-driver** (probe-драйвер i
мерджится только с build-драйвером i). Решение: реестр из `partitionCount` мостов; индекс — через
порядок создания операторов (прецедент `totalOperatorsCount`); для phase 1 — фиксированный параллелизм.

## 5.3. Где сортировать

**Вариант A — отдельный SortNode над каждым входом (рекомендация для phase 1)**:
`OrderByOperator` уже даёт внешнюю сортировку (PagesIndex → спилл отсортированных runs → merge) —
ноль нового кода по сортировке и спиллу; SMJ-оператор крошечный («обе стороны отсортированы,
только мерджить»). Планировщик элитирует Sort, если вход уже отсортирован (Iceberg sort order →
TableScan property). Минус: лишний проход/материализация probe даже там, где merge мог бы стримить.

**Вариант B — сортировка внутри оператора**: build — копия тела OrderByOperator (или выделить
общий `SortedPagesBuilder`); probe отсортировать «на лету» нельзя без полной материализации —
убивает главное преимущество. Вариант вырождается в A + дублирование кода.

**Итог: A.** Правило вставляет `SortNode(left/right)` при отсутствии нужного ordering.
Важно: **локальный hash-exchange не сохраняет порядок** (`PartitioningExchanger` интерливит страницы
драйверов) — при PARTITIONED SortNode обязан стоять после exchange; co-located bucket-execution case
сохраняет сортированность внутри бакета — идеальный кейс.

## 5.4. Алгоритм merge: конечный автомат

Ключевая асимметрия: **правая (build) группа буферизуется, левая стримится построчно**.

```
state ADVANCE_LEFT:
  if (!left.hasNext) -> DRAIN_RIGHT_TAIL            // RIGHT/FULL: все оставшиеся right как outer
  l = left.current
  if (l.hasNullKey): emitLeftOuterIfOuter(l); left.advance; stay   // NULL никогда не матчится
  cmp = comparator.compare(l.key, right.peekKey)
  if (cmp > 0):  // правая группа < l: её окно закрыто навсегда
     if (RIGHT/FULL && !rightWindowMatched) emitRightOuter(rightWindow)
     right.advanceWindow(); stay
  else if (cmp < 0):
     emitLeftOuterIfOuter(l); left.advance; stay
  else: -> LOAD_GROUP

state LOAD_GROUP:
  rightWindow  = successive rows with key == l.key   // bounded, см. MatchBuffer
  rightWindowMatched = false
  leftWindow   = successive left rows with key == l.key  // стриминг, не буферизуем
  -> EMIT

state EMIT:  // декартово, с resume
  for (li = savedLi; leftWindow.hasMore; li++):
     rowMatched = false
     for (ri = savedRi; ri < rightWindow.size; ri++):
        if (filter == null || filter(buildPos=rightWindow[ri], probePos=li, probePage)):
           pageBuilder.appendRow(probeRow=li, buildRow=rightWindow[ri])
           rowMatched = true; rightWindowMatched = true; markMatched(ri)
           if (outputSingleMatch) break
        if (!pageBuilder.hasRoom()):
           savedLi = li; savedRi = ri+1; return page (yield!)   // RESUME-точка
     if (!rowMatched && LEFT/FULL) emitLeftOuter(li)
     savedRi = 0
  -> CLOSE_GROUP

state CLOSE_GROUP:
  if (RIGHT/FULL): emitRightOuter(unmatched rows of rightWindow)   // по bitmap markMatched
  right.advanceWindow(); -> ADVANCE_LEFT

state DRAIN_RIGHT_TAIL:  // левая исчерпана
  все оставшиеся right-строки -> emitRightOuter; -> DONE
```

Resume после полного page builder — запомненная пара `(savedLi, savedRi)` в EMIT; благодаря pull-модели
«return page» не требует флагов. Residual filter — `JoinFilterFunction.filter(buildPos, probePos, probePage)`
(реюз `JoinFilterFunctionCompiler` как есть).

**NULL-ключи**: компаратор даёт тотальный порядок (NULLS FIRST/LAST), но равенство по компаратору
≠ equi-match при NULL. Правило: перед сравнением на окно — `isNull` по каждому ключевому каналу обеих
сторон; NULL → строка не матчится (левая — outer при LEFT/FULL; правая — outer при RIGHT/FULL).
NULL-кластеры проходят как «фантомные окна» без матчей.

**outputSingleMatch**: реюз вычисления из `createLookupJoin` (`maySkipOutputDuplicates` && правые
выходы ⊆ правых equi-символов, только INNER/LEFT); два уровня: break после первого матча (валидно
всегда) и skip всего левого окна (только при отсутствии residual filter).

## 5.5. Матрица edge cases

| Кейс | Поведение |
|---|---|
| Левая пуста | сразу `DRAIN_RIGHT_TAIL` (RIGHT/FULL — весь build outer; INNER/LEFT — finished) |
| Правая пуста | все левые outer (LEFT/FULL) или finished (INNER); build-finish гарантирован `whenBuildFinishes()` |
| **Все ключи равны (гигантская группа)** | вывод L×R взрывается — семантика. Память: буфер правой группы bounded + spill. Cost-модель не должна выбирать SMJ при NDV(key) ≪ rowCount |
| Разные ASC/DESC по каналам | требование: обе стороны отсортированы **одинаковой** последовательностью SortOrder; компаратор один (`OrderingCompiler` принимает sortChannels+sortOrders). Расхождение → правило вставляет выравнивающий Sort; пер-сторонние порядки не поддерживаем |
| Dictionary/RLE входы | компаратор encoding-агностик (Block+position). Вывод: левая без материализации (dictionary по позициям окна); RLE-страница с одним ключом = целое окно почти бесплатно; фастпас: RLE на всех ключевых каналах → одна проверка ключа на страницу |
| Нестабильная сортировка | порядок равных ключей недетерминирован — семантике не важно; тесты сравнивают с учётом сортировки |
| Спилл одной стороны | build спиллится как OrderByOperator **до** публикации SortedMergeSource; probe не спиллится вовсе: до `whenBuildFinishes()` блокируется, вверх exchange даёт backpressure (`StreamingDirectExchangeBuffer` bounded) — снимает класс проблем PrestoDB с буферизацией левой |
| LIMIT над join | после насыщения `finish()` → refcount → `destroy()` освобождает build до исчерпания build-драйвера; с outputSingleMatch+без фильтра — ранний выход из гигантской группы |
| Semi join (опционально) | phase 2: как INNER с outputSingleMatch и проекцией только левых колонок |
| Dynamic filtering | SMJ не строит хэш, но `DynamicFilterSourceOperator` на build-пайплайне независим: min/max домены собираются как обычно; `waitForBuild` бесплатен. Ограничение: distinct-values фильтр из merge не получить — только domains |

## 5.6. Буфер правой группы

Требования: append-only, последовательный доступ, re-iterate (декартово: L проходов), bounded, spill.

- `SortedMergeSource` внутри держит **`PagesIndex`** (build уже отсортирован в него build-оператором —
  реюз инфраструктуры); окно = просто диапазон `(startPosition, length)`.
- `InMemoryMatchBuffer` — пока `retainedSize < threshold` (порогы строк/байт по образцу Spark
  `ExternalAppendOnlyUnsafeRowArray`; конфиг): чтение прямо из PagesIndex.
- `SpillingMatchBuffer` — при превышении: окно читается последовательными чанками через
  `SingleStreamSpiller`/`PagesSerde`; **re-iterate спиленного окна = block nested loop**: правое окно
  чанками по C строк, на каждый чанк — проход по левому окну (левое тогда буферизуем, bounded).
  I/O = R×L/C вместо R×L.
- Matched-tracking для RIGHT/FULL — `long[]` bitmap по build-позициям (как `visited` в `OuterLookupSource`),
  живёт в bridge, сбрасывается `destroy()`.

## 5.7. Варианты архитектуры и рекомендация

**V1 «PrestoDB-way»** — streaming merge с bounded-буфером между пайплайнами. Минусы: дедлоки без
grouped execution, чужеродный backpressure-протокол, сложный спилл build «по ходу», координация
RIGHT/FULL хвостов. **Вердикт: нет.**

**V2 «Hash-join-way»** — полная материализация build (сортированной, spill-capable) +
`JoinBridgeManager`, probe стримится, сортировки — SortNode'ами в плане. Плюсы: максимум реюза
(JoinBridgeManager, OrderByOperator+спилл, PageWithPositionComparator, JoinFilterFunctionCompiler,
JoinStatisticsCounter, 80% обвязки createLookupJoin), детерминированный lifecycle. Минусы: build
материализуется целиком (но spill-capable), лишний проход сортировки при уже отсортированном входе
(элитируется). **Рекомендация: V2, phase 1.**

**V3 «Гибрид»** — build сортируется инкрементально, `SortedMergeSource` читается lazily из
спилл-стрима (не unspill целиком; merge и так последовательный). bounded memory даже на гигантском
build — это phase 2 поверх V2 и **главный аргумент SMJ против hash join на десятках млрд строк:
merge читает build строго последовательно**, в отличие от случайного доступа JoinHash.

## 5.8. Открытые вопросы (от роли)

1. Паринг драйверов при PARTITIONED: надёжная раздача bridge[i] (instance-index в OperatorFactory/DriverContext, прецедент totalOperatorsCount).
2. Cost-модель: SMJ включать по property-детекции (Iceberg sorted/bucketed) + escape-hatch session-флаг.
3. Гигантские группы с обеих сторон при спилле: рекурсивный block nested loop или fail с диагностикой?
4. Semi/anti join: отложить, но не проектировать в тупик.
5. FTE/grouped execution: JoinBridgeManager уже дружит; streaming V1 — нет (ещё довод за V2).
