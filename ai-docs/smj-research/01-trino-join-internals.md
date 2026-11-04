# 1. Внутреннее устройство JOIN в Trino (для разработки SMJ)

Код: `core/trino-main/src/main/java/io/trino/operator/` (пакет `io.trino.operator.join`
с подпакетами `nonspilling` и `spilling`), `core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`.

Важное общее замечание: probe-операторы join реализованы на pull-модели **WorkProcessorOperator**
и адаптируются к классической push-модели `Operator` через `WorkProcessorOperatorAdapter`.

## 1.1. Контракты базовых интерфейсов операторов

### Operator (push) — `operator/Operator.java`
```java
ListenableFuture<Void> isBlocked();   // future завершится, когда оператор разблокируется
boolean needsInput();                  // true iff может принять страницу
void addInput(Page page);              // вызывается ТОЛЬКО если needsInput()==true
Page getOutput();                      // страница или null
default ListenableFuture<Void> startMemoryRevoke(); // отзыв revocable memory; future done => отзыв завершён
default void finishMemoryRevoke();
void finish();                         // больше не будет addInput; НЕ вызывается при failure/cancel
boolean isFinished();
default void close();                  // всегда вызывается перед освобождением ссылки
```
**Критическая семантика memory revoke** (javadoc): после `startMemoryRevoke()` Driver **запрещено**
вызывать `isBlocked/needsInput/addInput/getOutput` до `finishMemoryRevoke()`; `finish()` остаётся
разрешённым, `close()` доступен всегда.

### OperatorFactory — `operator/OperatorFactory.java`
`createOperator(DriverContext)`, `noMoreOperators()` (ровно один раз), `duplicate()`
(фабрика клонируется на каждый драйвер пайплайна).

### WorkProcessorOperator (pull) — `operator/WorkProcessorOperator.java`
```java
WorkProcessor<Page> getOutputPages();          // вся логика как WorkProcessor
default Optional<OperatorInfo> getOperatorInfo(); // ДОЛЖЕН быть потокобезопасным
default void close();
```
Помечен `// TODO: support spill` (строка 21) — **pull-операторы пока не поддерживают отзыв памяти**.
Фабрика: `WorkProcessorOperatorFactory.create(OperatorContext, WorkProcessor<Page> sourcePages)`.

### WorkProcessor — `operator/WorkProcessor.java`
- `process()` → true (есть результат/финиш) | false (blocked/yielded); `isBlocked()/getBlockedFuture()`, `getResult()`, `isFinished()`;
- `Transformation.process(@Nullable T)` — null-элемент = «входных данных больше нет»; возвращает `TransformationState`: `needsMoreData() / blocked(future) / yielded() / ofResult(r[, needsMoreData]) / finished()`; `ofResult(r, false)` — результат выдан, но трансформация хочет тот же элемент снова (полный page builder);
- `yielding(BooleanSupplier)` — кооперативный yield с гарантией прогресса;
- **`static <T> WorkProcessor<T> mergeSorted(Iterable<WorkProcessor<T>>, Comparator<T>)`** — уже существующий k-way merge отсортированных потоков.

### Адаптер — `WorkProcessorOperatorAdapter`
Мост push→pull: `addInput` → однослотовый `PageBuffer`; `Factory` реализует **и** `OperatorFactory`,
**и** `JoinOperatorFactory` (пробрасывает `createOuterOperatorFactory()` — так outer-driver-механика
планировщика видит WorkProcessor-фабрику join'а).

## 1.2. Контракты lookup/join-bridge подсистемы

### LookupSource — `join/LookupSource.java` (`@NotThreadSafe`)
«Вторая сторона» join с позиции build:
```java
long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash); // -1 = нет матча
void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result); // bulk
long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage); // цепочка матчей
void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset); // материализация build-строки
boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage); // residual filter
```
Для SMJ-аналога это интерфейс «курсора по отсортированной build-стороне».

### LookupSourceProvider / JoinBridge / LookupSourceFactory
```java
interface LookupSourceProvider { <R> R withLease(Function<LookupSourceLease, R> action); }
interface LookupSourceLease { LookupSource getLookupSource(); long spillEpoch(); IntPredicate getSpillMask(); }
interface JoinBridge {
    OuterPositionIterator getOuterPositionIterator(); // только после finish build+probe (RIGHT/FULL)
    void destroy();
    ListenableFuture<Void> whenBuildFinishes();
}
interface LookupSourceFactory extends JoinBridge {
    ListenableFuture<LookupSourceProvider> createLookupSourceProvider();
    int partitions();
    ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> finishProbeOperator(OptionalInt lookupJoinsCount);
}
```

### JoinBridgeManager<T extends JoinBridge> — связка build/probe/outer пайплайнов
Механизм — **счётчики ссылок** (`ReferenceCount`): probe-refcount стартует с 1 («bootstrap»,
освобождается при первом `freezeProbeFactoriesIfNecessary()`); outer-refcount = 1 при buildOuter.
Вызовы: `incrementProbeFactoryCount()` (в конструкторе фабрики и `duplicate()`),
`probeOperatorCreated/Closed()`, `probeOperatorFactoryClosed()`, `outerOperatorFactory*/outerOperator*()`.
Когда build завершён и все probe/outer закрыты → `whenAllFinishes.addListener(joinBridge::destroy)`
— память build-стороны освобождается детерминированно.

### JoinProbe — итератор probe-страницы
- **nonspilling**: жадно вычисляет все join-позиции страницы в `joinPositionCache` (bulk), RLE-детект;
- **spilling**: ленивый `getCurrentJoinPosition(LookupSource)` на каждую позицию.

### JoinOperatorFactory и сборка пары
```java
public interface JoinOperatorFactory { Optional<OperatorFactory> createOuterOperatorFactory(); }
```
`OperatorFactories.join(...)` / `OperatorFactories.spillingJoin(...)` собирают связку.
Обе `LookupJoinOperatorFactory` реализуют `JoinOperatorFactory + WorkProcessorOperatorFactory`,
инкрементируют probe-refcount в конструкторе и `duplicate()`, зовут `probeOperatorCreated/Closed`
в create/close оператора.

## 1.3. Как build кормит probe (блокировки, futures)

### nonspilling HashBuilderOperator
Состояния `CONSUMING_INPUT → LOOKUP_SOURCE_BUILT → CLOSED`:
- `addInput` → `PagesIndex.addPage` + memory accounting (`CoarseGrainLocalMemoryContext`);
- `finish()` → резерв памяти (при недоне future от `setBytes` — yield через `operatorContext.isWaitingForMemory()`),
  `index.createLookupSourceSupplier(...)` (кодоген `JoinCompiler`),
  `lookupSourceFactory.lendPartitionLookupSource(partitionIndex, ...)`; когда все партиции сданы —
  factory заполняет futures всех probe-операторов;
- **`isBlocked()` в `LOOKUP_SOURCE_BUILT` = future `partitionsNoLongerNeeded`** — build-оператор
  держит driver заблокированным, пока probe не закончил (тогда `destroy()` → future done → build закрывается).

### nonspilling LookupJoinOperator
- `lookupSourceFuture = lookupSourceFactory.createLookupSource()`; при `waitForBuild` —
  `pages.blocking(...)` не тянуть probe-страницы до готовности build (важно для dynamic filters);
- внутри `PageJoiner.process`: неготовность build → `TransformationState.blocked(lookupSourceFuture)`;
- цикл join страницы: `processProbe → joinCurrentPosition → outerJoinCurrentPosition → advanceProbePosition`
  с прерыванием по `yieldSignal.isSet()`/`pageBuilder.isFull()`; RLE-короткое замыкание;
- `close()`: `Closer`, где `afterClose` (release refcount) зарегистрирован первым → выполнится последним.

### spilling-вариант
`SpillingJoinProcessor` — WorkProcessor.Process: (1) waitForBuild-block; (2) join in-memory probe;
(3) `finishProbeOperator` → `PartitionedConsumption`; (4) последовательный проход по заинспилленным
партициям: `partition.load()` → join пролитых probe-страниц + `SavedRow` → `partition.release()`.

## 1.4. Память и статистики

- `OperatorContext.localUserMemoryContext()/localRevocableMemoryContext()` — учёт;
  `LocalMemoryContext.setBytes()` возвращает future при нехватке (блокировка аллокации);
  `isWaitingForMemory()/isWaitingForRevocableMemory()` — оператор обязан на них yield-иться;
  `requestMemoryRevoking()` + `setMemoryRevocationRequestListener` — ревокация (регистрирует Driver).
- `JoinStatisticsCounter` / `JoinOperatorInfo`: лог-гистограмма build-строк на probe-строку
  (8 бакетов), `lookupSourcePositions`, `rleProbes`, `totalProbes`; `mergeWith` — агрегация по
  драйверам. **Новый SMJ-оператор может просто инстанцировать `JoinStatisticsCounter(joinType)`.**

## 1.5. Кодогенерация: что переиспользуемо для non-hash join

- **`PagesHashStrategy`** (`operator/PagesHashStrategy.java`) — кандидат на переиспользование:
  `positionEqualsRowIgnoreNulls`, `positionIdenticalToPosition`, **`compareSortChannelPositions(leftBlockIndex, leftPos, rightBlockIndex, rightPos)`** — сгенерированный типизированный компаратор;
- `PagesIndex.createChannelComparator(leftChannel, rightChannel)` / `createPagesIndexComparator(...)` — готовые компараторы позиций;
- **`JoinFilterFunctionCompiler`** — компиляция остаточного фильтра: контракт
  `JoinFilterFunction.filter(int leftPosition /*build*/, int rightPosition /*probe*/, Page rightPage /*probe-страница*/)`
  — **не привязан к хэшу, полностью переиспользуем для SMJ**;
- `InterpretedHashGenerator/PrecomputedHashGenerator` — для SMJ не нужны, но механика `getPartition` — образец разбиения на партиции при спилле.

## 1.6. LocalExecutionPlanner: точка встраивания SMJ

`visitJoin` (строки ~2539-2560) — **единственная точка выбора стратегии**:
1. регистрация локальных dynamic filters;
2. cross join → `createNestedLoopJoin`;
3. INNER/LEFT/RIGHT/FULL → `createLookupJoin(...)` — здесь добавляется ветка SMJ.

`createLookupJoin` (~2880-3081) — образец сборки:
- probe планируется в основном контексте (`probeNode.accept(this, context)`);
- `buildContext = context.createSubContext()` (общие driverFactories, свои operator-ids/parallelism);
- `partitionCount = buildContext.getDriverInstanceCount().orElse(1)`;
- компиляция residual filter (`compileJoinFilterFunction`), sort-expression context из фильтра;
- вставка `DynamicFilterSourceOperatorFactory` на build при локальных DF;
- выбор spilling/nonspilling реализации;
- **`context.addDriverFactory(false /*не output driver*/, PhysicalOperation(hashBuilderFactory, ..., buildSource), buildContext)`** — build-пайплайн добавляется в ту же задачу;
- join-оператор возвращается как `PhysicalOperation(operator, outputMappings, probeSource)` — встраивается в probe-цепочку;
- для RIGHT/FULL `addLookupOuterDrivers` (:718-756) добавляет третий пайплайн: находит `JoinOperatorFactory`,
  собирает `[outerOperatorFactory] + duplicate(...) всех операторов выше join`.

Связка пайплайнов — **не через страницы, а через shared bridge + futures**; `finish()` между
пайплайнами автоматически не распространяется — согласованность обеспечивает `JoinBridgeManager`.

## 1.7. Driver / SqlTaskExecution: как живут пайплайны одного join

- `Driver.processInternal`: (1) `handleMemoryRevoke`; (2) `processNewSources`; (3) прокачка страниц
  `current.getOutput() → next.addInput(page)`; `current.isFinished()` → `next.finish()` —
  no-more-input распространяется вверх по цепочке; (4) если ничего не двигалось — сбор blocked-futures.
- **Замечание в коде (`Driver.java:383-391`)**: если часть операторов уже закончена, `finish()`
  вызывается на корневом операторе repeatedly — «LookupJoinOperator и HashBuilderOperator требуют
  повторных finish» (ожидание памяти / `partitionsNoLongerNeeded`).
- `SqlTaskExecution`: driverFactories делятся на split-lifecycle (partitioned sources) и
  **task-lifecycle** — build/outer пайплайны join создаются по одному разу на задачу и живут до конца.

## 1.8. Точная последовательность hash join (nonspilling, INNER, без DF)

1. Планирование: `visitJoin` → `createLookupJoin`: probe в главном контексте; build в sub-context;
   `JoinBridgeManager(nonspilling.PartitionedLookupSourceFactory)`; build-фабрика добавлена как driverFactory.
2. Старт задачи: build-драйвер(ы) и probe-драйверы создаются (probe-фабрика делает `probeOperatorCreated`).
3. Build: страницы → `PagesIndex.addPage`; `finish()` → резерв памяти → построение LookupSource
   (кодоген) → `lendPartitionLookupSource` → заполнение `lookupSourceFutures` probe-операторов;
   build → `LOOKUP_SOURCE_BUILT`, `isBlocked() = partitionsNoLongerNeeded`.
4. Probe: адаптер тянет `pages.process()`; неготовность build → `blocked(lookupSourceFuture)`;
   далее на каждую probe-страницу JoinProbe + цикл join с yield-проверками; статистика.
5. Конец probe-входа: `Driver` видит `isFinished()` нижнего оператора → `next.finish()` →
   `PageJoiner.process(null)` флашит остаток; `close()`: release lease + `probeOperatorClosed`.
6. Destroy: все probe/outer закрыты → `joinBridge.destroy()` → `partitionsNoLongerNeeded.set(null)`
   → HashBuilderOperator закрывается, память в 0.

## 1.9. Контракт-чеклист для нового SMJ-оператора

**Структура:**
1. Две фабрики: build-sink (сортировка/буфер) + merge-оператор probe-стороны; для RIGHT/FULL —
   outer-driver через `JoinOperatorFactory.createOuterOperatorFactory()` или собственный эквивалент.
2. Общий мост (аналог `JoinBridge<T>`: future готового отсортированного build + `destroy` +
   `whenBuildFinishes`) + `JoinBridgeManager` с корректным refcount-паттерном:
   `incrementProbeFactoryCount` в конструкторе фабрики и `duplicate()`; `probeOperatorCreated/Closed`
   в create/close; **afterClose-колбэк запускать последним в `close()`**.

**Рантайм-контракты:**
- `addInput` только после `needsInput()==true`; блокировка до build — `TransformationState.blocked(future)`
  / `WorkProcessor.blocking(...)`; поддержка `waitForBuild`;
- yield: регулярные проверки `driverContext.getYieldSignal().isSet()`; полный page builder →
  `ofResult(page, false)`;
- память: `CoarseGrainLocalMemoryContext`-обёртки; при нехватке — ждать future от `setBytes` и
  `operatorContext.isWaitingForMemory()` (yield в `finish`); revocable + `startMemoryRevoke/finishMemoryRevoke`
  — если оператор спиллится (для WorkProcessor-операторов спилл — TODO, поэтому тяжёлую память
  лучше держать на push-стороне);
- `finish()` идемпотентен и может вызываться многократно; раннее завершение probe — build должен
  уметь закрываться по `bridge.isDestroyed()`;
- `close()` в любом состоянии; `getOperatorInfo()` потокобезопасен и зовётся после close;
- выходные страницы: лимиты по размеру/позициям (образец `LookupJoinPageBuilder.isFull()`);
- NULL-семантика equi-join: NULL-ключи не матчатся никогда (guard до сравнения);
- residual filter: переиспользовать `JoinFilterFunction` (`filter(buildPos, probePos, probePage)`);
- планировщик: ветка в `visitJoin` перед `createLookupJoin`, по образцу `createLookupJoin`
  (sub-context, addDriverFactory(false,...), PhysicalOperation на probe).
