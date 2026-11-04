# Brainstorm: Java developer

Независимый анализ локального кода `485-SNAPSHOT`, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Роль: реализация оператора и его жизненный цикл. Это проектные предложения, а не описание существующего SMJ. Запрещённые каталоги предыдущих исследований не использовались.

## Что уже есть в коде

| Компонент | Проверенный контракт | Следствие для SMJ |
|---|---|---|
| [`Operator`](../../../core/trino-main/src/main/java/io/trino/operator/Operator.java), `addInput(Page)`, `needsInput()` | Один вход без номера порта; `getOutput()` возвращает страницу или `null` | Просто добавить два `addInput` невозможно без изменения интеграции с Driver |
| [`Driver`](../../../core/trino-main/src/main/java/io/trino/operator/Driver.java), `processInternal`, `getBlockedFuture` | Двигает страницы между соседними операторами линейного pipeline; учитывает блокировку оператора, user/revocable memory и memory revoke | Два входа надо связать через отдельные producer pipelines и bridge/queues либо через один обычный вход и второй bridge |
| [`WorkProcessor`](../../../core/trino-main/src/main/java/io/trino/operator/WorkProcessor.java), `ProcessState` | Явные состояния `ofResult`, `blocked`, `yielded`, `finished` | Подходит для resumable merge kernel, но сам не создаёт двухвходовую модель исполнения |
| [`WorkProcessorOperator`](../../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorOperator.java) | В интерфейсе стоит `TODO: support spill` | Нельзя обещать revocable spilling только потому, что ядро написано на WorkProcessor |
| [`WorkProcessorOperatorAdapter`](../../../core/trino-main/src/main/java/io/trino/operator/WorkProcessorOperatorAdapter.java) | Использует один `PageBuffer`, не переопределяет `startMemoryRevoke`/`finishMemoryRevoke` | Нужен собственный `Operator` shell для spill-aware SMJ или отдельное расширение адаптера |
| [`PagesIndex`](../../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java), `addPage`, `sort`, `getSortedPages` | Хранит column blocks и `long[]` адресов строк; не внешний индекс; запрещает размер от 2 млрд записей | Использовать как ограниченный run buffer, а не для всей партиции или всего equal-key group |
| [`PagesIndexOrdering`](../../../core/trino-main/src/main/java/io/trino/operator/PagesIndexOrdering.java), `sort` | Сортирует адреса tuned quicksort | Переиспользовать в первом прототипе; стабильность порядка равных ключей не гарантируется и не нужна обычному SQL JOIN |
| [`OrderByOperator`](../../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java), `spillToDisk`, `mergeSpilledAndMemoryPages` | Сортирует runs, spill по revoke, затем объединяет все spilled streams и остаток в памяти | Есть строительные блоки external sort; нет готового контракта ограниченного fan-in merge для произвольного числа runs |
| [`MergeSortedPages`](../../../core/trino-main/src/main/java/io/trino/util/MergeSortedPages.java), `mergeSortedPages` | K-way merge уже отсортированных streams, `PageBuilder`, memory contexts, yield | Это merge сортировки, не relational join: не делает декартово произведение одинаковых ключей, residual predicates и outer rows |
| [`LocalMergeSourceOperator`](../../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalMergeSourceOperator.java) | Source-подобный `Operator` с `needsInput=false`, получает несколько streams из `LocalExchangeSource` | Подтверждает применимость source-подобного shell, который сам читает больше одного stream |

Дополнительный важный нюанс: в `OrderByOperator.finish()` при неудачной конвертации revocable memory в user memory есть синхронный `getFutureValue(spillToDisk())` с TODO сделать асинхронным. В `Spiller.getSpills()` выдаются обычные итераторы, поэтому обёртка `WorkProcessor.fromIterator` сама по себе не делает дисковое чтение асинхронным. Новый kernel должен честно различать доступный шаблон и необходимое улучшение I/O.

## Три варианта интеграции

### A. Один обычный вход + stream bridge второй стороны

Два сортирующих pipeline. Первый подключается через обычный `addInput`, второй через sink и ограниченный bridge. Merge operator находится в первом pipeline.

Плюсы: не меняет `Driver`; привычное построение execution plan. Минусы: асимметричный backpressure и опасность в `isBlocked`: блокировка всего оператора отключает также его `needsInput/addInput`, поэтому нельзя блокироваться на событии, разрешение которого требует продвижения первого входа. Для каждого состояния надо определить, чья страница нужна для следующего шага.

### B. Два sink + один merge source — рекомендую для прототипа

Обе стороны после обменов и сортировки публикуют страницы в отдельные ограниченные очереди одной локальной join-партиции. SMJ — source-подобный `Operator`, читает обе очереди и выдаёт результат downstream. `Driver` остаётся линейным.

Плюсы: симметричные ownership, completion и backpressure; внутренний код не зависит от push-семантики одного из входов. Минусы: дополнительный pipeline и буферизация, явная привязка двух producer streams к одному consumer, необходима проверка scheduler/dependency wiring. Это не готовая возможность `LocalMergeSourceOperator`: новый двухсторонний bridge и локальный planner всё равно нужны.

Нельзя разрешать произвольным driver экземплярам забирать произвольные куски одного sorted partition: каждый consumer должен получить полную согласованную последовательность своей левой и правой партиции. Если несколько sorted producers попадают в одну сторону, перед SMJ нужен корректный k-way merge этой стороны.

### C. Новый binary `Operator` и изменение `Driver`

Самый прямой абстрактный интерфейс, но большой объём изменений scheduler, memory revoke, pipeline fusion и тестов существующих операторов. Для первой реализации цена выше выигрыша. Отложить до появления нескольких операторов, которым нужен тот же контракт.

## Предлагаемые контракты, которых пока нет

Имена ниже проектные:

```text
SortedInput.poll() -> PAGE | BLOCKED(future) | FINISHED
SortedInput.close(cause)

RewindableGroup.append(page slice) -> READY | BLOCKED(writeFuture)
RewindableGroup.seal() -> READY | BLOCKED(writeFuture)
RewindableGroup.openCursor() -> replay cursor
RewindableGroup.close()

MergeJoinKernel.process(workBudget) -> PAGE | BLOCKED | YIELDED | FINISHED
```

`SortedInput` обязан гарантировать monotonic key order в пределах одной партиции, единые типы/направления/null ordering и явное отсутствие новых страниц после FINISHED. Это контракт planner + operator; сортировка файла в storage сама по себе его не доказывает.

`RewindableGroup` обязан ограничивать retained memory, поддерживать повторное чтение spill, учитывать bytes/readers и удалять файлы при ошибке/отмене. Текущий [`Spiller`](../../../core/trino-main/src/main/java/io/trino/spiller/Spiller.java) обещает список spill streams, но не документирует произвольное повторное открытие одного run; такой контракт нельзя предположить.

SMJ bridge не стоит бездумно наследовать от [`JoinBridge`](../../../core/trino-main/src/main/java/io/trino/operator/join/JoinBridge.java): тот требует `whenBuildFinishes()` и `getOuterPositionIterator()`. [`JoinBridgeManager`](../../../core/trino-main/src/main/java/io/trino/operator/join/JoinBridgeManager.java) тоже связывает cleanup с build/probe/outer lifecycle. Для streaming SMJ стороны должны прогрессировать до завершения build; полезен общий принцип reference counting, а не буквальное reuse всех hash lifecycle методов.

## Kernel и equal-key groups

Для INNER equi-join с повторениями недостаточно держать по одной строке. Для ключа k нужно выдать `|L_k| × |R_k|` пар; residual predicate применяется к каждой кандидатной паре. Ограничение памяти достигается replay, а не исчезновением этого объёма работы.

Простейший корректный алгоритм: сохранить одну правую equal-key group в `RewindableGroup`, затем для каждой левой строки этой группы перечитать её. Большая правая группа spill-ится; большой левый поток остаётся потоковым. Это bounded memory, но при большой левой группе возможен многократный дисковый reread. Следующий этап — блокировать несколько левых строк в пределах бюджета и переиспользовать одно чтение правой группы для блока. Выходный порядок внутри равных ключей можно менять, если planner не обещает более сильного свойства.

```text
ALIGN:
  получить доступные головы L и R, сохраняя позиции при BLOCKED/YIELD
  если одна сторона исчерпана: FINISH для INNER
  если null join key: пропустить по SQL semantics обычного '='
  если L.key < R.key: advance L
  если L.key > R.key: advance R
  иначе: запомнить key; перейти в CAPTURE_RIGHT_GROUP

CAPTURE_RIGHT_GROUP:
  append R, пока key тот же; граница может пересекать Page
  при spill pause -> BLOCKED; не терять head следующей группы
  seal group; открыть replay cursor; перейти в EMIT_MATCHES

EMIT_MATCHES:
  продолжать пары (current L, next replay R)
  predicate TRUE -> append output; FALSE/NULL -> не append
  заполнена output Page -> отдать страницу, сохранив оба курсора
  replay закончился -> advance L
  L имеет тот же key -> новый replay cursor
  иначе -> close group; перейти в ALIGN
```

Это намеренно INNER-псевдокод. LEFT требует учёта `leftMatched`, причём совпадение ключа ещё не означает match после residual predicate. RIGHT/FULL дополнительно требуют match state для правых строк и выдачи orphan rows. Bitmap всех строк огромной правой группы тоже нарушает bounded-memory обещание: нужен spillable/segmented match state либо иной алгоритм обработки групп. SEMI/ANTI могут экономить работу, но это отдельные SQL semantics и plan nodes; включать их автоматически нельзя.

Сравнение сортировки не следует приравнивать к SQL equality без доказательства для поддерживаемых типов. NULL, floating point и составные ключи нужно проверить на тех же type operators, которыми пользуется Trino. В MVP разумно начать с явно проверенного набора orderable типов и обычного `=`; null-safe equality должна быть отдельным решением planner и kernel.

## Память, yield и отмена

Верхняя оценка для одного consumer:

```text
M ≈ leftHead + rightHead + outputBuilder + groupBuffer
    + replayReadBuffer + queueReservations + runMergeHeads + metadata
```

Нельзя называть это O(1) памятью без фиксированных лимитов queues, groupBuffer и merge fan-in. Для `r` открытых runs требуются головы и I/O buffers порядка `r`, поэтому внешний sorter требует многоходового merge с ограниченным fan-in при большом `r`.

Практические требования:

1. Не держать region/dictionary страницы целиком ради одной текущей строки без учёта retained bytes; key bookmark должен иметь ясное владение памятью.
2. Бюджетировать сумму двух sorts, queues и join, а не выдавать каждому одинаковый независимый лимит, превышающий память task.
3. Проверять yield не только при выдаче результата: миллионы несовпадающих строк или residual FALSE могут долго не заполнять output.
4. Возвращать future только события, которое действительно необходимо для прогресса. Не ждать завершения обеих сторон, если одна уже позволяет двигаться; не spin на завершённом future.
5. При `startMemoryRevoke` фиксировать cursor state, остановить обращение к переданным writer pages, вернуть future. Менять `OperatorContext` после завершения на driver thread в `finishMemoryRevoke`; интерфейс явно запрещает его изменение из spawned threads.
6. `finish()` означает завершение input, а не закрытие всех ресурсов. `close()` может прийти во время spill и обязан закрыть producers/readers/files и разбудить заблокированных peers без двойного освобождения memory reservation.
7. Не оставлять источник ожидающим backpressure после downstream LIMIT/отмены; early finish одного consumer должен распространяться в обе стороны.

## Минимальный безопасный MVP

Рекомендую сначала локальный kernel INNER equi-join, входные streams уже отсортированы, обе стороны с повторами, bounded spillable right group, residual filter, пустые страницы и отмена. Затем source-shell с двумя очередями и принудительный planner path; полноценный CBO и использование storage ordering позже.

Reuse `OrderByOperator` подходит для функционального прототипа. Претензия на десятки миллиардов строк требует отдельной проверки fan-in, merge output memory, blocking I/O и долгого non-yielding sort. Обещать production memory robustness только по факту вызова существующей external sort нельзя.

Существующие отправные точки для тестов: [`TestOrderByOperator`](../../../core/trino-main/src/test/java/io/trino/operator/TestOrderByOperator.java), [`TestMergeSortedPages`](../../../core/trino-main/src/test/java/io/trino/util/TestMergeSortedPages.java), [`TestDriver`](../../../core/trino-main/src/test/java/io/trino/operator/TestDriver.java), [`TestWorkProcessor`](../../../core/trino-main/src/test/java/io/trino/operator/TestWorkProcessor.java). Новые обязательные сценарии: equal group на границах страниц, тысячи tiny runs, spill во время группы, residual FALSE для всех кандидатов, input blocked в каждой фазе, output backpressure, memory revoke без текущего revocable allocation, cancellation в write/read, skew с группой больше бюджета.

## Вопросы для cross-review

- Принимаем ли source-shell с двумя очередями или предпочитаем один вход + bridge ради меньшего изменения planner? Требуется доказательство отсутствия scheduler starvation для обоих вариантов.
- Кто владеет общей reservation двух sorters и join? Независимые локальные лимиты не дают гарантии для task.
- Входит ли bounded fan-in sorter в MVP для больших данных или честно выделяется отдельным milestone до production?
- Согласны ли отложить RIGHT/FULL до spillable right-match tracking? Иначе MVP обещает SQL покрытие, которого bounded-memory kernel не обеспечивает.
- Допускаем ли медленный, но ограниченный по памяти replay больших duplicate groups первым шагом? Бенчмарк обязан показывать reread bytes и output cardinality, а не только elapsed time.
