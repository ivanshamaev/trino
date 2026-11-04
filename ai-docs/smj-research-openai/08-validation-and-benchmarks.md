# Проверки и программа экспериментов

Ниже — план проверки будущей реализации. В рамках исследования benchmarks не выполнялись, ускорения и потребление памяти не измерялись. Нагрузочные цели должны быть зафиксированы до заявления о готовности к миллиардам строк.

## Существующие базы тестов

| Задача | Откуда взять инфраструктуру/семантический baseline |
|---|---|
| SQL joins и duplicates | [TestJoin](../../core/trino-main/src/test/java/io/trino/sql/query/TestJoin.java), [TestFullJoin](../../core/trino-main/src/test/java/io/trino/sql/query/TestFullJoin.java) |
| Распределённые queries | [TestJoinQueries](../../testing/trino-tests/src/test/java/io/trino/tests/TestJoinQueries.java) |
| Existing spill baseline | [TestSpilledJoinQueries](../../testing/trino-tests/src/test/java/io/trino/tests/TestSpilledJoinQueries.java), [TestJoinQueriesWithForceSpilling](../../testing/trino-tests/src/test/java/io/trino/tests/TestJoinQueriesWithForceSpilling.java) |
| Sorting/merge primitives | [TestMergeSortedPages](../../core/trino-main/src/test/java/io/trino/util/TestMergeSortedPages.java), [TestOrderBy](../../core/trino-main/src/test/java/io/trino/sql/planner/TestOrderBy.java) |
| Join planning | [TestDetermineJoinDistributionType](../../core/trino-main/src/test/java/io/trino/sql/planner/iterative/rule/TestDetermineJoinDistributionType.java), [TestColocatedJoin](../../core/trino-main/src/test/java/io/trino/sql/planner/optimizations/TestColocatedJoin.java) |
| Generated equality/filter | [TestJoinCompiler](../../core/trino-main/src/test/java/io/trino/sql/gen/TestJoinCompiler.java), [TestJoinFilterFunctionCompiler](../../core/trino-main/src/test/java/io/trino/sql/gen/TestJoinFilterFunctionCompiler.java) |
| JMH baseline | [BenchmarkHashBuildAndJoinOperators](../../core/trino-main/src/test/java/io/trino/operator/join/BenchmarkHashBuildAndJoinOperators.java) |

Эти классы — ориентиры, а не утверждение, что их существующий coverage уже проверяет SMJ.

## Последовательность проверок

1. **Kernel differential tests.** Сгенерировать небольшие inputs с фиксированным seed. Сравнить multiset с простым nested-loop oracle и существующим hash path. Сохранять каждый контрпример, включая seed, schema и rows.
2. **Storage tests.** Повторное чтение sealed группы, несколько readers в разрешённых пределах, fan-in overflow, multipass, EOF, corrupt/truncated file, disk quota, отмена записи, cleanup. Reader buffer и manifest memory входят в assertions.
3. **Driver tests.** Принудительные yield/revoke между любыми шагами, blocked futures в разном порядке, downstream finish, failed producer. Проверять отсутствие повторного output и зависших peers.
4. **Planner tests.** Forced SMJ реально выбран; обе стороны имеют совместимые partitions; sort после последнего перемешивания; hash-specific rewrites защищены; JSON roundtrip и EXPLAIN отражают алгоритм; unsupported mode объясняется явно.
5. **Distributed tests.** Несколько workers/tasks/local partitions, одинаковые ключи из разных splits, empty partitions, nested joins, DF cleanup, cancelled query.
6. **FTE tests.** Только после появления поддержки TASK: injection failure во всех фазах и проверка committed multiset/cleanup. Initial unsupported gate также тестируется.
7. **Resource/performance tests.** После correctness — sweep RAM, input size, skew и concurrency; отдельно CPU microbenchmarks и end-to-end latency.

## Минимальная correctness matrix

| Ось | Обязательные случаи |
|---|---|
| Cardinality | Empty/empty, empty/nonempty, 1:1, 1:N, N:M, disjoint ranges |
| Границы | Group через pages/runs/splits; output ровно на page boundary; EOF внутри группы |
| Ключи | Negative/min/max BIGINT; composite; NULL в каждом компоненте; расширение типов отдельными наборами |
| Floating point после включения | NaN, разные NaN encodings, ±0, ±∞ |
| Predicates | Нет residual, TRUE, FALSE, NULL, разные matches внутри одной группы |
| Outer semantics | Preserved row без key match; с key match, но без residual match; частично matched group |
| Encodings | Flat, dictionary, RLE, lazy-loaded blocks; region с большим retained backing |
| Control flow | Yield без output, revoke на границе spill, cancel после публикации manifest, LIMIT |
| План | Flip/copy/prune, remote/local exchanges, больше одной partition, algorithm serialization |

Для типов вне заявленной поддержки правильный результат — deterministic planner rejection/fallback до execution, а не случайный success на одном примере.

## Программа нагрузочных экспериментов

Сначала 10⁶–10⁸ строк для настройки и обнаружения ошибок, затем 10⁹ и 10¹⁰ при достаточном времени/диске. Это предлагаемые размеры испытаний, не обещание доступной ёмкости текущего окружения. Для каждого масштаба известны projected bytes, а не только row count.

| Workload | Что проверяет |
|---|---|
| Огромный probe, малый build | Сохраняется ли преимущество hash/broadcast baseline |
| Оба входа больше RAM, равномерные уникальные ключи | Основная целевая задача external SMJ |
| 1:N и N:M с контролируемым output | Цена replay без неконтролируемого объёма результата |
| Один heavy key, низкая selectivity residual | Память группы и candidate work при малом output |
| Zipf frequencies и разные row widths | Stragglers, skew, retained bytes |
| Уже отсортированные входы | Потенциал будущего sort reuse, с проверкой реального плана |
| Случайные и clustered left blocks | Full merge против возможного partial merge прототипа |
| Несколько joins/queries одновременно | Совместный memory/disk budget и fairness |

Sweep: memory budget от in-memory режима до многопроходного; concurrency `1/4/16` как стартовая сетка; fan-in `4/16/64` при допустимом числе файлов; узкий BIGINT payload и широкие строки, например 32/256/1024 байт логической нагрузки. Эти числа — экспериментальная сетка, не рекомендуемые production defaults. Менять одну ось за раз, затем отдельно проверить взаимодействия.

Baselines: текущий nonspilling hash в области, где он помещается; legacy spilling hash для поддерживаемых joins; существующий FTE hash; SMJ. Сравнение FTE и non-FTE подписывать как разные execution regimes, включая exchange storage overhead. Spark/ClickHouse — отдельное сравнение систем, а не прямой микробенчмарк одного оператора.

Не измерять только `count(*)`, если цель — wide-row materialization: optimizer может удалить payload и существенно изменить работу. Использовать несколько запросов: cardinality-only, контролируемое потребление payload через checksum/aggregate, materialized output в заданный sink. Проверять EXPLAIN и consumed columns. Любой aggregate sink тоже имеет стоимость, её нужно показывать. Избегать вывода миллиардов строк через клиент.

## Метрики

- Wall time, CPU time, throughput input/output rows и bytes; время до первой строки.
- Peak user/revocable memory и фактический process heap/RSS; allocation rate, GC pause/time.
- Sort runs, merge levels, peak open readers/files, manifest bytes.
- Sort spill bytes/read bytes отдельно от group spill/replay bytes и FTE exchange bytes.
- Largest group, candidate pairs, output rows, max/median partition bytes и task duration.
- Network bytes, disk bandwidth/latency, blocked time по memory/I/O/bridge/output.
- Cleanup: оставшиеся temporary bytes/files/reservations после success/failure/cancel.

Инструментирование hot loop не должно добавлять atomic update на каждую пару. Агрегировать счётчики локально с безопасными 64-битными типами; cost instrumentation измерять отдельно.

## Критерии приёмки

**Correctness:** exact multiset equality для заявленной семантики; все planner и lifecycle regression tests проходят. **Memory:** при фиксированных budgets retained memory не растёт пропорционально input или duplicate group; допустимый overhead задокументирован и измерен, включая metadata и минимальную страницу. **Progress:** delayed-input/revoke/cancel tests завершаются; отсутствие output не мешает yield. **Disk:** quota failure контролируем, файлы удаляются. **MPP:** нет случайного global gather, работают multiple local partitions. **FTE:** unsupported gate либо полноценные failure-injection tests.

Универсального performance threshold пока нет. До automatic selection измерить границы выигрыша относительно hash по memory pressure, order reuse и I/O. Публиковать и regressions: особенно малый build, CPU-heavy keys и большой group replay. Нельзя считать проект успешным только потому, что один forced SMJ завершился.

## Воспроизводимость и команды

Следовать [Building и Running benchmarks](../../.github/DEVELOPMENT.md#building). Для будущих Java edits обязателен `mvnd airstyle:format`. После сборки зависимостей пример запуска существующего targeted test:

```bash
mvnd test -Dair.check.skip-all=true -pl core/trino-main -Dtest=TestMergeSortedPages
```

Шаблон JMH для existing baseline из руководства проекта:

```bash
mvnd test-compile exec:exec -Dair.check.skip-all=true -Dmaven.compiler.proc=full \
    -pl core/trino-main -Dexec.classpathScope=test -Dexec.executable=java \
    -Dexec.args="-cp %classpath io.trino.operator.join.BenchmarkHashBuildAndJoinOperators"
```

Эти команды здесь не запускались. Новые классы тестов сначала должны быть реализованы; документация не выдаёт их за существующие. Run manifest эксперимента должен хранить commit, JVM/JDK flags, hardware, RAM/disk/network, worker count, session/config properties, SQL/EXPLAIN, seed, input statistics, число warmups/repetitions и сырые метрики. Cold/warm cache результаты разделяются.
