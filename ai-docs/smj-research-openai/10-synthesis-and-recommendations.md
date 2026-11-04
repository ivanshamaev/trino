# Синтез и рекомендации

**Рекомендация: разрабатывать native partitioned external sort-merge join как дополнительную стратегию Trino.** Первый measurable результат — корректный equijoin, у которого рабочая память ограничена при росте размера каждой input partition и duplicate group. Ускорение относительно hash не следует обещать до измерений.

Исследование относится к Trino `485-SNAPSHOT`, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Выводы основаны на чтении кода, официальных источников других движков и brainstorm пяти агентов. Реализация и benchmarks ещё не выполнялись.

Дополнение по запросу о неотсортированных **15 млрд JOIN 17 млрд**: [пошаговый сценарий и расчёты](14-case-15b-17b-unsorted.md), [несовпадающие partition/bucket keys](12-layout-bucketing-and-repartitioning.md), [Java-алгоритмы и их наличие в Trino](13-java-sorting-algorithms-and-code.md), [проект sorter и проверки](15-bounded-java-sort-design-and-experiments.md). Velox исключён из рассмотрения.

Новые выводы: составное bucketing по `(a,b)` обычно нельзя сохранить для JOIN только по `a`; требуется совместимое repartitioning. Для 32 млрд rows при условных 64 projected bytes/row вход составляет 2,048 TB; при 512 равномерных pairs, 256 MiB run buffers и fan-in 16 модель даёт около 4,096 TB sort temporary read+write без scan/shuffle/output. Это аналитический пример, не измерение. Также обнаружен уже существующий Java bounded multipass в Hive `SortingFileWriter`, переиспользуемый Iceberg: он дополняет reuse-кандидаты, но требует нового query lifecycle и memory accounting.

## Что установлено

1. **General-purpose SMJ в исследованном execution path отсутствует.** Обычные JoinNode направляются в lookup hash path; cross, index, spatial и semi-related joins имеют отдельные пути. `SortedPositionLinks` ускоряет часть поиска внутри hash structure и не является SMJ. См. [02](02-current-joins.md).
2. **Отсутствие SMJ не делает миллиарды строк невозможными.** Малый build/большой probe, распределённый hash, legacy spill и FTE уже позволяют работать с большими данными. Проблема — build footprint, его максимальная partition, skew, число concurrent tasks и память промежуточных структур. См. [03](03-memory-spill-and-fte.md).
3. **Legacy spill сохраняет границу размера восстанавливаемой partition.** HashBuilder unspill загружает страницы и перестраивает lookup. RIGHT/FULL не входят в обычное разрешение spill этого planner path. Это конкретная зона потенциального выигрыша внешнего merge, но FULL потребует отдельной разработки.
4. **У Trino есть sorting primitives, но нет готового полного bounded SMJ substrate.** Нужны query-compatible multipass fan-in, replayable group store и ограничение metadata; существующий single-stream spiller не допускает повторного чтения. Multipass policy уже есть в connector writer и может быть обобщена. См. [04](04-sort-merge-algorithms.md) и [13](13-java-sorting-algorithms-and-code.md).
5. **Главный integration risk — physical properties и lifecycle.** Необходимо парное remote/local partitioning; обычный SortNode может вызвать gather; поздние optimizer rules могут разрушить новый contract; dynamic filter waits и scheduler должны сохранять progress. См. [06](06-proposed-architecture.md).
6. **SQL equality нельзя заменить comparator=0.** В этом checkout NaN является конкретным контрпримером. Outer matched flags зависят от equality и residual predicate. См. [07](07-correctness-and-lifecycle.md).

## Рекомендуемый первый дизайн

```text
JoinNode с явным algorithm и проверяемыми requirements
  -> согласованный hash repartition обеих сторон
  -> partition-local bounded external run sort
  -> manifests завершённых runs, принадлежащих task attempt
  -> source-like merge operator с replayable duplicate groups
  -> обычные Page/Block output и существующий task lifecycle
```

Это проектная схема; названные новые контракты в Trino ещё не реализованы. Merge kernel использует resumable state machine, а собственный Operator shell обеспечивает blocked/yield/revoke/close. Driver interface не меняется. Последний merge runs может быть fused с join, если соблюдены budgets; запись одного полного sorted file не обязательна.

Для первой интеграции предпочтено поле algorithm в JoinNode с audit всех copy/flip/rules и final validator. Отдельный physical node остаётся обоснованной альтернативой при готовности обновить visitor/serialization/DF/scheduler surface. Аргументы обеих сторон сохранены в [brainstorm](09-five-agent-brainstorm.md), без искусственного единогласия.

## Последовательность разработки и критерии перехода

| Этап | Результат | Условие завершения |
|---|---|---|
| 0. Контракт и oracle | Ordinary BIGINT INNER, duplicates, residual, sorted cursors | Differential multiset tests, включая NULL и границы страниц |
| 1. External primitives | Bounded sorter, multipass, replayable group store | Memory bound при большом числе runs/большой группе; quota/cancel cleanup |
| 2. Local integration | Producers, manifests, merge source и paired IDs | Driver yield/revoke/blocked tests; нет leaks/deadlocks |
| 3. Distributed forced SMJ | Algorithm selection, exchanges, guards, validator, EXPLAIN | Несколько workers/local partitions, реальные планы без случайного gather |
| 4. Расширение semantics | LEFT, дополнительные типы; затем RIGHT/FULL отдельными задачами | SQL differential matrix и bounded matched-state для FULL |
| 5. FTE | Attempt-owned runs, retry из exchange, adaptive guards | Failure injection во всех фазах, корректная публикация результата |
| 6. Производительность | Базовые workload sweeps и bottleneck profiles | Воспроизводимые сравнительные данные, включая regressions |
| 7. Automatic choice и reuse | Cost model, proven ordering reuse, позднее streaming fast path | Проверенные eligibility/ordering proofs и границы выбора стратегии |

Этапы — зависимые deliverables, а не календарная оценка. Объём работы нельзя честно свести к одному operator class или заранее обещать срок без prototype.

На этапах 0–3 нельзя заявлять поддержку всех joins, всех типов или TASK retries. Forced unsupported режим должен сообщать причину; automatic mode сохраняет существующий допустимый план. Dynamic filters выбранного SMJ сначала согласованно удаляются producer/consumer pass-ом; поддержка их сборки добавляется отдельно.

## Почему не другие первые варианты

- **Глобально сортировать две таблицы и соединить на одном worker:** подходит для малого correctness prototype, но создаёт MPP bottleneck.
- **Добавить только merge-loop над OrderBy:** не решает paired partitions, group replay, fan-in и scheduler.
- **Partial merge как универсальный первый алгоритм:** возможен более привычный build/probe lifecycle, но repeated range reads могут доминировать на случайном input.
- **Сразу два streaming inputs и новый Driver:** расширяет integration surface до подтверждения пользы базового алгоритма.
- **Автоматически заменять каждый большой hash на SMJ:** size estimates не дают полной картины sorting, skew, output и I/O.
- **Считать FTE или spill готовым group storage:** у них другие lifecycle и reader contracts.

## Условия, при которых SMJ наиболее перспективен

Это гипотезы для проверки, не измеренные результаты: обе стороны после pruning существенно превышают бюджет build; есть эффективно работающий temporary storage; output не доминирует над всем запросом; ключи можно сортировать разумной ценой; либо входной порядок можно доказанно использовать повторно.

Hash ожидаемо остаётся сильным baseline при маленьком build, broadcast, fit-in-memory и полезных dynamic filters. Современный external hash также необходимо учитывать: DuckDB показывает самостоятельное направление larger-than-memory processing без обязательной сортировки. Сравнение источников — [05](05-other-engines.md).

## Главные риски и способы проверить их

| Риск | Проверка/мера |
|---|---|
| Тихая потеря matches из-за неверных partitions/order | Final validator, differential distributed tests, runtime assertions в debug |
| OOM в огромной группе или metadata | Spillable group/bitmap, bounded manifests/readers, memory sweep |
| Deadlock новой topology | Delayed branch, tiny budgets, nested joins, DF/LIMIT/revoke tests |
| I/O amplification | Отдельные sort/group replay метрики, tile replay, bounded fan-in tuning |
| Один hot key остаётся straggler | Heavy-key tests и диагностика; parallel skew protocol позднее |
| Ошибки FULL/NULL/NaN | Узкая первоначальная eligibility и расширение по semantic tests |
| Retry даёт повторный output/утечки | Attempt ownership и failure injection под TASK |
| Некорректное обещание ускорения | End-to-end baselines с тем же payload, режимом execution и ресурсами |

## Какие данные ещё нужны от реальной нагрузки

Для выбора thresholds понадобятся: schemas/types и ширина projected rows; build/probe sizes после фильтров; NDV и top-key frequencies; виды join/residual; memory limit на worker/task; concurrent queries; local disk throughput/capacity; remote exchange storage; требуемые latency и completion rate. Получение этих данных не блокирует подготовленный architecture prototype, но блокирует обоснованный production auto-selection.

Практический следующий шаг — этапы 0 и 1 как отдельные небольшие изменения: kernel/oracle и external storage primitives. Они быстро проверят самую важную гипотезу — можно ли получить корректный и управляемый по памяти join при input/group значительно больше RAM — до вмешательства во весь распределённый planner.
