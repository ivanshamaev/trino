# Память, legacy spill и fault-tolerant execution

## Почему число строк не является пределом

Для hash join определяющим является build после projection/filter и распределения. Приближённая модель:

```text
M_hash_task ≈ retained(build_task) + hash arrays + duplicate links
            + probe/output/exchange buffers + transient allocations

M_unspill_partition ≈ retained(build_partition) + rebuilt lookup
                     + transient read/index buffers
```

Это модель оценки, не точная формула Trino. Сжатые байты Parquet на диске не равны `retained(build)`. При равномерном распределении build делится между tasks и локальными partitions, но один heavy key может нарушить предположение о равномерности. Миллиарды probe rows против малого build могут обрабатываться потоково. Десятки миллионов широких build rows могут оказаться тяжелее миллиардов узких probe rows.

## Что реально делает spill

[Документация spill](../../docs/src/main/sphinx/admin/spill.md) прямо называет его legacy functionality и рекомендует рассмотреть FTE с task retries. Она также предупреждает, что spill не гарантирует успех при слишком большой минимальной порции unspill.

В [spilling.HashBuilderOperator](../../core/trino-main/src/main/java/io/trino/operator/join/spilling/HashBuilderOperator.java) есть состояния `CONSUMING_INPUT`, `SPILLING_INPUT`, `LOOKUP_SOURCE_BUILT`, `INPUT_SPILLED`, `INPUT_UNSPILLING`, `INPUT_UNSPILLED_AND_BUILT`, `CLOSED`. Revocation может выгрузить ещё собираемый build либо готовый lookup. Probe для соответствующих partitions тоже сохраняется; [SpillingJoinProcessor](../../core/trino-main/src/main/java/io/trino/operator/join/spilling/SpillingJoinProcessor.java) координирует дальнейшую обработку.

Критическая деталь: `unspillLookupSourceIfRequested` вызывает `getAllSpilledPages`, а `finishLookupSourceUnspilling` возвращает страницы в index и заново строит lookup source. **Одна восстанавливаемая build-partition должна поместиться в память вместе с индексами и overhead.** Этот путь не является рекурсивным Grace hash join, который автоматически делит любую слишком большую partition до заданного бюджета. Рост `task.concurrency` помогает только до пределов распределения и при отсутствии патологического skew.

В [LocalExecutionPlanner](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java), `createLookupJoin`, реальный spill разрешается при `isSpillEnabled`, spillable node и отсутствии build-outer semantics. RIGHT/FULL исключены из этого разрешения; factory дополнительно требует `partitionCount > 1`. Поэтому нельзя написать «все joins умеют spill», просто сославшись на наличие класса `spilling.LookupJoinOperator`.

## FTE решает другую задачу

[Fault-tolerant execution](../../docs/src/main/sphinx/admin/fault-tolerant-execution.md) с `retry_policy=TASK` сохраняет межстадийные exchanges, позволяет повторять tasks и управлять планированием по memory estimates. [EventDrivenFaultTolerantQueryScheduler](../../core/trino-main/src/main/java/io/trino/execution/scheduler/faulttolerant/EventDrivenFaultTolerantQueryScheduler.java) и [адаптивная оптимизация](../../docs/src/main/sphinx/optimizer/adaptive-plan-optimizations.md) — отдельный слой от operator spill.

| Свойство | Local operator spill | FTE exchange storage |
|---|---|---|
| Единица данных | Состояние оператора/partition/run | Передача между stages/tasks |
| Основная цель | Освободить RAM внутри task | Повторное выполнение и развязка stages |
| Жизненный цикл | Локальная попытка task | Хранилище exchange и учёт попыток |
| Обеспечивает bounded join memory сам по себе | Нет: hash unspill восстанавливает partition | Нет: task всё равно исполняет operator |
| Будущий SMJ | Sort runs и group replay | Повторить task из входного exchange |

FTE позволяет планировать работу с большим числом partitions и увеличивать memory request при повторе после OOM, но память узла конечна; обычное hash repartitioning не разделяет один одинаковый ключ. Это не обещание рекурсивного дробления failed JOIN partition: в [HashDistributionSplitAssigner](../../core/trino-main/src/main/java/io/trino/execution/scheduler/faulttolerant/HashDistributionSplitAssigner.java) production `canSplit` ограничен scale writers. SMJ должен проектироваться совместимым с task retries, а не объявляться заменой FTE. В первом прототипе поддержку TASK можно явно отключить, пока не проверены lifecycle и disk management.

Ещё одна граница reuse: [SystemSessionProperties.isDistributedSortEnabled](../../core/trino-main/src/main/java/io/trino/SystemSessionProperties.java) явно возвращает false при `RetryPolicy.TASK`, поскольку существующая distributed sort topology требует одновременного исполнения stages. Это ограничение текущей реализации, а не доказательство невозможности partition-local external sort при FTE.

## Что мешает объявить существующий sort готовым решением

[OrderByOperator](../../core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java) умеет сортировать `PagesIndex` и spill sorted runs. Но `mergeSpilledAndMemoryPages` передаёт все runs в [MergeSortedPages](../../core/trino-main/src/main/java/io/trino/util/MergeSortedPages.java). Это reusable merge primitive, а не явный bounded-fan-in multipass sorter. При росте числа runs растёт количество одновременно поддерживаемых входов и reader state.

Дополнительный audit обнаружил bounded multipass в другом месте: Hive `SortingFileWriter.combineFiles`, используемый также Iceberg sorted writer. Его нельзя объявить готовым query sorter: текущий merge синхронный, memory accounting и metadata bounds требуют доработки. Подробности и план reuse — в [13](13-java-sorting-algorithms-and-code.md).

[FileSingleStreamSpiller](../../core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java), `getSpilledPages`, разрешает чтение только один раз. Значит, repeated scan одной группы совпадений требует другого storage contract. Нельзя заменить replay вызовом того же метода второй раз. [PagesIndex](../../core/trino-main/src/main/java/io/trino/operator/PagesIndex.java), `addPage`, также имеет ограничение числа позиций на один index: большой input должен разбиваться на bounded runs, а не накапливаться в одном index.

Вывод: ценность SMJ — снять зависимость минимальной рабочей памяти от всей build-partition. Для этого память должна ограничиваться run buffers, merge fan-in, страницами входов, output и group replay. Временный диск и время исполнения всё равно растут с данными. Лимит памяти не может быть меньше минимально обрабатываемой страницы/строки и служебных буферов; для сверхширокой строки нужен явный отказ либо отдельный контракт.
