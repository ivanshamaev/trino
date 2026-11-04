# Неотсортированные данные: partitioning, bucketing и ключ JOIN

Дополнение к исследованию для случая **15 млрд строк JOIN 17 млрд строк**. Рассматривается реализация в Java внутри Trino; Velox не рассматривается. База кода прежняя: `485-SNAPSHOT`, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Существующие механизмы ниже отделены от предлагаемого SMJ, которого в этом checkout нет.

**Главный ответ:** несовпадение storage layout с ключом JOIN не препятствует SMJ. Оно означает, что перед сортировкой нужно доставить все потенциально совпадающие строки в соответствующие join instances. В общем случае это shuffle обоих входов по ключу JOIN, затем внешняя сортировка каждой полученной партиции. Переписывать исходные таблицы не требуется.

## Четыре разных понятия

| Понятие | Пример | Что гарантирует | Чего не гарантирует |
|---|---|---|---|
| Storage partition | `day=2026-10-01/region=eu` | Группировку файлов и возможность pruning по этим значениям | Что все строки одного customer попадут на один worker |
| Storage bucket | `bucket(customer_id, 256)` | Детерминированную bucket-функцию при корректных metadata/data | Сортировку customer внутри bucket; автоматическую совместимость с другой таблицей |
| Execution partition | Hash exchange по `customer_id` | Доставку equal keys в один destination при корректном mapping | Порядок строк внутри destination |
| Ordering | `customer_id ASC, event_time ASC` | Монотонный порядок в указанной области | Совместимое распределение двух сторон и порядок других потоков |

Storage partition, remote execution partition и local driver partition не являются взаимозаменяемыми. Число files/splits также не равно числу join partitions. Один logical bucket может состоять из файлов многих дат, и один task может читать множество splits.

## Формальное условие корректной доставки

Для обычного equijoin необходимо:

```text
joinEquality(l.key, r.key) = TRUE
    => destinationLeft(l) = destinationRight(r)
```

В destination включены task и локальный merge instance. При базовом partitioned исполнении каждая строка должна иметь одного владельца; replication/skew handling требует более сложного протокола. Сортировка применяется **после** обеспечения этого условия. Сортировка по нужному ключу внутри неправильных buckets не восстанавливает недостающие пары между buckets.

Если execution распределение вычисляется по набору переменных `P`, а equality JOIN использует `J`, достаточное условие для colocation внутри стороны — `P ⊆ J` после учёта доказанных constants и соответствий symbols. Направление включения принципиально:

- partitioned by `a`, JOIN by `(a,b)` — одинаковая пара `(a,b)` обязательно имеет одинаковое `a`;
- partitioned by `(a,b)`, JOIN by `a` — одинаковое `a` может иметь разные `b` и оказаться в разных destinations.

Это условие не доказывает совместимость **между** сторонами: функции, types/coercions, bucket counts, seed/version и bucket-to-task mappings тоже должны быть согласованы. Storage directory layout сам по себе ещё не является advertised execution partitioning.

## Матрица решений

| Фактическое распределение после scan | JOIN | Нужный шаг |
|---|---|---|
| Обе стороны совместимо bucketed по `a`, порядок отсутствует | `L.a=R.a` | Возможно избежать remote shuffle; sort обеих сторон внутри полных matching partitions |
| Только одна сторона совместимо распределена по `a` | `L.a=R.a` | Возможно shuffle только другой стороны в её mapping; обе стороны всё равно сортировать |
| Обе стороны hash-partitioned по `(a,b)` | `L.a=R.a` | Обычно repartition по `a`; одинаковое число старых buckets не помогает |
| Одна сторона по `a`, другая по `(a,b)` | `L.a=R.a` | Можно сохранить первую и shuffle вторую, если planner может реализовать соответствующий mapping |
| Обе стороны по `a` | `L.a=R.a AND L.b=R.b` | Colocation может быть достаточна; sort по полному согласованному ключу `(a,b)` |
| Обе стороны по `(a,b)` | `L.c=R.c` | Обычно shuffle обеих по `c`, затем sort по `c` |
| Storage partitioned by `(day,region)`, JOIN customer | `L.customer=R.customer` | Prune допустимые даты/регионы, затем обеспечить распределение customer через все выбранные storage partitions |
| Sort order `(a,b)`, JOIN only `b` | `L.b=R.b` | Общего порядка `b` нет; требуется новая сортировка после корректного распределения |
| Sort order `(a,b)`, JOIN only `a` | `L.a=R.a` | Порядок `a` является prefix, но reuse требует сохранения этого порядка всем execution path |
| Несколько файлов каждый sorted по `a` | `L.a=R.a` | Нужен merge файлов или полный sort; произвольная конкатенация не sorted |

Здесь «возможно» означает необходимость proof со стороны planner/connector, а не обещание существующей оптимизации для любого storage format.

### Контрпример для составного бакетирования

Пусть для иллюстрации `bucket(a,b)=(31*a+b) mod 4` — это **учебная функция, не Hive hash**. Строки `L=(a=7,b=0)` и `R=(a=7,b=1)` имеют buckets 1 и 2. JOIN по `a` обязан их соединить. Если соединять только старые buckets с одинаковым номером, пара потеряется. Сортировка обоих buckets по `a` не исправит ошибку. Repartition по `a` доставляет обе строки одному consumer.

Если фильтры доказывают `b=constant`, составное распределение становится функцией `a` и константы. Но разные constants слева и справа или разные bucket functions всё ещё могут дать разные destinations. Нельзя просто удалить `b` из текста metadata. Аналогично функциональная зависимость может быть математически достаточной, но её наличие и использование optimizer-ом должны быть доказаны, а не выведены из приблизительной статистики.

## Что уже делает Trino

[Partitioning.isPartitionedOn](../../core/trino-main/src/main/java/io/trino/sql/planner/Partitioning.java) реализует проверку включения partition arguments в требуемые columns, игнорируя известные constants. `isPartitionedOnExactly` строже; выбор exact mode проходит через [ActualProperties](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/ActualProperties.java) и session policy. Поэтому математически допустимое reuse не обязательно будет выбрано планировщиком.

[AddExchanges.planPartitionedJoin](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java) уже содержит нужную основу:

1. Если left имеет подходящее node partitioning, построить соответствующее требование для right.
2. Если right несовместим, вставить remote exchange только справа.
3. Иначе попробовать сохранить подходящее распределение right и перераспределить left.
4. Если ни одна сторона не подходит, вставить remote partitioned exchanges с обеих сторон.
5. Проверить совместимость; политика colocated joins может потребовать дополнительное перераспределение.

В комментарии к этому коду явно учтены Hive/Iceberg схемы с разными, но совместимыми bucket counts; согласование проходит через `PlanFragmenter.PartitioningHandleReassigner`. Поэтому «число buckets отличается → обязательно shuffle обеих» тоже неверно. Совместимость устанавливается engine/connector API, а не сравнением одного числа.

[DetermineTableScanNodePartitioning](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineTableScanNodePartitioning.java) решает, использовать ли connector partitioning при scan: учитываются fixed mapping, policy и соотношение buckets/tasks. Bucketed metadata не означает, что конкретный план сохранил это распределение. Для сценария нужно читать EXPLAIN с exchanges и actual scan properties.

## Hive и Iceberg: обнаруженные ограничения ordering

В [HiveMetadata.getTableProperties](../../plugin/trino-hive/src/main/java/io/trino/plugin/hive/HiveMetadata.java) есть раздельные пути для table partitioning и local sorting properties. При `isPropagateTableScanSortingProperties` и непустом `sortedBy` connector публикует `SortingProperty`. Комментарий предупреждает: ложный `sorted_by` поверх unsorted files может дать неверный результат. Это реальный существующий механизм; неправильно утверждать, что Trino вообще не умеет принимать sorting properties от connector.

В [IcebergMetadata.getTableProperties](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java) table partitioning передаётся отдельно, а список sorting properties пуст с `todo support sorting properties`. Значит, в этом срезе Iceberg sort order нельзя автоматически считать уже доступным physical proof для SMJ.

Для рассматриваемого **unsorted** случая оба пути ordering reuse не нужны. Корректный baseline всегда сортирует после последнего перемешивания. Если позднее включить reuse, необходимо доказать весь путь: files → splits → scan drivers → local exchange → join cursor, включая порядок нескольких файлов. Простого `ORDER BY` в подзапросе недостаточно как пользовательского способа включить SMJ: optimizer и exchanges меняют physical plan, а native SMJ пока отсутствует.

## Локальное распределение и скрытая потеря параллелизма

Не следует писать remote и local partitioning как одинаковое `hash % count`. Например, после `hash % 64` все строки task имеют фиксированные младшие биты; повторное `hash % 8` отправит их в одну локальную partition.

Trino учитывает это: [LocalPartitionGenerator.processRawHash](../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalPartitionGenerator.java) повторно смешивает hash через `XxHash64.hash(Long.reverse(rawHash))`. [LocalExchange.createPartitionFunction](../../core/trino-main/src/main/java/io/trino/operator/exchange/LocalExchange.java) отдельно обрабатывает system и connector partitioning. В connector варианте один bucket остаётся у одного local thread; если buckets на worker мало, объявленные восемь drivers не создадут восемь равномерных потоков.

Для SMJ требуется одинаковое local mapping обеих сторон. При переиспользовании storage buckets иногда выгоднее заново распределить строки по системному join hash: дополнительное перемещение может окупиться лучшим параллелизмом. Это cost-based решение, а не обязательное использование любого имеющегося bucketing.

## Итоговый путь для произвольного нового ключа

```text
выбранные storage partitions / любые старые buckets
  -> scan, допустимые filters и column pruning
  -> materialize/coerce реальные join keys
  -> сохранить доказанно совместимое распределение ИЛИ shuffle по join keys
  -> одинаковое local mapping обеих сторон
  -> bounded external sort каждого input partition
  -> merge cursors одной пары partitions
```

Объём сортировки определяется всеми выбранными после фильтров rows, если источник unsorted по новому ключу. Старое partitioning может уменьшить scan за счёт pruning, но не сортирует выбранные rows по произвольному JOIN key. Подробный численный разбор — [14](14-case-15b-17b-unsorted.md); audit Java-сортировок — [13](13-java-sorting-algorithms-and-code.md).
