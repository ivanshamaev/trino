# Модель выбора Sorted Merge Join

`OFF` остается штатным значением `prefer_merge_join`. `FORCE` и `PREFER` сохраняют прежнюю семантику выбора любого допустимого equi-join. `COST_BASED` использует `MergeJoinCostModel`, `CostComparator` и `TaskCountEstimator` после определения distribution, до добавления exchanges.

## Ограничения выбора

Все прежние eligibility gates действуют: только partitioned INNER/LEFT/RIGHT/FULL с equi-критериями, допустимыми orderable-ключами, distributed sort и retry NONE. DOUBLE/REAL и ROW/ARRAY исключены. Для автоматического выбора дополнительно нужны включенный spill и известные положительные row count, input bytes, key bytes и NDV. Пустые входы и неизвестная статистика оставляют hash.

Число задач ограничено числом workers и max hash partition count. Доля входа на крупнейшей задаче оценивается как `max(1/tasks, 1/NDV)`: все строки одного ключа попадают на одну задачу. Для составного ключа используется максимальный NDV отдельного столбца, без предположения независимости. Это нижняя оценка перекоса: статистика NDV не описывает частоту самого популярного значения.

Рабочий бюджет `U` равен половине `query_max_memory_per_node`, оставляя запас для других операторов. Это эвристика планировщика, а не отдельный runtime memory limit. Фактические резервы и лимиты по-прежнему контролируются обычным query/task memory context.

## Состав стоимости

Оценки относятся только к входам. Построение выходных строк общее для обоих алгоритмов. CPU измеряется в условных байтовых единицах, принятых в других cost calculators Trino; это не прогноз миллисекунд.

- Hash: линейный проход по payload, хеширование ключей, оценка памяти `2 * buildBytes + 24 * buildRows`, запись/чтение двух сторон при spill. Число локальных hash-партиций берется из task_concurrency. Trino загружает одну целую партицию через getAllSpilledPages()/PagesIndex и не дробит ее рекурсивно: если даже такая партиция не помещается в query memory limit, hash считается неприемлемым. Один hot key не разделяется между локальными партициями. Для RIGHT/FULL штатный hash spill недоступен; сравнивается память всего build на задаче с полным query memory limit.
- Merge: линейное сравнение ключей, comparison sort `keyBytes * log2(taskRows)` или проверка и merge страниц `keyBytes * (1 + log2(taskRows/1024))`, external sort и запись/повторное чтение build. `SortingSpiller` попарно объединяет одноуровневые раны во время накопления и в конце оставляет один ран. При spill учитываются initial write/read и уровни compaction, то есть примерно `2 * bytes * (1 + ceil(log2(runs)))`. Большая повторяющаяся probe-группа увеличивает число обходов build в зависимости от размера блока probe.
- Network: shuffle обоих входов; для совместимого connector partitioning по join-ключам при разрешенном colocated join стоимость сети равна нулю. Наличие одинаковых имен или числа buckets само по себе не подтверждает совместимость. Single-node источники не считаются colocated partitioned join.
- Memory: сравниваются оценки пика hash и буферизованного/spillable merge; штатные веса CPU/memory/network берутся из `CostComparator`.

Известная сортированность означает порядок внутри страниц. Она уменьшает число сравнений, но не отменяет стоимость merge, адресного индекса, буферизации и spill. Runtime проверяет страницы и при нарушении порядка возвращается к comparison sort. Метрики `Merged pre-sorted input rows` и `Fallback sorted input rows` показывают фактический путь.

## Пороги и запас

Сохраняются `merge_join_small_build_threshold`, `merge_join_memory_pressure_threshold` и `merge_join_big_side_threshold`. Входы, для которых hash уверенно помещается, не переводятся в merge ради одной экономии памяти. Исключение допускается для двух подготовленных colocated входов, но и тогда нужно выиграть сравнение стоимости.

`merge_join_cost_margin` передается как session property и config `optimizer.merge-join-cost-margin`. Допустим диапазон `[0,1)`, NaN и infinity запрещены. Все компоненты merge cost умножаются на `1/(1-margin)` перед сравнением. Дефолт 0.1 означает требование не менее 10% оцененного выигрыша в полной взвешенной стоимости; равенство сохраняет hash.

## Воспроизводимые проверки

Обычная регрессия: `TestMergeJoinCostModel`, `TestDetermineJoinAlgorithm`, `TestOptimizerConfig`, `TestPagesIndex`, `TestOrderByOperator`, операторные/spiller и SQL oracle-тесты SMJ.

Большая операторная матрица включается `-Dsmj.large=true`; FULL JOIN с полным выводом 2³¹ строк — отдельно `-Dsmj.extreme=true`.

`TestIcebergSortedWriting#testMergeJoinCostCalibration` с `-Dsmj.calibration=true` сравнивает OFF/FORCE/COST_BASED на миллионе строк, sorted/unsorted/skew/FULL, с checksum результата, лимитом 64 МиБ на узел для sorted/unsorted/skew и 128 МиБ для FULL с payload 512 байт, фактическими memory/spill stats, прогревом и медианой трех повторов. Для этого benchmark отдельно уменьшены sink и local-exchange buffers до 4 МиБ и включен ранний отзыв revocable memory (threshold 0.02, target 0), чтобы общий query limit не исчерпывался одними exchange buffers до проверки join. Время не служит нестабильным CI assertion.

`TestPartitionedTpcdsCostBasedPlan#testCostBasedMergeTpcdsPlans` сравнивает OFF и COST_BASED при включенном spill и фиксированном query memory limit 16 ГиБ для 103 частей запросов TPC-DS SF-1000 на существующих Iceberg metadata fixtures. В диагностический отпечаток плана включен algorithm. У 100 query parts план совпадает с OFF; для q04, q11 и q74 зафиксированы отдельные ожидаемые merge-планы: статистика build на задачу превышает 16 ГиБ уже по сырым байтам. Это решение на оценках планировщика, а не измерение фактических промежуточных данных. Это проверка планов, без выполнения терабайтного набора данных.

Результаты последних прогонов и фактически использованный JDK фиксируются в IMPLEMENTATION_CHECKLIST.md. Эта модель остается статистической эвристикой: NDV, средний размер строки и одинаковая условная цена дискового байта не заменяют сведения о hot keys, конкретных устройствах и конкурирующих запросах.

## Проверенный физический spill-путь

При большом числе маленьких ранов прежний OrderBy открывал их одновременно и удерживал страницу каждого входа. Широкий macro выявил до ~199 МиБ в MergeSortedPages. Новый SortingSpiller использует binary-level compaction, spill-страницы около 64 КиБ, немедленно закрывает промежуточные файлы и читает один финальный ран. Дополнительный IO включен в модель; число файлов/буферов ограничено O(log runs), число одновременно читаемых страниц на одну compaction — двумя. `finish()` ожидает futures асинхронно и закрытие отменяет незавершенную запись/compaction.

Реальный файловый тест: 129 перекрывающихся ранов, 132 096 проверенных строк, учтённая память ≤1 МиБ, одновременно не более 16 файлов, после close — нулевой резерв и пустой spill-каталог. Проверены ошибки compaction и отмена уже начавшегося merge с освобождением его резерва.

Для VARCHAR Iceberg оценивает размер в памяти через размер на диске и коэффициент. Высокая сжимаемость может занижать оценку. FULL macro использует 512-байтовые строки из salted SHA-512; с учетом трех узлов даже средний сырой build (~171 МБ) превышает лимит 128 МиБ на узел. Прогон подтвердил OOM hash и успешный автоматический MERGE. Сырые измерения сохранены в CALIBRATION_RESULTS.json. Значения порогов 0.7/0.3/1.0 и margin 0.1 оставлены консервативными: в трех проходящих hash-сценариях AUTO сохраняет hash, а FULL выбирает bounded merge. Это локальная калибровка, а не гарантия оптимального времени на произвольном оборудовании и статистике.
