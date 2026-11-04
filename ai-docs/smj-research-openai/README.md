# Sorted Merge Join в Trino: независимое предварительное исследование

Исследован checkout **Trino 485-SNAPSHOT**, commit `e112ffdf7304333b50bfdff50dda4229c897896d`. Дата исследования: **4 ноября 2026 года**. Это исследование и проектные предложения, не реализованный оператор и не результаты нагрузочного тестирования.

Основной вывод: partitioned external sort-merge join имеет смысл как дополнительная стратегия для больших build sides и предсортированных потоков. Его полезное свойство — управляемая память при росте входа. Для этого недостаточно добавить merge-loop: нужны bounded external sort, повторное чтение duplicate groups, согласованное распределение обоих входов и корректное взаимодействие со scheduler.

Исходное утверждение «без SMJ миллиарды строк обработать невозможно» слишком сильное. Trino уже имеет partitioned hash join, legacy spill и fault-tolerant execution. Ограничение hash join связано с размером одновременно материализованного build и его максимальной партиции, а не с общим числом строк. SMJ также не устраняет стоимость shuffle, сортировки, skew и огромного результата many-to-many join. Обоснование и исходники — в главах ниже.

| Документ | Содержание |
|---|---|
| [00 — Методика и границы](00-scope-and-method.md) | Независимость, версия, что проверено и что остаётся гипотезой |
| [01 — Архитектура ядра](01-trino-architecture.md) | SQL → optimizer → fragments → tasks → drivers → pages |
| [02 — JOIN сегодня](02-current-joins.md) | Build/probe, lookup sources, фильтры, специализированные joins |
| [03 — Память, spill и FTE](03-memory-spill-and-fte.md) | Реальные ограничения и отличие local spill от exchange storage |
| [04 — Алгоритмы SMJ](04-sort-merge-algorithms.md) | External sort, группы дубликатов, стоимость и skew |
| [05 — Другие движки](05-other-engines.md) | Spark, ClickHouse, Vertica, DuckDB и исследовательские работы |
| [06 — Варианты архитектуры](06-proposed-architecture.md) | Контракты и проект интеграции в Trino |
| [07 — Семантика и корректность](07-correctness-and-lifecycle.md) | NULL, NaN, outer joins, residual filters, отмена и retry |
| [08 — Проверки и benchmarks](08-validation-and-benchmarks.md) | Воспроизводимая программа экспериментов и критерии готовности |
| [09 — Brainstorm пяти агентов](09-five-agent-brainstorm.md) | Роли, разногласия, cross-review и принятые решения |
| [10 — Синтез и рекомендации](10-synthesis-and-recommendations.md) | Приоритеты, этапы разработки и открытые вопросы |
| [11 — Источники и карта кода](11-source-index.md) | Проверяемые ссылки, версии внешних исходников |
| [12 — Layout, buckets и repartition](12-layout-bucketing-and-repartitioning.md) | Несовпадающие storage/JOIN keys, составное бакетирование, reuse и shuffle |
| [13 — Сортировки в Java и Trino](13-java-sorting-algorithms-and-code.md) | Quicksort/radix/merge, audit ядра и существующего Hive/Iceberg multipass writer |
| [14 — Кейс 15 млрд × 17 млрд](14-case-15b-17b-unsorted.md) | Пошаговое исполнение unsorted JOIN, расчёт RAM/I/O/network, skew и output |
| [15 — Проект bounded Java sorter](15-bounded-java-sort-design-and-experiments.md) | Контракты, extraction existing code, memory admission и эксперименты |

Для первого чтения: **10 → 06 → 03 → 07**. Для разработчика оператора: **01 → 02 → 04 → 07 → 08**, затем индивидуальные записки в [brainstorm](09-five-agent-brainstorm.md).

Дополнительный разбор полностью неотсортированных **15 + 17 млрд строк**: **12 → 14 → 13 → 15**. Численная модель сохранена в [CSV](calculations/capacity-model.csv) и [JSON с допущениями](calculations/capacity-assumptions.json). Это расчёты, не benchmarks. Реализация рассматривается на Java, без Velox.

Новая находка дополнения: bounded multipass merge уже реализован в Hive `SortingFileWriter` и переиспользуется Iceberg. Его наличие уточняет карту reuse: в основном `OrderByOperator` такого scheduling нет, а connector writer требует адаптации к memory/yield/cancel контрактам JOIN.

Исследования `ai-docs/smj-research/` и `ai-docs/smj-research-kimi/` не читались. Java-код и существующая документация не изменялись. Все предлагаемые классы, свойства и тесты явно обозначены как новые; их имена не являются существующим API Trino.
