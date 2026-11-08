# Аудит реализации Sorted Merge Join

> Исторический аудит исходного HEAD от 2026-11-05. Доработки и финальные проверки от 2026-11-08 описаны в [COMPLETION_REPORT.md](COMPLETION_REPORT.md); все пункты [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md) закрыты.

Дата: 2026-11-05. Ветка: `smj-plan`, HEAD `1d7ab12541e`, база сравнения `master` (`e112ffdf730`). Это аудит исходников, а не повторение отметок checklist. Рассмотрены план `ai-docs/smj-plan-dev/`, `ai-docs/dev_todo_list.md`, связанный корпус `ai-docs/smj-research*`, Java-код оператора, планировщика, обменов, Iceberg и их тесты. `hs_err_pid*.log` не читались.

Основные исходники: [merge-оператор](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinOperatorFactory.java), [build-оператор](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBufferOperatorFactory.java), [мост](../../core/trino-main/src/main/java/io/trino/operator/join/merge/MergeJoinBridge.java), [правило выбора](../../core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/DetermineJoinAlgorithm.java), [обмены](../../core/trino-main/src/main/java/io/trino/sql/planner/optimizations/AddExchanges.java), [локальное планирование](../../core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java), [Iceberg sortedness](../../plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java). Номера строк ниже относятся к указанному HEAD.

## Вердикт

Текущая реализация **не готова к включению SMJ для рабочих запросов**. Основная цель плана — корректный equi-join на входах больше RAM с ограниченной памятью — не достигнута. Есть независимые ошибки результата на границах страниц и для составных ключей, а build целиком материализуется в памяти перед probe. Режим `OFF` по умолчанию ограничивает зону воздействия, но `PREFER`, `FORCE` и `COST_BASED` способны выбрать проблемный путь.

Положительная часть: есть отдельный маркер `JoinNode.algorithm`, базовое правило eligibility, сортировка обеих сторон, мост, учет простых `NULL`/`NaN`, residual filter и короткие unit/e2e-тесты. На небольших данных с простым ключом и одним probe-page алгоритм действительно выполняет merge. Эти тесты не проверяют основные предельные условия плана.

## Блокирующие замечания

### P0-1. Дубликаты probe-ключа на границе страниц теряют матчи

В `MergeJoinTransformation.advanceProbe()` флаг `groupLoaded` сбрасывается на последней позиции текущей страницы (`MergeJoinOperatorFactory.java:581-592`). При следующей странице `process()` меняет `probePage` и ставит `probePos=0` (`:385-388`), затем `loadGroup()` начинает искать от `scanPage/scanPos`, которые уже стоят **после** найденной build-группы (`:538-563`). Если первая строка новой probe-страницы имеет тот же ключ, `groupStartPage=-1`; INNER теряет строку, LEFT/FULL выводит ее как unmatched. Минимальный сценарий: build `[k=7]`, probe-страницы `[k=7] | [k=7]`; ожидаются два совпадения, выводится одно. Текущий тест на границу страниц использует разные probe-ключи по разные стороны `.pageBreak()` (`TestMergeJoinOperator.java:108-135`). Нужно сохранять найденную группу между probe-страницами до фактического изменения ключа и добавить тесты для INNER/LEFT/FULL, с residual filter и без него.

### P0-2. При заполнении выходной страницы обход build-группы начинается заново

`processProbeRow()` каждый раз инициализирует `iterationPage/iterationPos` началом группы и `anySurvived=false` (`MergeJoinOperatorFactory.java:463-466`). Если `pageBuilder` заполняется внутри группы, метод возвращается, не продвигая `probePos` и не сохраняя позицию в группе (`:467-490`). Следующий вызов `process()` снова обходит ту же группу с начала. Группа больше емкости одной выходной страницы дает повторные строки и может бесконечно выдавать одинаковые страницы. Ошибка возникает и на меньшей группе, если страница уже частично заполнена. Для LEFT/FULL сброс `anySurvived` дополнительно способен выдать ложную строку с `NULL` после фильтрации. Требуется состояние продолжения по build-позиции и флагу найденного совпадения; тест должен принудительно пересечь границу выходной страницы внутри одной группы.

### P0-3. `compare == 0` используется как SQL-равенство для всех orderable-типов

Правило допускает любой `Type.isOrderable()`, кроме верхнеуровневых `DOUBLE/REAL` (`DetermineJoinAlgorithm.java:238-253`). Оператор проверяет только верхнеуровневый `NULL` и `NaN` (`MergeJoinOperatorFactory.java:671-686`) и объявляет совпадение, если компаратор сортировки вернул ноль (`:538-555`, `:600-613`). Для `ROW(1, NULL)` = `ROW(1, NULL)` результат SQL-равенства `UNKNOWN`, но компаратор `ROW` считает два внутренних `NULL` равными (`RowType.java:554-579`, `:931-943`). Аналогично `ARRAY[NULL]` (`ArrayType.java:685-696`), а вложенный `DOUBLE NaN` не проходит верхнеуровневый guard. Итог — ложные join-строки, включая INNER. Надо либо консервативно исключить сложные типы из eligibility, либо перед выводом проверять реальный оператор `EQUAL` для ключей и считать `NULL`/`FALSE` несовпадением. Нужен oracle-тест против hash join для вложенных `NULL`/`NaN`.

### P0-4. Планировщик может уничтожить сортировку после ее вставки

В `AddExchanges.planPartitionedJoin()` сортировка обеих сторон добавляется на строках `1026-1029`. После этого при `colocated_join=false` и `hasMultipleSources(...)` правая сторона оборачивается обычным `partitionedExchange` без `orderingScheme` (`:1031-1036`). Обычный remote exchange не гарантирует порядок; probe-оператор при этом предполагает отсортированный build-поток. Это условный путь, но настройка поддерживается и guard для SMJ отсутствует. Нужно переставить эту ветку до сортировки или использовать ordered exchange и проверить итоговый план на сохранение порядка.

### P0-5. Ordered repartition может изменить схему распределения одной стороны join

При bucketed/совместимом partitioning `planPartitionedJoin()` создает exchange для правой стороны со схемой, переведенной из левой (`AddExchanges.java:985-998`), и проверяет совместимость **до** сортировки (`:1020`). `withCanonicalSort()` заменяет любой remote REPARTITION на `mergingPartitionedExchange` (`:1052-1072`), а его фабрика безусловно строит новый `FIXED_HASH_DISTRIBUTION` по колонкам (`ExchangeNode.java:243-256`). Исходный connector partitioning handle, bucket mapping, layout и параметры exchange теряются. Если левая сторона остается в собственном bucket-разбиении, одинаковые ключи могут оказаться в разных задачах и не соединиться. Замену надо делать с сохранением полного `PartitioningScheme` исходного exchange; после замены вновь проверить совместимость. Тест: colocated/bucketed левая сторона и repartition правой по ее handle, затем сравнение с hash join.

### P0-6. Build целиком удерживается до завершения; заявленной bounded memory нет

`MergeJoinBridge` хранит все build-pages в `ArrayList` (`MergeJoinBridge.java:59-76`), публикует их только после `finish()` (`:79-85`), а probe ожидает этого события перед чтением (`MergeJoinOperatorFactory.java:344-354`). Даже при включенном spill `MergeJoinBufferOperator.finish()` читает **все** раны обратно в `allPages`, затем снова складывает их в bridge (`MergeJoinBufferOperatorFactory.java:164-180`). Весь build повторно находится в heap; память чтения не резервируется. `zero retention` относится лишь к отсутствию отдельного буфера группы в probe, но не к системе в целом. При build > RAM запрос закончится OOM или превышением лимита памяти вместо выполнения цели `smj-plan-dev/README.md` §1 и stage 5. Нужен потоковый или ленивый spill-backed источник с повторяемыми группами и контролем памяти на всем жизненном цикле.

### P0-7. Spill запускается асинхронно без ожидания и проверки результата

`Spiller.spill()` возвращает future, завершающийся после записи (`Spiller.java:24-30`); `FileSingleStreamSpiller` отправляет запись в executor (`FileSingleStreamSpiller.java:127-132`). Build-оператор игнорирует future (`MergeJoinBufferOperatorFactory.java:214`), сразу считает память свободной (`:215-216`) и продолжает принимать страницы. Следующий `spill()` либо `getSpills()` на `finish()` требует отсутствия незавершенной записи (`GenericSpiller.java:60-76`, `:90-93`) и может упасть с `previous spill still in progress`; асинхронная ошибка записи тоже не учитывается. После исправления ожидания все равно остается P0-6. Нужны `isBlocked()`/состояние spill и обработка исключения future, тест с управляемой медленной записью.

## Высокий приоритет

### P1-1. RIGHT/FULL могут неограниченно накапливать unmatched build-вывод

На `process(null)` оператор проходит все еще не выведенные build-строки и добавляет их в один `PageBuilder` без проверки `isFull()` (`MergeJoinOperatorFactory.java:356-382`). Для большого unmatched build это расход памяти пропорционально размеру build и нарушение stage 5 acceptance. Нужен инкрементальный drain с курсором `(page, position)` и выдачей страницы при заполнении.

### P1-2. Мост RIGHT/FULL не освобождается по lifecycle

`createMergeJoin()` создает `JoinBridgeManager` с `buildOuter=true` для RIGHT/FULL (`LocalExecutionPlanner.java:2927-2931`). Менеджер при этом держит outer-reference со счетчиком 1 и вызывает `destroy()` только после его освобождения (`JoinBridgeManager.java:140-155`). В merge-ветке outer factory не создается и `outerOperatorFactoryClosed()` не вызывается (`LocalExecutionPlanner.java:2888-2984`). Следовательно, `whenAllFinishes` не наступает и `MergeJoinBridge.destroy()` не очищает страницы до освобождения всего объекта задачи. Поскольку unmatched build-строки уже выводит сам merge-оператор, manager должен создаваться с `buildOuter=false` или явно закрывать outer lifecycle.

### P1-3. COST_BASED выбирает SMJ именно там, где текущий build не помещается в память

`DetermineJoinAlgorithm.isCostBasedMergePreferred()` возвращает `true`, когда оценка build > `0.7 * query.max-memory-per-node` (`DetermineJoinAlgorithm.java:143-177`). Но P0-6 требует материализации всего build, а P0-7 делает spill ненадежным. Правило не учитывает реальную память на задачу, степень параллелизма, стоимость сортировки, существующую сортированность или оценку результата; `mergeJoinCostMargin` объявлен в config, но в правиле не применяется. Это не реализация сравнения стоимости из stage 6 и политики §15.6. До исправления памяти `COST_BASED` нужно выключить или ограничить безопасным доказуемым случаем.

### P1-4. Iceberg sortedness не ускоряет SMJ и проверяется планированием всех файлов

`IcebergMetadata.getSortingProperties()` проходит `tableScan.planFiles()` для каждого вызова `getTableProperties` (`IcebergMetadata.java:1754-1813`), что на таблицах с большим числом файлов добавляет дорогое manifest-планирование. Сам `AddExchanges.withCanonicalSort()` безусловно вставляет `SortNode` или partial sort (`AddExchanges.java:1042-1082`), не проверяя уже выведенные `SortingProperty`. Поэтому заявленный W1/S1-GOLD путь сейчас отсутствует: даже отсортированные Iceberg-файлы пересортировываются. Это прежде всего пробел производительности, а не доказательство неправильного результата. Нужен тест EXPLAIN/метрик на Iceberg с несколькими отсортированными файлами и оценка стоимости получения property.

### P1-5. Наблюдаемость и acceptance не соответствуют checklist

`MergeJoinStatistics` объявляет `outputRows` и `keyGroups`, но методы `recordOutputRow()`/`recordKeyGroup()` нигде не вызываются (`MergeJoinOperatorFactory.java:216-283`; поиск по пакету). Эти показатели всегда нули. Тесты `TestMergeJoinOperator` проверяют короткие группы и страницы, но не тестируют P0-1/P0-2, spill, ограничение памяти или вложенные ключи; e2e `TestMergeJoin` содержит семь небольших VALUES-запросов (`TestMergeJoin.java:66-123`). Отметки `[x]` в checklist отражают успешное прохождение этих тестов, а не выполнение риск-гейтов из stage 2/5/6. Нужны тесты, проверяющие именно инварианты, и переоценка статуса этапов.

## Соответствие плану

| Этап | Статус по исходникам |
|---|---|
| 0 — Iceberg sortedness | Метаданные и kill switch реализованы; полная матрица файлов/удалений и польза для SMJ не доказаны. |
| 1 — модель `JoinNode` | Маркер, сериализация и базовые инварианты есть. |
| 2 — оператор | Работает лишь на ограниченных случаях; P0-1/2/3 нарушают результат. |
| 3 — выбор и guards | Основные guards есть; eligibility пропускает сложные типы; `PREFER`/`FORCE` выбирают любой eligible join. |
| 4 — обмены и e2e | Базовая схема работает на малых тестах; P0-4/5 нарушают гарантии порядка/распределения на других путях. |
| 5 — spill, RIGHT/FULL, multi-driver | Не завершен: P0-6/7, P1-1/2; multi-driver не реализован. |
| 6 — partitioned merge, CBO | Есть заготовки exchange и пороговое правило; полного CBO, сохранения partitioning и acceptance нет. |
| 7 — интеграция | Зависит от исправления предыдущих этапов; заявленные исследовательские пункты не проверялись как готовые функции. |

## Порядок исправления и проверки

1. Отключить выбор SMJ в пользовательских режимах до исправления ошибок результата; `OFF` оставить штатным значением. Исправить P0-1/2/3/4/5 и добавить oracle-сравнение мультимножеств с hash join на границах probe/build/output страниц, nested `NULL`/`NaN`, bucketed partitioning и `colocated_join=false`.
2. Переделать build bridge и spill в потоковый, повторяемый источник с явным учетом памяти и ожиданием асинхронных операций; проверить запрос, где build существенно больше лимита памяти, и несколько spill-ранов подряд. Исправить RIGHT/FULL drain и lifecycle.
3. Затем включать `COST_BASED`: реализовать сравнение затрат, реально использовать sortedness, проверить число стадий, peak memory, объем spill, параллелизм и регрессию OFF согласно stage 5/6.

## Границы проверки

Выводы P0-1/2/3/4/5/6/7 следуют из конкретных ветвей Java-кода и контрактов типов/Spiller; это статически воспроизводимые сценарии. Полные Maven-тесты и runtime-профили не запускались: в окружении нет `java`, `mvnd` и `/usr/lib/jvm`. Поэтому отчет не утверждает измеренных времен, peak memory или фактического прохождения/падения предложенных новых тестов. Исходные файлы не изменялись.
