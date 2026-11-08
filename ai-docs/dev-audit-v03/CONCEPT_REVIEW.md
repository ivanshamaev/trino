# Концепция и архитектурные инварианты

Ревизия: `0393e45fcf9`. Автор определяет проект как личный MVP; upstream scope не согласован, CLA не подписан. Эти внешние условия не являются найденными ошибками Java. Здесь оценивается техническая концепция.

## 1. Что именно реализовано

Это partitioned external sort + task-local merge + replayable build stream. Равные ключи распределяются по совместимым задачам, обе стороны упорядочиваются в ASC NULLS LAST, build публикует завершённые spill batches через bridge. Probe использует монотонный scan cursor, discover границ равной группы и её повторный обход. Homogeneous probe regions объединяются в bounded chunks.

В каждой задаче один merge driver. Между workers параллелизм есть; внутри задачи task_concurrency не создаёт несколько merge drivers. Один hot key не разбивается между tasks. Поддерживаются INNER/LEFT/RIGHT/FULL и residual predicates, PARTITIONED, retry NONE; DOUBLE/REAL/ROW/ARRAY исключены из eligibility.

Идея жизнеспособна как дополнительный алгоритм для memory-sensitive equi-join. Spill не превращает many-to-many output в линейный и не гарантирует общий выигрыш latency. Discovery гигантской группы должен увидеть её конец; наличие streaming публикации batches не обещает ранний вывод такой группы до окончания build.

## 2. Порядок — контракт всего физического плана

Сортировка отдельных файлов, страниц, driver streams и task input — разные гарантии. Canonical SortNode, LOCAL merge и REMOTE ordered repartition обеспечивают task input в основном пути. Hash partitioning сохраняет относительный порядок выбранных строк отправителя; принимающая merge-сторона должна объединять отсортированные потоки отправителей.

Существенное условие: после выбора MERGE и вставки этой топологии последующие rules должны сохранять её либо заново строить корректную топологию и spillability для нового алгоритма. Простого поля JoinNode.algorithm недостаточно.

Третий аудит обнаружил достижимый сброс маркера в PushProjectionThroughJoin, который вызывается из PushPartialAggregationThroughJoinWithProjection уже после AddLocalExchanges. Новый hash join наследует настройки, рассчитанные для MERGE. Воспроизведены ошибки runtime, несмотря на прохождение прежних unit tests. Это самостоятельный дефект интеграции, а не опровержение merge-loop. См. [REPORT.md](REPORT.md).

Другая проверяемая граница — PushPartialAggregationThroughExchange: вариант без projection отвергает ordered exchange, вариант с projection такого условия в pattern не имеет и создаёт exchange с пустым ordering. Отдельный distributed SQL с residual и aggregation пять раз совпал с OFF; исследованный план сохранил MERGE/RemoteMerge. Эта проверка не воспроизвела тихую потерю ordering. Соседний rewrite нужно покрыть при изменении маркера, но самостоятельным доказанным дефектом v03 он не объявляется.

Практическая защита — final-plan validator для MERGE: одинаковые criteria/order semantics, совместимое partitioning, sorted single stream обеих сторон, допустимые retry/types и согласованные dynamic filters. Валидация должна выполняться после всех rewrites, а не только при первоначальном выборе.

## 3. File order в Iceberg

Проверка current/non-null file sortOrderId и identity prefix консервативна относительно содержимого файла. Она не утверждает порядок конкатенации файлов. Runtime merge-sort path проверяет страницы и делает fallback, если hint не подтверждён.

В третьем аудите отдельно проверено влияние общего ConnectorTableProperties на обычные Window, partitioned running sum, GROUP BY и FETCH FIRST WITH TIES при SMJ OFF: пять отсортированных файлов с перекрывающимися ключами, task_concurrency=1/4, сравнение report_sortedness=true/false. Эти сценарии прошли. Это положительное свидетельство, а не доказательство для всех connector/optimizer consumers.

Включённое по умолчанию report_sortedness меняет planning metadata даже при prefer_merge_join=OFF. Поэтому исходный dev plan «OFF = ноль изменений / byte-for-byte plans» нельзя интерпретировать буквально как уже выполненный глобальный gate. Isolation bounded sorter в v02 уменьшает риск общей регрессии ORDER BY; его state machine при этом тоже изменён.

## 4. Память и хранение

Внешний offset index устраняет поиск от разреженного checkpoint. Он хранит 8 bytes/page на диске; heap metadata не растёт пропорционально всему build. Native bridge держит до двух replay pages и освобождает optional cache при росте output builder. Probe chunks и output builder имеют memory contexts.

RIGHT/FULL с residual predicate требуют информации о совпавших build rows. Group-local boolean array либо disk bitmap — корректный способ сохранить эту информацию без удержания всей таблицы. Это bounded RAM при наличии внешнего хранилища, но не буквальное отсутствие match state. Строгое равенство heap FULL==INNER из раннего плана не следует из наличия bitmap.

Managed reservations не равны полному JVM heap: существуют upstream input pages, serialization buffers, временные allocations и объектный overhead. Публичная документация теперь прямо это оговаривает. Проверки малого query limit и 10M-row joins измеряют reservations и SQL result; они не сертифицируют любой размер строки, любую конкурентность или 15B×17B.

Шифрование — часть storage contract, а не только деталь serialization data pages. Auxiliary match bitmap содержит результаты residual predicate по build-row ordinals. Он пишется через raw RandomAccessFile и сохраняет plaintext даже при включённом encryption у native delegate. Исправленная cleanup не решает конфиденциальность файла во время выполнения.

## 5. Цена и scope MVP

COST_BASED остаётся локальной эвристикой после join-order/distribution selection. Общие CostCalculator/EXPLAIN totals не стали полной SMJ-aware CBO. Средние NDV не определяют частоту hot keys; модельная allowance не является admission. Исключение ROW/ARRAY и floating keys — допустимое ограничение текущего scope, а не обещание поддержки любых orderable types.

Подтверждённое v02 ограничение compressed VARCHAR statistics сохранено в public docs: AUTO может оставить hash и превысить память там, где FORCE проходит. Подмена этого факта произвольным коэффициентом была бы слабее явного experimental контракта. В третьем аудите это не заново найденный дефект и не закрытая универсальная проблема статистик.

Ресурсная польза подтверждается отдельными ограниченными сценариями. Полный stage 5/6/7 из первоначального плана (multi-driver, совместная CBO enumeration, FTE, range/MergePath, масштабные acceptance gates) не реализован и не должен следовать из закрытого MVP checklist. Подготовка upstream требует согласовать именно узкий experimental scope.
