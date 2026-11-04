# 08. Dynamic filtering и пропуск файлов (split skipping) в SMJ

Контекст вопроса: если одна таблица в процессе SMJ быстро проскакивает диапазоны, которых точно
нет во второй, сможет ли итератор передавать сигналы коннектору Iceberg для пропуска целых
групп файлов (skip split)? Поддержим ли dynamic file filtering?

## 8.1. Что уже есть в Trino (проверено по коду)

1. **Сбор DF — на build-стороне над sink**: `LocalExecutionPlanner.createDynamicFilter(...)`
   → `createDynamicFilterSourceOperatorFactory(...)` оборачивает build-пайплайн
   (:2722-2731 для nested-loop, :2959+ для lookup join); собираются min/max-домены по колонкам.
2. **Применение — два уровня**:
   - page-level: домены фильтруют страницы probe-скана;
   - **split-level (пропуск файлов!)**: SPI `ConnectorSplitSource.getNextBatch(int maxSize,
     DynamicFilterSnapshot dynamicFilterSnapshot)` — коннектор получает снапшот домена **во время
     генерации сплитов** и прунит. Iceberg: `IcebergSplitSource.getNextBatchInternal`
     (IcebergSplitSource.java:268+, ~:287+) строит `dynamicFilterPredicate` из
     `dynamicFilterSnapshot.currentPredicate()` и применяет к манифестам/файлам —
     **«пропуск целых групп файлов» на основе min/max уже существует**, домен-основанно.
3. Session-выключатель: `enable_dynamic_filtering` (SystemSessionProperties:146).

## 8.2. SMJ + DF: план поддержки (MVP)

- Сбор: та же обёртка `createDynamicFilterSourceOperatorFactory` **над MergeJoinBufferOperator**
  (план §4.2 п.2) — SMJ даже удобнее hash: build-сторона полностью материализуется до merge,
  домен DF финализируется **до** первого merge-шага.
- **Гейтинг по типу join (важно)**: DF-фильтрация probe валидна **только для INNER**
  (для LEFT/FULL фильтрация probe по build-домену уничтожила бы null-padded строки —
  семантическая ошибка). Для non-INNER не строим `DynamicFilterSourceOperator` вовсе
  (несобираемые DF подчистит существующее правило `RemoveUnsupportedDynamicFilters`).
- Тайминг: probe-скан стартует параллельно со сборкой build (как у hash); снапшоты DF доезжают
  в `getNextBatch` по мере готовности — контракт тот же, отдельного механизма не требуется.
- Тесты: product-test DF+SMJ (INNER: файлы второй таблицы вне диапазона первой прунятся —
  проверка по `IcebergSplitSource`-метрикам/EXPLAIN ANALYZE); LEFT: DF не собирается.

## 8.3. Курсорный range-skip (запрос «итератор сигнализирует коннектору») — P12, фаза 4

Пользовательский сценарий: merge-курсор уже прошёл ключ `K` — все файлы со `max(k) < K`
второй (co-located) стороны можно **не читать вовсе**. В Trino такого позиционного
канала «курсор → коннектор» **нет**: DF — это up-front домены, а не потоковые сигналы.

### Дизайн-набросок (только для S1-GOLD-пути)

1. Iceberg-статистики дают min/max по `k` **на файл** (манифесты) → сплиты ранжируемы по диапазону;
2. фаза-0 гарантирует порядок чтения сплитов по сортировке → scan-сторона выдаёт данные
   по возрастанию `k` (после LOCAL merging);
3. merge-оператор публикует **low-watermark ключа** `K_min` (позиция медленной стороны) в
   контекст задачи;
4. scan/split-source **отменяет невычитанные сплиты** с `max(k) < K_min` (prefetch-окно
   ограничено backpressure'ом буферов → отмена успевает).

### Почему НЕ после shuffle

REPARTITION-shuffle распределяет по **hash(k)** — порядок по `k` разрушается, диапазонной
локальности нет → range-skip работает **только** на co-located/sorted пути (S1-GOLD/S2),
не на generic 15B×17B-shuffle.

### Риски P12 (почему это фаза 4, а не MVP)

- новый канал проброса watermark (task-context → connector scan) — нет готового SPI,
  `DynamicFilterSnapshot` не подходит (pull-домены, не push-позиции);
- гонки watermark vs prefetch (отмена уже читаемого сплита);
- выигрыш ограничен случаем слабого диапазонного перекрытия таблиц; при сильном перекрытии — ноль;
- дешёвая альтернатива уже работает: статический DF (§8.1-8.2) прунит по min/max **до** старта
  чтения + planner-статистики (table-level min/max) — покрывает большую часть выигрыша.

**Рекомендация**: MVP — DF (INNER-only, §8.2); P12 — research-spike фазы 4 с бенчмарком
«узкое диапазонное перекрытие, S1-GOLD» против DF-бейзлайна; в core без убедительного выигрыша
не тащить.

## 8.4. Связь с рисками

- I3 (DF product-test) — расширен гейтингом INNER-only;
- P12 (новый, регистр) — opportunity с mitigation «DF + фаза-4 spike»;
- C9 (NULL) — DF-домены не включают NULL (Domain без nulls для INNER после R-C9.2-фильтра) —
  NULL-файлы прунятся и через null_count (06, §6.4 R-C9.2).
