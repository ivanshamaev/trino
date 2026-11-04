# 01. Риски корректности SMJ (верифицировано по коду)

Класс рисков: **SMJ даёт неверные результаты** относительно hash join / SQL-семантики. Это самый
опасный класс — такой риск не ловится бенчмарками, только направленными тестами.

Инвариант, на котором держится SMJ:

> Для любых двух ключей k1, k2: `equal(k1, k2) == true` ⇔ `compare(k1, k2) == 0`.

Hash join использует `EQUAL`/`HASH_CODE`; merge — `ORDERING`. Если операторы типа расходятся —
результаты разойдутся.

## 1.1. 🔴 C1: NaN в DOUBLE/REAL join-ключах — расхождение подтверждено

`core/trino-spi/src/main/java/io/trino/spi/type/DoubleType.java` (REAL — идентично в `RealType.java`):

```java
@ScalarOperator(EQUAL)
private static boolean equalOperator(double left, double right) { return left == right; }
// IEEE: NaN != NaN → false. Hash join: строки с NaN-ключами НИКОГДА не матчатся.

@ScalarOperator(COMPARISON_UNORDERED_LAST)
private static long comparisonUnorderedLastOperator(double left, double right) { return compare(left, right); }

private static int compare(double left, double right) {
    if (left == right) { return 0; }        // NaN == NaN → false, идём дальше
    return Double.compare(left, right);     // Double.compare(NaN, NaN) == 0 (!)
}
```

**Следствие**: `compare(NaN, NaN) == 0`, но `equal(NaN, NaN) == false`. Наивный SMJ положит все
NaN-строки в одну группу и смержит их → **лишние строки в результате** (hash join их бы отбросил).
Все NaN при этом каноникализируются хэшем (`doubleToLongBits`) в один паттерн — то есть hash join
сталкивает их в одном бакете и честно отбрасывает по equal.

Заметим: **-0.0/+0.0 обработаны в Trino консистентно** (`if (left == right) return 0` — комментарий
«Double.compare considers 0.0 and -0.0 different»): `compare(-0.0, +0.0) == 0` и `equal == true`,
хэш-код тоже нормализует (`if (value == 0) value = 0`). Риска нет, но инвариант надо зафиксировать тестом.

**Решения** (по нарастанию сложности):
1. **MVP-консерватизм**: правило `DetermineJoinAlgorithm` не выбирает SMJ, если хотя бы один
   equi-ключ имеет тип DOUBLE/REAL (и производные ROW/ARRAY с DOUBLE-полями). Просто и безопасно.
2. **NaN-guard в merge-цикле** (правильное решение, фаза 1): перед группировкой ключа — проверка
   `isNaN` по каждому floating-каналу (по образцу NULL-guard): NaN-ключ ведёт себя как NULL —
   никогда не матчится, эмитится как outer при LEFT/FULL. Стоимость: один branch на строку на
   floating-колонку; на flat-префиксе NaN-паттерн ловится префиксной маской бесплатно.
3. Тесты: fuzz с NaN/-0.0/+0.0/Infinity в ключах обеих сторон, oracle = hash join; unit-тесты
   компаратора на всех SortOrder.

## 1.2. 🔴 C2: не-orderable типы ключей (MAP)

`MapType` не переопределяет `AbstractType.isOrderable()` → **false** (`core/trino-spi/.../type/MapType.java`).
При этом MAP имеет EQUAL/HASH и `MapType.isComparable()` **переопределён** (строки 282-284, от
value-типа) → analyzer **разрешает** equi-join по MAP (`StatementAnalyzer:3298` проверяет
`commonSuperType.isComparable()`) — **equi-join по MAP сегодня работает через hash join**.
SMJ такой join отсортировать не может. (Верифицировано: risks/05, п.4.)

`RowType`/`ArrayType` orderable iff все поля orderable (`RowType.java:152`) — ключ-структуры с
MAP-полем тоже выпадают.

**Решение**: eligibility: `criteria.stream().allMatch(c -> type(c).isOrderable())`. Плюс тест:
join по MAP не выбирает SMJ даже под FORCE (падение с внятной ошибкой в FORCE-режиме).

## 1.3. 🟠 C3: NULL-порядки сортировки сторон

`SortingProperty` несёт `SortOrder` (ASC/DESC × NULLS_FIRST/LAST). Если левая сторона физически
отсортирована `ASC NULLS LAST`, а правая `ASC NULLS FIRST` — компаратор с одним SortOrder не
согласован с обеими. NULL-ключи и так не матчатся (guard), но **упорядочивание NULL-кластеров
влияет на ход merge**: левая строка с NULL придёт в конце, правые NULL — в начале; merge без
guard'а может «пройти мимо» ненулевых групп. При guard'е (§05, edge-cases) NULL-строки
обрабатываются отдельно — риск снимается, **но** компаратор обязан быть одинаков для обеих сторон.

**Решение**: правило требует единый канонический `OrderingScheme` (например ASC NULLS_LAST) для
обеих сторон; если коннектор сообщил сортировку с другим NULL-порядком — eligibility отказ
(не переупорядочивать неявно). Тест: смешанные NULL/не-NULL ключи, разные SortOrder сторон.

## 1.4. 🟠 C4: префиксная сортированность

- Таблица отсортирована по `(a)`, join по `(a, b)` — внутри равных `a` порядок `b` произвольный →
  **merge невозможен**: `LocalProperties.match` вернёт частичное покрытие — SMJ-eligibility обязан
  требовать покрытие **всех** equi-ключей (матчинг полного OrderingScheme, не префикса).
- Обратное валидно: sorted by `(a, b)` ⊇ sorted by `(a)` — join по `a` может использовать
  сортировку (projection свойства). `SortingProperty(a,b)` имплицирует `SortingProperty(a)` —
  механизм `LocalProperties.match` это умеет (префиксное поглощение), нужно только не сломать в
  своём правиле.

**Решение**: в `DetermineJoinAlgorithm` — полный матчинг; тест на оба направления.

## 1.5. 🟠 C5: Iceberg «sorted» — это сортировка per-file, не глобальная

Iceberg sort order (`sorted_by`, `SortOrder` в метаданных) применяется **на записи**: `IcebergPageSink`
сортирует данные **внутри каждого writer'а → каждого файла**. Следствия:

1. **Каждый data-файл = отсортированный run**; конкатенация файлов в партиции/бакете **не
   отсортирована** глобально.
2. Хороший вывод: «sorted-таблица» = набор готовых отсортированных ранов → **SMJ = k-way merge
   файлов без всякой пересортировки** (сплиты одного файла — отсортированные диапазоны: любой
   contiguous-отрезок отсортированного файла отсортирован). Память = k курсоров + окно группы.
3. Плохой вывод: коннектор **не может сообщать SortingProperty как свойство потока скана**
   (`localProperties` семантически = «в каждом сплите/потоке» — а нам нужно «k отсортированных
   потоков на бакет»). Честные варианты:
   - сообщать `SortingProperty` (сплит отсортирован) и научить SMJ-рантайм **мержить сплиты**
     (как partitioned merging exchange / MergeOperator над сплитами своей партиции);
   - либо не сообщать до появления «sorted-run-aware» API.

**Решение (дизайн)**: fast-path «sorted Iceberg» реализуется как merge k отсортированных
файловых потоков бакета (это ровно `MergeOperator`-механика), а не как «один отсортированный
стрим». Дополнительные guard'ы: все файлы снапшота с текущим `sortOrderId` (legacy 0 — refuse);
row-level deletes порядок не ломают (только вырезают строки) — но позиционные deletes удваивают
вес сплита (учёт в split weight уже есть).

## 1.6. 🟡 C6: cast в equi-клаузе теряет sortedness-свойство

`PropertyDerivations.visitProject` (`.../PropertyDerivations.java:783-787`) транслирует свойства
**только через identity-присваивания** (`computeIdentityTranslations`). Join по
`CAST(user_id AS double)` теряет `SortingProperty(user_id)` → SMJ не выбирается.

Это **не баг, а осознанное упущение**: сортировка по `user_id` (bigint) и по `CAST(...)` (double)
в общем случае не совпадают (XOR-знаковая нормализация отличается) — переупорядочивание кастом
некорректно в общем случае.

**Решение**: принять; документировать как ограничение («join-ключ = сырая колонка таблицы»).
Возможное будущее расширение: order-preserving cast'ы (int→bigint, timestamp→timestamp(p>3)).

## 1.7. 🔴 C7: коннектор сообщил ложную sortedness

Единственный источник «бесплатной» сортировки — метаданные коннектора. Ошибка в отчёте =
тихая порча данных. Подкатегории для Iceberg:
- mixed `sortOrderId` у файлов (решено: refuse при любом не-текущем id);
- сортировка с трансформами, которые движок не умеет сравнивать (решено: только identity —
  `getSupportedSortFields` уже фильтрует);
- hidden-колонки/эволюция схемы (поле удалено/переименовано — fieldId-матчинг спасает);
- deletes/DV (не ломают порядок — вырезание строк).

**Решение**: (а) `PlanSanityChecker`-подобная валидация в FORCE/PREFER-режиме на старте выполнения
(первый сплит можно верифицировать isSorted-проверкой на sample); (б) fuzz-тест «отчёт vs факт»
на всех вариациях метаданных; (в) kill-switch session-флагом; (г) документировать, что отчёт
даётся только при детерминированных условиях (§1.5).

## 1.8. Прочие зафиксированные инварианты (не риски, но тесты обязательны)

- **Нестабильность quicksort** (`PagesIndexOrdering`) — порядок равных ключей недетерминирован:
  join-семантике безразличен; oracle-тесты обязаны сравнивать **мультимножества** строк вывода,
  а не порядок.
- **`outputSingleMatch` + residual filter**: skip всего левого окна валиден только при
  отсутствии фильтра (фильтр зависит от не-ключевых колонок левой строки) — уже учтено в
  дизайне (05, §5.4), добавить тест.
- **VARCHAR**: equal/ordering оба на UTF-8 байтовом сравнении — консистентны; тест-дым всё равно.
- **DECIMAL (Int128)**, TIMESTAMP(*), DATE, integers — сравнение и равенство консистентны
  (проверено по AbstractLongType/Int128); покрыть fuzz'ом.

## 1.9. Сводная матрица «тип ключа → опасность → решение»

| Тип ключа | Опасность | Статус | Решение |
|---|---|---|---|
| DOUBLE / REAL | NaN: compare=0, equal=false | 🔴 подтверждено кодом | NaN-guard или exclusion в MVP |
| DOUBLE / REAL | -0.0/+0.0 | ✅ консистентно | тест-инвариант |
| MAP (в т.ч. внутри ROW/ARRAY) | не orderable, но hash-join'ится | 🔴 | eligibility isOrderable |
| VARCHAR, VARBINARY | — | ✅ | fuzz |
| DECIMAL, INTEGER-семейство, DATE/TIME/TIMESTAMP | — | ✅ | fuzz |
| ROW/ARRAY | порядок полей: (a,b) vs (a) | 🟠 см. C4 | полный матчинг scheme |
