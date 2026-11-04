# 7. Brainstorm: Java/performance-инженер — hot-path, память, бенчмарки

> Роль: специалист по производительности Java/JVM. Факт из кода: target JDK этого репо — **25**
> (`project.build.targetJdk=25`); Vector API — инкубатором (`--add-modules=jdk.incubator.vector`),
> есть инфраструктура детекции возможностей (AVX512F/VBMI2-compress, SVE — `io.trino.simd`) — её надо
> переиспользовать, а не изобретать.

## 7.1. Hot-path: сравнение ключей left[i] vs right[j]

Инвариант: в merge-цикле сравнение — это не N log N, а **≤ L+R−1 сравнений на весь merge** (лестница).
Цена одного сравнения решает меньше, чем паттерн доступа вокруг него → важнее layout ключа, чем
хитрость компаратора.

**(а) MethodHandle из TypeOperators** — статус-кво для `WorkProcessor.mergeSorted`. На каждое
сравнение: N колонок × 2 косвенных вызова; после прогрева JIT инлайнит мономорфные хэндлы, но
контейнерные типы (Row) делают каллсайт полиморфным → деоптимизация в середине merge дороже сравнения.
Каждое сравнение — два случайных доступа; для VariableWidthBlock ещё дериференс Slice. Аллокаций ноль.
**Вердикт**: reference-implementation и редкие типы; в hot-path SMJ не должно быть ни одного MH-вызова на строку.

**(б) Кодогенерация (OrderingCompiler-style)** — один мономорфный вызов
`compare(leftPage, leftPos, rightPage, rightPos)`, прямое чтение блоков без boxing, early-exit по
первой колонке (на сортированных данных почти всегда решает первая — распределение выходов компаратора
скошено, branch predictor любит). `defineHiddenClass` амортизируется кэшем. С dictionary работает, но
каждый доступ — лишний indirection; правильнее один раз на страницу сделать `getUnderlyingValueBlock()`
и сравнивать в координатах словаря. **Вердикт: обязательный baseline для row-vs-row.**

**(в) Векторизованное сравнение батчами + SIMD.** Merge двух потоков — data-dependent ветвление на
каждый элемент, SIMD merge-stepping на JVM малополезен. Полезны два места: (1) **поиск границы**
«первая позиция right ≥ key» — gallop/binary search, отлично векторизуется для fixed-width ключей
(`Vector<Long>` compare + mask firstTrue); (2) сравнение блока левых ключей с одним правым (группы
дубликатов). Всё это требует плоских массивов ключей → реально (в) существует как надстройка над (г).
Из кода `io.trino.simd`: эмуляция compress на NEON медленнее скаляра → **любой SIMD-путь обязан
gating по `SimdSupport.capabilities()`**.

**(г) Материализация плоского ключа (Spark prefix-compare) — ядро дизайна.** Ключ нормализуется в
8 байт: order-preserving encoding (long — XOR знакового бита; descending — инверсия; null-флаг —
отдельный бит; multi-column — конкатенация пока влезает; коллизия префикса → fallback на (б)).
В Trino уже есть почти готовая инфраструктура: **`FlatHashStrategy`/`FlatHashStrategyCompiler`** из
`FlatGroupByHash` — плоские колоночные layout'ы и порядок-сохраняющие кодировки. Что даёт:
`long[] leftKeys, rightKeys` → merge-цикл — чистый sequential scan двух массивов, `Long.compare`
инлайнится в одну инструкцию, prefetcher работает идеально, SIMD gallop тривиален, radix-sort входов
без сравнений. Цена: проход материализации (O(N), дешёвый), +8 Б/строку (на спилле ключи отдельным
компактным сортированным стримом — жмутся отлично). Кэш-модель: ключи компактны (8Б×batch = 1 кэш-лайн
на 8 строк), payload не трогается до сборки выхода.

**Рекомендация: композит — flat 64-bit prefix в merge-цикле, сгенерированный компаратор для коллизий
префикса и экзотики, MH — только interpret-режим до компиляции.**

## 7.2. Выходные страницы: словарная сборка vs PageBuilder

Сегодня `MergeSortedPages`/`DefaultPageJoiner` копируют построчно (appendTo) — per-row вызовы,
копирование всех payload-байтов. Velox-подход: собрать `int[] leftIds`/`int[] rightIds` и завернуть в
**DictionaryBlock поверх исходных страниц** — копируются только 4-8 Б идентификаторов на строку.

Как ложится на Trino Block API:
- **Lifecycle**: DictionaryBlock держит ссылку на underlying → исходные страницы живут, пока жива
  хоть одна выходная; `getRetainedSizeBytes` включает underlying (учёт корректен), но OperatorContext
  продолжит показывать память после `finish()` — нужно аккуратно перенести учёт, иначе
  MemoryRevokingScheduler будет ревоковать закрытый оператор.
- **Compaction/вложенность**: downstream может создать вложенные словари; правило Velox: флаттенить
  при selectivity < порога (аналог `Page.compact()`/пороги DictionaryBlock); unmatched-строки outer
  join — просто ids позиций, ни одной копии.
- **RLE-кейс**: группа дублей слева × последовательные позиции справа → sequence-блок (ids = start..start+n)
  как «умножение без данных» в NestedLoopJoinPages.

Оценка: построчный appendTo на широком payload — 3-10× больше инструкций; словарная сборка —
**2-5× на inner join с широким выходом**, кратный рост при skew-группах; на узком (2-3 колонки) —
1.3-2×. GC: аллокации на страницу вместо на строку. Downstream dictionary-aware
(`DictionaryAwarePageProjection`) — выигрыш не съедается.

## 7.3. Дубликаты ключей и skew

- Правая группа — это **диапазон [groupStart, groupEnd) в отсортированном правом потоке**, не
  материализованный набор; материализовать (flatten) только когда группа пересекает границу
  удерживаемого окна страниц (Velox «right flattening only when group crosses batch boundary»).
- Выдача блочная, cursor-based resumable (yield после N строк/X байт); состояние =
  (leftPos, rightPos, groupStart, groupEnd, фаза).
- **Избегать O(log n) на строку**: (1) не использовать PriorityQueue при k=2 — прямой двухпутёвый
  цикл (PQ даёт boxing/аллокации узлов); (2) advance внутри группы — инкремент указателя, сравнение
  только на границе; (3) **galloping** (экспоненциальный+бинарный поиск по flat-ключу) адаптивно —
  когда одна сторона «убежала» (детект счётчиком последовательных advance); для равномерных данных
  gallop вреден, для skew — превращает O(log m) в per-group.
- Статистика: runtime-гистограмма размеров групп (p50/p99/max, доля строк в группах >1000) — сигнал
  для gallop и данные для cost-модели (предсказание cartesian-взрывов + warnings).
- Декартов взрыв — проблема вывода: метрика output rows / input rows — must.

## 7.4. Сортировка входов: три сценария

1. **Вход уже отсортирован** (Iceberg sort order) — SMJ = O(N+M) sequential; безусловно лучше hash
  по CPU и памяти. Planner должен видеть sortedness (отдельная работа).
2. **Сортировка внутри оператора (PagesIndex)**: quicksort по адресам — без копирования, но случайный
  доступ на сравнение × N log N. С flat-ключами — **radix sort по long[] (ключ+адрес)**: sequential,
  cache-friendly, без сравнений (доказано Spark). Главный аргумент материализовать ключ ещё на сортировке.
3. **Отдельный SortNode (OrderByOperator)**: лишняя материализация, но переиспользуемый и обкатанный.

**SMJ vs hash по CPU**: hash build = N хэшей + N случайных вставок; probe = хэш + 1-2 случайных
lookup'а на строку — на данных > LLC это 100-300 циклов/строку из миссов. SMJ после сортировки —
sequential scan, ~1 мисc на страницу, prefetch работает. Практическое правило:
**оба входа больше LLC и хотя бы один pre-sorted → SMJ выигрывает; всё влезает в кэш → hash почти
непобедим; pre-sorted входы + несколько join'ов по одному ключу → сортируем один раз, merge'им k раз —
доминирует SMJ.** Плюс FULL OUTER у SMJ бесплатно.

## 7.5. Спилл

- **Формат**: PagesSerde + LZ4 (не ZSTD — на локальном NVMe декомпрессия доминирует). Десериализация —
  главная скрытая цена (полный rebuild блоков).
- **Спиллить ключи отдельно от payload**: key-stream (flat long[] ключи + rowId, сортированные — жмутся
  до копеек) + payload-stream (страницы как есть). Merge-фаза читает только key-stream, payload
  подтягивается при сборке выхода и сразу уходит в словарные выходные страницы.
- **Prefetch**: double buffering поверх FileSingleStreamSpiller (асинхронное чтение рана k+1, пока
  мерджим ран k) — на десятках млрд строк разница между I/O-bound и CPU-bound.
- **Раны**: спилл-раны уже отсортированы → восстановление = k-way merge (PQ уместен при k>2, на плоских
  ключах — loser-tree/tournament); **не пересортировать после unspill**.
- Память словарных выходов может держать source-страницы дольше оператора — при ревокации уметь
  флаттенить выданные страницы или учитывать в downstream-контексте.

## 7.6. План бенчмарков

**JMH-микро**: компараторы (MH vs generated vs flat-long; BIGINT/VARCHAR/ROW; dictionary/RLE/flat);
merge-цикл (2-way loop vs PQ, gallop on/off; uniform, zipf s=1.1/2.0, одна гига-группа); сборка выхода
(Dictionary vs PageBuilder; payload 2/10/50 колонок × 8/100 Б); спилл-сериализация (ключи отдельно vs
вместе; LZ4 vs ZSTD; с/без prefetch).

**Макро**: TPC-H (Q3/Q10/Q12/Q21), TPC-DS join-heavy (Q17/Q26/Q72), синтетика 1/10/100 млрд строк
(спилл-режимы), «pre-sorted Iceberg» vs shuffle, skew-сценарии (zipf, «один ключ = 1% таблицы»).

**Метрики**: rows/s и **cpu-time/row** (главный критерий), alloc B/row + GC-паузы (JFR), `perf stat`:
LLC-load-misses/row, IPC; spill bytes + re-read factor + I/O utilization; memory peak/row.
Критерий выбора hash vs SMJ: паритет по памяти при меньшем cpu/row на >LLC данных и явная победа на
pre-sorted — фиксировать как пороги cost-модели.

**Добавить в JoinStatisticsCounter/OperatorStats**: число сравнений (prefix-hit/tie/fallback),
гистограмма размеров групп, строк через dictionary vs copied, gallop-события, число merge-runs и
re-read factor, время по фазам (sort/materialize/merge/output).

## 7.7. Топ-5 must-have оптимизаций и анти-оптимизации

**Must-have:**
1. **Flat 64-bit prefix ключ** (реюз FlatHashStrategy-энкодингов) — питает merge, radix sort, gallop,
   компактный спилл; всё остальное вторично.
2. **Словарная сборка выхода** (DictionaryBlock/RLE поверх входов) + compaction-пороги и корректный
   memory-ownership.
3. **Специализированный 2-way merge-цикл** без PQ/Iterator/boxing, resumable по yield, group-range
   итерация вместо per-row поисков.
4. **Radix sort входов по плоскому ключу** вместо quicksort со случайными доступами; спилл сортированными
   ранами + k-way merge.
5. **Ключи отдельно от payload при спилле + prefetch ранов** (double buffering).

**Анти-оптимизации (не делать на JVM):**
- Не выносить merge-цикл в off-heap/unsafe «ради скорости» (JIT отлично инлайнит long[]-циклы; off-heap —
  только spill-буферы).
- Не полагаться на Vector API без gating по SimdSupport (эмуляция медленнее скаляра).
- Не допускать мегаморфных каллсайтов и MH-вызовов во внутреннем цикле; не аллоцировать на строку.
- Не генерировать классы на каждый план-инстанс без кэша по ключу сортировки (Metaspace).
- Не копировать данные «на всякий случай» (превентивный flatten, double-buffering через copy); гигантские
  группы детектить по гистограмме и капить output-страницами, а не пытаться буферизовать.
