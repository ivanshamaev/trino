# 07. FULL OUTER JOIN в SMJ: стриминг null-padding без накопления стейта

Контекст вопроса: будет ли логика генерации пустых значений (null-padding) стримиться так же
эффективно, как INNER, без накопления состояния в памяти?

## 7.1. Короткий ответ

**Да.** SMJ — однопроходный стриминг по обеим сторонам; null-padding — это O(1)-эмиссия строки
с пустыми полями другой стороны, состояние не копится **по построению**. Более того, FULL OUTER
для SMJ **проще**, чем для hash join: в hash FULL движок буферизует позиции несовпавших build-строк
(`OuterPositionIterator`, собирается через `JoinBridgeManager.getOuterPositionsFuture()` —
потребители `PartitionedLookupSourceFactory`/`OuterLookupSource`) и пере-итерирует build после
конца probe. SMJ ничего из этого не требует.

## 7.2. Merge-цикл для FULL (однопроходный)

| Ситуация | Действие | Память |
|---|---|---|
| leftKey < rightKey | emit(left, null-padding) | O(1) |
| leftKey > rightKey | emit(null-padding, right) | O(1) |
| leftKey == rightKey (non-NULL) | cross-product внутри группы (MatchBuffer на правую группу) | O(текущая группа) |
| probe исчерпан, build нет | **drain хвоста build** с padding по порядку | O(1) на строку |
| build исчерпан, probe нет | drain хвоста probe с padding (стриминг) | O(1) |
| NULL-группы (хвост) | стриминг по C9/R-C9.1, не буферизуются | O(1) |

Пик памяти = сортировочные буферы (revocable) + текущая группа + текущая левая группа —
**симметрично INNER на тех же данных**; ноль «отложенного стейта» вида hash-OuterPosition.

## 7.3. Тонкости (почему FULL — фаза 2, а не MVP)

1. **Residual filter + outer-семантика**: если все правые кандидаты левой строки отфильтрованы,
   левая строка обязана выйти с padding («заматчилась, но не выжила»). Требуется флаг
   `anyMatchSurvivedFilter` на текущую левую группу — O(1) на группу, стримингово.
   (В hash эта логика — `OuterPositionIterator` + до-проверка фильтра.)
2. **Спилл групп (фаза 2)**: FULL требует re-iterate **обеих** сторон группы: правая — для
   cross-product, левая — для проверки «был ли выживший матч» (P4-амплитфикация актуальна
   вдвойне: bytes-read ≤ L + R·(L/C) обязан выполняться и тут).
3. **Порядок вывода**: SMJ-FULL выдаёт строки в порядке merge (левые/правые interleaved) —
   семантически корректно (JOIN не гарантирует порядок), но oracle-тесты сравнивают
   мультимножествами (§1.8), не последовательностью.
4. **Bridge-lifecycle**: `JoinBridgeManager.outerOperator*`-механика (для hash-FULL) SMJ не
   нужна; bridge используется только для refcount/lifecycle build-стороны (как в INNER).

## 7.4. RIGHT OUTER

Зеркален LEFT: правые несовпавшие строки выходят в момент прохода build (probe-курсор ушёл
вперед — emit(padding, right)). Drain-хвост build после конца probe — тот же механизм, что FULL.
Реализуется тем же outer-driver'ом (фаза 2).

## 7.5. Acceptance-критерии (включены в план §4.3)

- **Zero-retention**: пик памяти FULL == пик INNER на тех же данных (метрика оператора);
  отсутствие роста памяти на гига-хвостах (probe-хвост 10⁹ без матчей);
- гига-NULL-хвост + FULL (C9-стриминг) при малом пуле — без OOM;
- residual-filter-отсекающий-всё + FULL → все строки с padding (oracle);
- спиленный re-iterate 10M×10M + FULL: bytes-read ≤ L + R·(L/C).

## 7.6. Вывод

Null-padding в SMJ стримится с O(1)-стейтом — архитектурно FULL OUTER для SMJ **выгоднее**, чем
для hash (нет OuterPositionIterator, нет пере-итерации build). Ограничение MVP (INNER/LEFT) —
чисто инженерное (residual+спилл-группы), не фундаментальное.
