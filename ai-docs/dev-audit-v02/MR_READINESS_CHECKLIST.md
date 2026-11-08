# Условия готовности SMJ к upstream MR

Источник критериев: [REPORT.md](REPORT.md) и [CONCEPT_REVIEW.md](CONCEPT_REVIEW.md), исходный HEAD `773afdee3bd`. Отметки ниже обновлены по выполненным исправлениям; доказательства и ограничения — в [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md). Исходный checklist v01 сохранён.

Дополнительные требования к оформлению: [GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md).

## Исправления до MR

- [x] V02-01: null/unknown file sort order безопасно дают отсутствие sortedness; обычный SELECT с SMJ OFF проходит на legacy/mixed Iceberg metadata.
- [x] V02-02: TestIcebergConfig defaults и full mappings проходят; config/session descriptions и пользовательская документация добавлены.
- [x] V02-03: физический sort нормализует повторяющиеся equi-символы, сохраняя все JOIN constraints; FULL SQL reproducer и зеркальная матрица слева/справа проходят.
- [x] V02-04: group replay использует точную позицию начала без растущего чтения от редких checkpoints; sweeps 768/3072/12288+ страниц на группах 2/3/4 страницы показывают линейную дополнительную работу при линейном output.
- [x] V02-05: scan/discovery/replay/drain регулярно проверяют yield и сохраняют состояние; rejected residual и cancel tests не выполняют всю группу в одном вызове.
- [x] V02-10: RIGHT dynamic filters имеют producer и завершаются, либо согласованно удалены из final plan вместе с consumers; distributed regression проверяет stats и результат.

## Контракты и доказательства

- [x] V02-06: JSON round-trip, merge операторной статистики и сохранение её в final query info проверены.
- [x] V02-07: memory contract явно различает reservation и фактический heap; output/serde/page overhead измерен или учтён; формулировки acceptance приведены в соответствие измерениям.
- [x] V02-08: bitmap temp files охвачены штатной безопасной startup cleanup; оставленный после аварии файл удаляется тестом восстановления.
- [x] V02-09: expression-only rewrite сохраняет algorithm; phase contract намеренных сбросов и применимость post-selection rules проверены.

## Scope и приёмка изменений

- [x] Документированы поддержанные scalar types, четыре join type, только PARTITIONED и retry NONE; multi-driver/FTE/range/SemiJoin-specific support не выдаются за реализованные возможности.
- [x] Iceberg compatibility matrix включает старые/mixed/null IDs, sort evolution, transforms, deletes, несколько splits, TopN/Window/aggregation и SMJ OFF.
- [x] Общий SortingSpiller вынесен в понятную отдельную границу изменения или его влияние на spill-enabled ORDER BY при SMJ OFF подтверждено benchmarks и regression tests.
- [x] Cost model согласована с исправленным replay и реальными sort memory/I/O; отдельно определён scope локальной эвристики и глобальных CostCalculator/EXPLAIN costs.
- [x] Есть distributed sweeps около порогов, hot-key skew, join chains, малый query memory, конкурентные SMJ и delayed branches; сравнительная польза подтверждена end-to-end данными.
- [x] Пройдены штатные config tests, расширенные regressions изменённых модулей и требуемые CI verify/Error Prone проверки. Точные наборы и skips описаны в IMPLEMENTATION_RESULTS.md; полный JUnit suite монорепозитория не запускался.
- [x] Пользовательские properties, release notes и EXPLAIN/metrics соответствуют окончательному поведению; подготовлено разбиение MR на reviewable изменения.
- [x] История предлагаемого PR проходит закреплённый check-commit-messages; каждый промежуточный commit собирается. Исходные 17 отклонений smj-plan не исправляются force-push: подготовлена отдельная локальная smj-upstream-v02 без private audit files.
- [x] Устранены source diagnostics Error Prone (G-07); отдельно прошли main и test compilation каждого изменённого модуля. Обычный verify уже прошёл, но его недостаточно.
- [x] Удалён дублированный header TestJoinNodeAlgorithm, имена/Javadoc tests соответствуют фактической проверке; выполнены согласованные правки readability.
- [x] Проверена совместимость с upstream `8ae1f91f853`: все четыре предлагаемых промежуточных дерева собраны, regression tests и verify прошли.
- [ ] Для будущего upstream submission: подписан Trino CLA и согласован scope крупного изменения с maintainers. Автор подтвердил, что CLA не подписан и scope не согласован; текущая цель — личный MVP, поэтому этот пункт не блокирует его завершение.

Для первоначального узкого experimental MR часть нагрузочных/глобальных CBO возможностей может быть согласованно отложена. Падающий обычный SELECT, штатные config tests, FULL planning exception и несоблюдение yield нельзя считать такой необязательной оптимизацией.
