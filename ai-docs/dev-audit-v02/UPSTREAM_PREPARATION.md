# Подготовка upstream PR

Текущий статус по сообщению автора: личный MVP. Trino CLA не подписан, scope SMJ с upstream не согласован. Ниже сохранён черновик на случай будущего submission; его публикация не входит в текущую задачу.

Исправления v02 ведутся отдельно от опубликованной истории `smj-plan`. История с 17 отклоняемыми сообщениями не должна отправляться upstream без подготовки. Force-push пользовательской ветки не требуется.

## Предлагаемые границы коммитов

1. `Add indexed replay for append-only spill streams` — opt-in disk index, append/read primitives, spill accounting и их tests. Обычные spillers не создают replay index.
2. `Add bounded spill runs for explicit merge sorts` — SortingSpiller, PagesIndex и OrderBy operator API/tests. По умолчанию ORDER BY сохраняет прежний final merge; bounded sorter включается явно.
3. `Add experimental partitioned sorted merge joins` — JoinNode marker, planner/exchanges, merge operator, group bitmap/replay, yield, memory, operator info, локальная cost heuristic, core/distributed plan tests.
4. `Report verified Iceberg file ordering` — консервативная sortedness, nullable/mixed metadata, config tests, connector integration и пользовательская документация.

Каждая граница должна проходить компиляцию main/test sources и применимые tests. Все четыре промежуточных дерева собраны, применимые regression tests прошли. Актуальные SHA и логи закреплены в [review-series.json](implementation-evidence/review-series.json); финальные profile checks — в [IMPLEMENTATION_RESULTS.md](IMPLEMENTATION_RESULTS.md).

## PR description draft

### Description

Add an experimental sorted merge join for partitioned equi-joins with retry policy NONE. Matching build groups can be replayed from local spill without retaining the full group in memory. INNER, LEFT, RIGHT and FULL joins support residual predicates and preserve unmatched rows.

Selection defaults to OFF. FORCE and PREFER select eligible joins; COST_BASED applies a local resource heuristic after join order and distribution selection. The feature uses one merge driver per task and does not implement task retries or joint enumeration of join order and algorithm.

The bounded sorter is explicitly enabled for merge-join sorts. Ordinary ORDER BY retains its existing final spill merge. Iceberg can independently report verified identity prefixes of file sort orders, with conservative fallback for missing or mixed metadata.

### Additional context and related issues

The intended benefit is execution under memory pressure, including build groups larger than memory. This does not imply a general latency improvement over hash join. Validation results, resource measurements and limitations must accompany the proposed changes.

Before publication, replace this paragraph with the actual upstream design discussion and links to agreed scope. A local implementation audit does not establish maintainer agreement or CLA status.

### Release notes

( ) This is not user-visible or is docs only, and no release notes are required.
( ) Release notes are required. Please propose a release note for me.
(x) Release notes are required, with the following suggested text:

```markdown
## General
* Add experimental partitioned sorted merge joins, disabled by default,
  with spill-backed group replay for memory-sensitive workloads.

## Iceberg
* Report verified file sort-order prefixes to the optimizer. Configure this
  behavior with `iceberg.report-sortedness` or the `report_sortedness`
  catalog session property.
```

При публикации maintainer/автор добавляет фактический номер PR к release-note entries по принятому формату. Черновик не публикуется автоматически.

## Внешние условия

- Подпись CLA должна относиться к действительному автору вклада. Её нельзя заменить изменением Java header или самостоятельно подтвердить от имени пользователя.
- Согласование upstream scope требует фактического обсуждения с maintainers. Подготовленный текст не является состоявшимся согласованием.
- Проверка актуального master выполняется по конкретному SHA и устаревает по мере новых upstream commits. Готовность к merge окончательно подтверждает CI предлагаемого PR.
