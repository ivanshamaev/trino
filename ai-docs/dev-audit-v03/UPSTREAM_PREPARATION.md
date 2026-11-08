# Подготовка review после v03

Рабочий scope — личный experimental MVP. Подготовка локальной истории и PR draft не является публикацией PR, подписанием CLA или согласованием scope с maintainers.

## История

Для исправлений используется отдельная локальная `smj-upstream-v03` поверх `smj-upstream-v02@a0a11bff2fa`. Четыре исходных review-коммита сохранены. Дополнительные границы:

1. `Preserve merge join contracts in late rewrites`: сохранение algorithm, guards aggregation pushdown, property derivation, final validator и SQL/rule regressions.
2. `Encrypt merge join match spill state`: native encryption policy для bitmap, bounded encrypted blocks, accounting/lifecycle и соответствующие regressions.

Фактические SHA, изменённые paths и результаты промежуточных builds — [review-series.json](implementation-evidence/review-series.json), [review-results.json](implementation-evidence/review-results.json). История `smj-plan` не переписывается. В review branch не включаются private ai-docs и логи аудита.

База исходной review-серии — закреплённая `8ae1f91f85313e50fd72286810c52f4b058b1292`. Это проверяемая локальная база, а не утверждение об актуальности GitHub master в момент будущей публикации. Перед реальным submission нужно проверить выбранный upstream SHA и CI.

## PR draft

Текст ниже следует разделам локального pull_request_template.md. Внешние issue/PR numbers не выдумываются.

### Description

Add an experimental sorted merge join for partitioned equi-joins. Build groups larger than memory can be replayed from local spill using bounded retained state. INNER, LEFT, RIGHT and FULL joins support residual predicates and preserve unmatched rows.

Selection defaults to OFF. FORCE and PREFER select eligible joins. COST_BASED applies a local resource heuristic after join order and distribution selection. Each task uses one merge driver; task retries, hot-key range splitting and joint enumeration of join order and algorithm are outside this implementation.

The bounded external sorter is explicitly enabled for merge-join sorts. Iceberg can report verified identity prefixes of file sort orders, with conservative handling of absent or mixed metadata. Late rewrites preserve the physical join contract, which is validated after optimization. Spill encryption also protects the outer-join match bitmap.

### Additional context and related issues

The intended benefit is execution under memory pressure, rather than a general latency improvement over hash join. Managed reservations are not a whole-JVM heap cap. Automatic selection depends on approximate statistics and does not provide memory admission guarantees, particularly for highly compressible variable-width data.

This is a personal MVP. Upstream design scope has not been agreed and the author's CLA has not been signed. The local audits, tests and prepared commit series do not establish maintainer approval. Validation commands, exact test selections and limitations are recorded with the implementation results.

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

Фактический номер PR добавляется к release-note entries при публикации. Этот документ не создаёт issue/PR и не отправляет сообщения другим участникам проекта.
