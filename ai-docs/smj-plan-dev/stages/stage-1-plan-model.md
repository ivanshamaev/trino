# Stage 1 — План-модель: `JoinNode.algorithm`

PR1 фазы 1 ([risks/04 §4.2 п.1](../../smj-research/risks/04-implementation-plan-with-risk-gates.md);
нарезка [09 §9.7 п.1](../../smj-research/09-brainstorm-trino-developer.md)). Ничего не включает —
чистая модель. Стартует одновременно со stage 0.

## Цель

`JoinNode` получает `Optional<JoinAlgorithm>` (enum `MERGE`), Jackson-сериализацию и инварианты;
компилятор гарантирует проброс поля во всех 45 местах пересборки (I13).

## Состав работ

| Файл | Изменение | Якорь |
|---|---|---|
| `core/trino-main/.../plan/JoinNode.java` | 1) внутренний `public enum JoinAlgorithm { MERGE }` — по прецеденту `DistributionType` (:52); 2) поле `Optional<JoinAlgorithm> algorithm`; 3) параметр **в единственном публичном `@JsonCreator`-конструкторе** (:78), без дефолт-перегрузок — приватный полный (:105) только делегирует; 4) `withAlgorithm(...)`, геттер; 5) инварианты: `MERGE ⇒ distributionType=PARTITIONED ∧ type ∈ {INNER,LEFT} ∧ criteria непусты` | конструкторы :78/:105; прецеденты полей `spillable`, `maySkipOutputDuplicates` |
| 45 мест `new JoinNode(` | механический проброс (большинство — `Optional.empty()`); особые: `JoinNode.flipChildren` (:170) и `with*` — сохраняют поле | grep-счёт 45 (main), стабилен |
| `PlanPrinter`/`ExplainPrinter` | печать `algorithm` **только при наличии** (OFF-планы не меняются) | I10 |
| Тесты | golden-JSON по прецеденту `TestPatternRecognitionNodeSerialization`; трансформационные golden: UnaliasSymbolReferences / PredicatePushDown / flipChildren не теряют поле; unit инвариантов | I13, I9 |

## Тесты / Acceptance

- сериализация round-trip; старый JSON (без поля) → `empty` (mixed-кластер I9);
- все 45 мест обновлены (компилятор), CI план-диффы OFF = 0;
- инварианты: попытка `MERGE` с REPLICATED/пустыми criteria → `IllegalArgumentException` в канонических фабриках.

## Откат

Поле аддитивное (default empty) — откат = не читать его дальше (stage 3 не активируется).

## Согласованность (проверено против корпуса)

- [x] **risks/04 §4.2 п.1 / §4.9 п.2**: конструкторная механика (@JsonCreator без перегрузок —
  фикс U1 шестого прохода), enum-прецедент `:52`, нейминг — совпадают дословно;
- [x] **09 §9.1/§9.7**: «новый параметр в @JsonCreator (прецеденты роста — spillable,
  maySkipOutputDuplicates)» — тот же подход;
- [x] **risks/README I13/I9/I10**: митигации (канонический конструктор→@JsonCreator-механика,
  @JsonCreator-дефолт для mixed, печать только при наличии) — включены в состав;
- [x] **openai/06 ADR1**: рекомендуют то же поле + обязательный аудит copy/flip (= наши
  golden-тесты) — расхождений нет;
- [x] **base 04-joins.md**: перечисление полей JoinNode (distributionType, spillable,
  maySkipOutputDuplicates, reorderJoinStatsAndCost) — новое поле аддитивно к списку.
