# Stage 2 — Оператор merge join (пакет `io.trino.operator.join.merge`)

PR2 фазы 1 ([risks/04 §4.2 п.2](../../smj-research/risks/04-implementation-plan-with-risk-gates.md);
дизайн [05 §5.1-5.6](../../smj-research/05-brainstorm-java-developer.md)). Можно вести параллельно
со stage 1 (тесты кормят оператор отсортированными страницами напрямую, без план-модели).

## Цель

Рантайм-оператор: bridge-модель двух пайплайнов, конечный автомат merge (05 §5.4), MatchBuffer
(05 §5.6), NaN/NULL-guards с первого коммита.

## Состав работ

| Компонент | Решение | Якорь |
|---|---|---|
| `MergeJoinBufferOperator(Factory)` | build-сторона: push-`Operator` (как `HashBuilderOperator`), копит pages, **вход уже отсортирован** (SortNode выше по пайплайну — ставит stage 4), публикует мост `MergeJoinSource` через `JoinBridgeManager<MergeJoinBridge>` (рефкаунт-lifecycle бесплатно). **Контракт `JoinBridge` — 3 метода**: `whenBuildFinishes()` (готово при публикации источника), `destroy()`; `getOuterPositionIterator()` в MVP — заглушка (вызывается только при outer-operator lifecycle, т.е. RIGHT/FULL — stage 5) | 05 §5.1/§5.2; JoinBridge.java (3 метода) |
| `MergeJoinOperator(Factory)` | probe-сторона: `WorkProcessorOperator` + **`WorkProcessorOperatorAdapter.Factory` — он уже `implements OperatorFactory, JoinOperatorFactory`** (там же комментарий, зачем нужен JoinOperatorFactory) — фабрика probe напрямую пригодна для `createOuterOperatorFactory`-подобной сборки | 05 §5.1; WorkProcessorOperatorAdapter:46-47 |
| Конечный автомат merge | состояния/переходы по 05 §5.4: init → advanceL/advanceR → groupMatch (compare==0, non-NULL) → drain; **NaN-guard**: перед трактовкой compare==0 как матч — NaN-проверка ключей (NaN-группы не матчатся, C1); **NULL-fast-path**: NULL-группа никогда не буферизуется (INNER — O(1)-skip хвостов обеих сторон; LEFT — стриминг левого хвоста с padding, правая не читается) | C1 (risks/01 §1.1), C9 (risks/06) |
| MatchBuffer | буфер текущей build-группы (05 §5.6): addresses+pages для residual-фильтра (`JoinFilterFunctionFactory.create(session, LongArrayList, List<Page>)` — SyntheticAddress-адресация) | I8 (risks/02 §2.8); JoinFilterFunctionCompiler:257-259 |
| residual filter | фильтр через `JoinFilterFunction`; LEFT: строка со всеми отфильтрованными матчами → padded-вывод | 05 §5.5 |
| `outputSingleMatch` | реюз вычисления `isMaySkipOutputDuplicates() && правые выходы ⊆ правых equi-символов` | LocalExecutionPlanner:2911 |
| Статистика | `JoinOperatorType.mergeJoin(...)` (класс со static-фабриками) + `JoinStatisticsCounter` + operatorType-строка WebUI; счётчики: вход/выход строк, сравнений, размер макс. группы, skipped-NULL/NaN | I10 (risks/02 §2.10, D3) |
| Ветвление в LEP | `if (node.getAlgorithm() == MERGE) return createMergeJoin(...)` до `createLookupJoin` (скелет по образцу :2879; вызов — из `visitJoin:2539`, все join-типы сегодня идут через `createLookupJoin` :2558) | 09 §9.5 |

## Тесты / Acceptance (TestMergeJoinOperator по образцу TestHashJoinOperator)

- все типы MVP (INNER/LEFT) × {дубли ключей, NULL-ключи 0/1/50/95/100%, NaN/-0.0/+0.0/Inf,
  пустые стороны, RLE/dictionary, multi-page группы, residual (вкл. отсекающий всё),
  outputSingleMatch вкл/выкл};
- NULL-хвост 10⁹ строк при `SMALL_MEMORY_POOL_BYTES` (TestHashJoinOperator:127) — память
  оператора плоская (C9-гейт);
- oracle против hash join (мультимножества, §1.8);
- `finish()` идемпотентен; отмена/fail-during-merge не течёт spill-файлами (I12);
- DOUBLE/REAL ключи в MVP **не поддержаны** (eligibility stage 3 отфильтрует; guards-тесты
  включения — отдельный follow-up после стабилизации C1-guards).

## Откат

Ветка в LEP не активна, пока stage 3 не выбирает MERGE (план без маркера = createLookupJoin).

## Согласованность (проверено против корпуса)

- [x] **05 §5.1-5.6**: модель (build=push, probe=WorkProcessor, мост, автомат, MatchBuffer,
  edge-кейсы) — состав работ реализует её дословно; нейминг приведён к канону
  `join.merge`/`MergeJoin*` (фикс R2 пятого прохода — аннотация в 05 §5.1 учтена);
- [x] **risks/01 §1.1-1.8** (C1-C4, §1.8 мультимножества) и **risks/06** (C9) — guards/fast-path
  в составе автомата; NULL-порядки канонизированы (A4: ASC NULLS_LAST);
- [x] **risks/02 §2.8 (I8)**: addresses+pages для фильтра; **§2.10 (I10)**: статистика;
  **§2.12 (I12)**: fail-during-merge в acceptance;
- [x] **risks/04 §4.2 п.2**: DF-обёртка build-пайплайна (`createDynamicFilterSourceOperatorFactory`
  над sink, :2959-паттерн) — переносится в stage 4 (когда появляется build-пайплайн в LEP);
  здесь оператор DF-интерфейсов не реализует;
- [x] **09 §9.5**: «createMergeJoin по скелету createLookupJoin (2880+), probe в основном
  контексте, build в [отдельном]» — совпадает;
- [x] **#12618 (план 04 §4.0 п.4)**: один оператор, без spilling/nonspilling-форка —
  спилл-ветвление внутри (stage 5).
