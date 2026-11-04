# Stage 0 — Iceberg sortedness (enabler)

Фаза 0 плана [risks/04 §4.1](../../smj-research/risks/04-implementation-plan-with-risk-gates.md).
Самостоятельно ценен (выгоды TopN/Window уже сегодня), не требуется для forced-MVP.

## Цель

`IcebergMetadata.getTableProperties` сообщает `SortingProperty` консервативно и никогда не лжёт
(C7: ложный отчёт = неверные результаты). Гарантия «сплит = отсортированный диапазон» (порядок
чтения сплитов одного файла — по offset).

## Состав работ

| Файл | Изменение | Якорь |
|---|---|---|
| `plugin/trino-iceberg/.../IcebergMetadata.java` | `getTableProperties`: вместо `// todo support sorting properties` + `ImmutableList.of()` — вычисление SortingProperty | метод :1003, todo ~:1085 |
| — // — | критерий: **все** data-файлы снапшота (после прунинга) с file-level `sortOrderId == table.sortOrder().orderId()` (из манифестов, не табличный — C8); все поля порядка — identity; сообщаем валидный identity-**префикс** (sorted by (a,b) ⇒ свойство (a) валидно) | `getSupportedSortFields`:1740, `table.sortOrder()`:1725, `TrinoSortField`; доступ к файлам — `tableScan.planFiles()` уже есть в `getTableProperties` (FileScanTask → `sortOrderId()`; паттерн использования — :1803/:2309) |
| — // — | гарантия «сплит = отсортированный диапазон»: в Trino-Iceberg **1 сплит = 1 целый файл** (`toIcebergSplit(FileScanTaskWithContext)`: один `location/start/length`, без комбинирования файлов; `CombinedScanTask` в склейке не участвует) ⇒ каждый сплит отсортированного файла — валидный диапазон; **глобальный порядок = k-way merge сплитов** (LOCAL merging — stage 4 generic-путь; per-bucket merge — S1-GOLD fast-path) | IcebergSplitSource `toIcebergSplit` (:911+); план 04 §4.1 |
| `IcebergSessionProperties`/`IcebergConfig` | kill-switch `iceberg.report-sortedness` (default true — после стабилизации) | — |

## Тесты / Acceptance (из рисков C5/C7/C8)

- fuzz «отчёт sortedness vs фактический порядок файлов» на матрице
  {file-level sortOrderId ∈ 0/current/mixed; identity/truncate/bucket поля; deletes on/off;
  многосплитовые файлы} — **0 ложных отчётов**;
- потребители работают проверяемо: `AddExchanges.visitTopN` PARTIAL→Limit на pre-sorted входе
  (код уже содержит ветку), `PropertyDerivations.visitWindow` pre-sorted окна;
- опциональная deliverable: новое iterative-правило элиминации `SortNode` над сканом с
  удовлетворяющей SortingProperty (~50 строк; правило `RemoveRedundantSorts` в Trino **не
  существует** — пишем с нуля);
- mixed-версионный product-test не нужен (SPI не меняется).

## Откат

`iceberg.report-sortedness=false`.

## Согласованность (проверено против корпуса)

- [x] **10-synthesis §10.5** (строка «0. Iceberg sortedness»): критерии совпадают —
  RemoveRedundantSorts упомянут как «не существует, пишем с нуля» (фикс D8 третьего прохода);
- [x] **risks/01 §1.5/§1.7** (C5, C7): per-file критерий + kill-switch; **risks/05 §5.3 C8**:
  file-level sortOrderId + префиксная отчётность — состав работ включает оба;
- [x] **risks/04 §4.1**: состав/acceptance/откат зеркалируют фазу 0 дословно;
- [x] **15 §15.3**: `sortedL/sortedR` читаются из SortingProperty, произведённой этим этапом —
  контракт совпадает (канон ASC NULLS_LAST не навязывается коннектору: матчинг через
  `LocalProperties.match`, требование полного покрытия equi-ключей);
- [x] **14 §14.1 / 11 §11.3**: условия S1-GOLD (file-level sortOrderId, identity, префикс) —
  те же, что в критерии этапа.
