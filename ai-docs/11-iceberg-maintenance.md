# 11. Обслуживание Iceberg: процедуры, DDL, системные таблицы

Код: `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/{procedure,system}` + методы
`IcebergMetadata` (`getTableHandleForExecute`, `beginTableExecute`, `executeTableExecute`,
`finishTableExecute`).

## 11.1. Процедуры

### Табличные процедуры — `ALTER TABLE ... EXECUTE <name>(...)`

Регистрация: `Multibinder<TableProcedureMetadata>` в `IcebergModule`. Только **OPTIMIZE**
распределённая (`distributedWithFilteringAndRepartitioning`), остальные — `coordinatorOnly()`.

| Процедура | Свойства | Что делает |
|---|---|---|
| `optimize` | `file_size_threshold` (100MB), `sorted_by` | Компактация мелких файлов: `RewriteFiles` — старые data-файлы + полностью применённые delete-файлы удаляются, пишутся новые; `dataSequenceNumber` наследуется; возвращает `rewritten_data_files_count`, `removed_delete_files_count`, `added_data_files_count` (подробнее [09, 9.7](09-iceberg-write-path.md)) |
| `optimize_manifests` | — | `RewriteManifests` с `clusterBy` по первой партиционной колонке (≤200 кластеров) — манифесты перезаписываются ради manifest pruning |
| `expire_snapshots` | `retention_threshold` (7d), `retain_last` (≥1), `clean_expired_metadata` (false) | `table.expireSnapshots().expireOlderThan(...).retainLast(...).cleanExpiredMetadata(...)`; проверка `gc_enabled` и минимального retention |
| `remove_orphan_files` | `retention_threshold` (7d) | Сбор «валидных» имён: manifest list всех снапшотов, file_path из всех манифестов, metadata-файлы (`ReachableFileUtil`), statistics files, `version-hint.text`; удаление прочих файлов старше retention, батчами по 1000 |
| `drop_extended_stats` | — | `removeStatistics` для всех statistics-файлов; возвращает `removed_statistics_count` |
| `rollback_to_snapshot` | `snapshot_id` | `manageSnapshots().setCurrentSnapshot(...)` |
| `add_files` | `location`, `format`, ... | Импорт существующих Parquet-файлов в таблицу без копирования (метрики из футеров); гейт `iceberg.add-files-procedure.enabled` |
| `add_files_from_table` | `source_table`, ... | То же из другой таблицы |

Для storage-таблиц materialized views разрешены только: OPTIMIZE, OPTIMIZE_MANIFESTS,
EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, DROP_EXTENDED_STATS.

### CALL-процедуры (`system.<name>`)

- `system.register_table(schema, table, table_location, metadata_file_name?, metadata_location?)` — регистрация внешней таблицы по metadata.json (гейт `iceberg.register-table-procedure.enabled`).
- `system.unregister_table(schema, table)` — удалить ссылку из каталога, не трогая файлы.
- `system.rollback_to_snapshot(schema, table, snapshot_id)`.
- `system.migrate(schema, table)` — только HMS-каталог: конвертация Hive-таблицы в Iceberg (in-place, свойство `provider=trino_migrate` в коммите).

## 11.2. ALTER TABLE (поддерживаемые операции)

`IcebergMetadata`:
- **Колонки**: `addColumn` (топ-уровень и nested, с позиций; Trino сам назначает field id),
  `dropColumn`, `renameColumn` (включая вложенные), `setColumnComment`.
- **Свойства** (`setTableProperties`, `UPDATABLE_TABLE_PROPERTIES`): `format` (default file format),
  `format_version` (2↔3: `UpdateProperties.set(FORMAT_VERSION)` — Iceberg сам мигрирует метаданные),
  `compression_codec`, `target_max_file_size`, `parquet_writer_row_group_size`,
  `object_store_layout_enabled`, `data_location`, `gc_enabled`, `max_commit_retry`,
  `delete_after_commit_enabled`, `max_previous_versions`, `max_inferred_metrics_column_defaults`,
  bloom-фильтры (ORC/Parquet), `extra_properties` (белый список).
- Комментарии таблицы; авторизация/ренейм — на уровне каталога.
- **Смена партиционирования через ALTER не поддерживается** — новая спека применяется только к файлам, записанным после `CREATE OR REPLACE`/новых спек Iceberg.

## 11.3. Системные таблицы (`system/`)

Доступ: `SELECT * FROM catalog.schema."table$<type>"`. Диспетчеризация — `IcebergMetadata.getRawSystemTable`.

| Таблица | Класс | Колонки (ключевые) |
|---|---|---|
| `t$snapshots` | `SnapshotsTable` | `committed_at, snapshot_id, parent_id, operation, manifest_list, summary (map)` — из `table.snapshots()` |
| `t$history` | `HistoryTable` | `made_current_at, snapshot_id, parent_id, is_current_ancestor` |
| `t$manifests` | `ManifestsTable` | `content, path, length, partition_spec_id, added_snapshot_id, added/existing/deleted_data_files_count, added/existing/deleted_rows_count, partition_summaries (contains_null/nan, lower/upper)` |
| `t$all_manifests` | `AllManifestsTable` | как `$manifests` + delete-счётчики + `reference_snapshot_id`; скан через Iceberg metadata-table scan |
| `t$files` | `FilesTable` (distributed) | `content (0=data/1=deletes), file_path, file_format, spec_id, partition, record_count, file_size_in_bytes, column_sizes, value_counts, null/nan_value_counts, lower/upper_bounds, split_offsets, equality_ids, sort_order_id, readable_metrics (JSON), data_sequence_number, first_row_id, content_offset/size (DV)`; сплит — на манифест (`FilesTableSplitSource` → `FilesTablePageSource`) |
| `t$entries` / `t$all_entries` | `EntriesTable` | `status, snapshot_id, sequence_number, data_file (row: ...), readable_metrics`; через Iceberg `ENTRIES`/`ALL_ENTRIES` scans |
| `t$properties` | `PropertiesTable` | `key, value` (reserved: provider, format, current-snapshot-id, location, format-version, sort-order...) |
| `t$refs` | `RefsTable` | `name, type, snapshot_id, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms` |
| `t$metadata_log_entries` | `MetadataLogEntriesTable` | `timestamp, file, latest_snapshot_id, latest_schema_id, latest_sequence_number` |
| `partitions` (view) | `PartitionsView` | генерируемый SQL: `SELECT partition, SUM(record_count), COUNT(*), SUM(file_size_in_bytes), <min/max/null/nan per column> FROM t$files GROUP BY 1` |
| `system.iceberg.tables` | `IcebergTablesSystemTable` | глобальный список Iceberg-таблиц |

`$files` — единственная распределённая системная таблица (`Distribution.ALL_NODES`);
поддерживает пер-табличные credentials (`getTableCredentials`).

## 11.4. Материализованные представления (сводка)

- Определение — `IcebergMaterializedViewDefinition` (JSON+Base64 в комментарии view): originalSql, catalog/schema, columns, grace period, when-stale-behavior, `path` (зависимости для incremental refresh).
- Storage — Iceberg-таблица `{name}$materialized_view_storage` (скрыта: `iceberg.materialized-views.hide-storage-table=true`).
- Freshness: `getMaterializedViewFreshness` — по storage snapshot / last-refresh summary.
- Incremental refresh и экспайр снапшотов — см. [09, 9.6](09-iceberg-write-path.md).

## 11.5. Табличная функция `table_changes`

`SELECT * FROM TABLE(system.table_changes(catalog => 'iceberg', schema => ..., table => ..., since_snapshot_id => ..., up_to_snapshot_id => ...))` — changelog между снапшотами через
Iceberg `newIncrementalChangelogScan()` (`functions/tablechanges/TableChangesSplitSource`).
Ограничение: `DeletedRowsScanTask` не поддерживается (TODO в коде).
