# 7. Метаданные Iceberg, транзакции и commit protocol

Код: `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/` (главный класс —
`IcebergMetadata.java`, ~4766 строк), `.../catalog/*`, патч `src/main/java/org/apache/iceberg/IcebergTransactions.java`.

## 7.1. Транзакции коннектора

- `IcebergConnector.beginTransaction(SERIALIZABLE, autocommit)` → `HiveTransactionHandle` → `IcebergTransactionManager.begin`.
- `IcebergTransactionManager` хранит `ConnectorTransactionHandle → MemoizedMetadata`; **на каждую транзакцию создаётся свой `IcebergMetadata` со своим `TrinoCatalog` (per-identity)** — это естественная изоляция и per-transaction кэш метаданных.
- Внутри `IcebergMetadata` живут «долгоживущие» состояния транзакции: `createTableTransaction` (staged create от `beginCreateTable` до `finishCreateTable`), `writeTable` (таблица, загруженная в `beginInsert`/`beginMerge`/...; после коммита сбрасывается — повторный коммит даёт `ICEBERG_COMMIT_ERROR "Write was already committed"`), `fromSnapshotForRefresh` (инкрементальный MV refresh).

## 7.2. IcebergTableHandle / IcebergColumnHandle

**`IcebergTableHandle`** — сериализуемое представление таблицы, с которым живёт весь запрос:
- `schemaName, tableName, TableType tableType, OptionalLong snapshotId`;
- `tableSchemaJson` — схема на конкретном снапшоте; `specId` + `Map<Integer,String> partitionSpecJsons` — все партиционные спеки; `formatVersion`; `tableLocation`; `storageProperties`;
- **`unenforcedPredicate` / `enforcedPredicate`** (`TupleDomain<IcebergColumnHandle>`) — результат `applyFilter`;
- `projectedColumns` (result `applyProjection`), `limit`, `nameMappingJson`, `tablePartitioning` (bucket execution, только координатор), `constraintColumns` (для `query_partition_filter_required`), `recordScannedFiles/maxScannedFileSize` (только OPTIMIZE), `forAnalyze`.

**`IcebergColumnHandle`**: `ColumnIdentity baseColumnIdentity` (дерево field-id), базовый тип, `List<Integer> path` (field id вложенной проекции), итоговый `type`, nullable, comment. Trino-специфичные id начинаются с `Integer.MIN_VALUE` (Iceberg reserved — с `Integer.MAX_VALUE` вниз).

## 7.3. Загрузка таблицы (`getTableHandle`)

- Имя матчится `IcebergTableName.TABLE_PATTERN`; не-DATA имена → null или системные таблицы.
- `catalog.loadTable(...)` → `BaseTable` (через `IcebergUtil.loadIcebergTable`: `new BaseTable(ops, quotedName, TRINO_METRICS_REPORTER)`); `TableNotFoundException` → null; `ICEBERG_MISSING_METADATA/INVALID_METADATA` → `CorruptedIcebergTableHandle` (lazy exception).
- **Time travel** (`FOR AS OF TIMESTAMP/VERSION`): `endVersion` — `TARGET_ID` (snapshot id или имя ref из `table.refs()`) либо `TEMPORAL` (timestamp → `getSnapshotIdAsOfTime`); для snapshot-специфичного чтения берётся `schemaFor(table, snapshotId)`.
- Скрытые метаданные-колонки (`IcebergMetadataColumn`, регистрируются в `getColumnHandles`):
  - `$partition` (VARCHAR row), `$path`, `$file_modified_time` (TIMESTAMP_TZ_MILLIS);
  - `$row_id` (BIGINT) и `$last_updated_sequence_number` (BIGINT) — только format version ≥ 3.

## 7.4. Commit protocol (AbstractIcebergTableOperations)

Все каталоги коммитят таблицу через **оптимистичную конкуренцию (OCC)**: условную замену
указателя `metadata_location` на новый metadata.json-файл.

`.../catalog/AbstractIcebergTableOperations.java` (расширяет Iceberg `TableOperations`):

1. `refresh(invalidateCaches)` — получить свежий `metadata_location` у каталога (`getRefreshedLocation`), прочитать metadata.json с ретраями (Failsafe: 3 попытки, backoff 100ms→5s, максимум 3 минуты; abort по `TrinoFileSystem.isUnrecoverableException` / `IcebergExceptions.isFatalException`); проверка совпадения UUID таблицы.
2. `commit(base, metadata)`:
   - `base != current()` → `CommitFailedException("stale table metadata")` → Iceberg-библиотека повторит весь `SnapshotUpdate`;
   - `writeNewMetadata(metadata, newVersion)` — файл `%05d-%s.metadata.json` (версия-uuid) в `{location}/metadata/`;
   - `base == null` (новая таблица) → `commitNewTable`; иначе `commitToExistingTable` (каталог-специфичный условный UPDATE) + удаление старых metadata-файлов по `delete-after-commit`/`previous-versions-max` (`CatalogUtil.deleteRemovedMetadataFiles`);
   - после коммита `shouldRefresh = true`.
3. `checkNewTableCommit/checkExistingTableCommit` — при исключении из каталога определяется фактический статус: если каталог уже указывает на наш metadata-location → SUCCESS (warn); таблица исчезла/rejected → `CommitFailedException` (ретрай) или `CreateTableException` (с удалением orphaned metadata); иначе `CommitStateUnknownException` (файлы не трогаем).

Ретраи на уровне Iceberg-библиотеки: `SnapshotUpdate`/`BaseTransaction` повторяют коммит при
`CommitFailedException`; число попыток — табличное свойство `commit.num-retries`
(в Trino: `max_commit_retry`).

**`org.apache.iceberg.IcebergTransactions`** (патч в плагине) — `newTransaction(BaseTable)`:
транзакция **без refresh** от уже загруженных метаданных — правильная семантика для
retry-повторов записи в `IcebergMetadata.finish*`.

Транслирование ошибок (`IcebergMetadata.commitUpdate/commitTransaction`):
`CommitFailedException` при commitTransaction → `TRANSACTION_CONFLICT`;
`UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException | RESTException` → `ICEBERG_COMMIT_ERROR`.

## 7.5. Ключевые потоки `IcebergMetadata`

### Создание таблицы / CTAS
- `createTable` — коммит пустой таблицы сразу.
- `beginCreateTable` → `catalog.newCreateTableTransaction(...)` (staged Iceberg `Transaction`; свойства из `IcebergUtil.createTableProperties`: `write.format.default`, `format-version`, `commit.num-retries`, компрессия, bloom-фильтры, object-store layout...); проверка **пустоты location** (иначе ошибка с подсказкой про `iceberg.unique-table-location`); возвращает `IcebergWritableTableHandle`.
- `finishCreateTable`: идемпотентность ретраев (staged snapshot не стейджится повторно); `stageInsert` + `commitCreateTable` → `transaction.commitTransaction()`; `AlreadyExistsException` → `TABLE_ALREADY_EXISTS`.

### INSERT
- `beginInsert`: валидации (`validateNotModifyingOldSnapshot`, `validateTableForTrino`, `validateNotEncryptedForWrite`, `validateNoUnknownTypeForWrite`), `setWriteTable`, `newWritableTableHandle` (формат из `write.format.default`, sort-поля — только identity).
- `finishInsert`: `newWriteTransaction()` → `stageInsert` (декодирует fragments `CommitTaskData` → `DataFiles.builder(...)`; `AppendFiles` при `merge_manifests_on_write=true`, иначе `FastAppend`) → `stageStatistics` (NDV, инкрементально) → `commitWriteTransaction`.

### UPDATE / DELETE / MERGE (row-level)
- Парадигма: `getRowChangeParadigm()` = `DELETE_ROW_AND_INSERT_ROW`; row-id колонка `$merge_row_id` = struct(`file_path, row_position, partition_spec_id, partition_data, source_row_id (v3), file_record_count, file_size, file_format`).
- `beginMerge` (format ≥ 2): возвращает `IcebergMergeTableHandle` (таблица + writable handle); распределение — `getUpdateLayout` → `IcebergPartitioningHandle.forUpdate()` (bucket по hash(file path) — строки одного файла попадают к одному writer'у).
- `finishMerge → finishWrite`: **`RowDelta`** с валидациями OCC:
  - `validateFromSnapshot(baseSnapshotId)`;
  - `validateNoConflictingDataFiles()` (при `write.delete.isolation-level=serializable`);
  - `validateDeletedFiles()`, `validateDataFilesExist(referencedDataFiles)`, `validateNoConflictingDeleteFiles()` (для UPDATE/MERGE; для DELETE не нужно — идемпотентно);
  - `conflictDetectionFilter(...)` — домены партиций из commit-задач (`CommitTaskDomainCollector`) ∩ запросные предикаты — сужение области конфликта;
  - добавляются data files + position deletes (v2) / deletion vectors (v3, puffin через `DeletionVectorWriter`, файлы, покрытые полностью, удаляются целиком через `rowDelta.removeRows`).
- Metadata-only DELETE: `applyDelete` (если предикат только по метаданным) → `executeDelete`: `newDelete().deleteFromRowFilter(toIcebergExpression(enforcedPredicate))`; счётчик строк — из snapshot summary.

### Materialized views
- Определение: `IcebergMaterializedViewDefinition` (JSON+Base64 в комментарии view); storage — Iceberg-таблица `{name}$materialized_view_storage` (скрыта по умолчанию).
- `beginRefreshMaterializedView`: решает incremental vs full (по `DEPENDS_ON_TABLES` из snapshot summary и `incremental_refresh_enabled`); incremental — `fromSnapshotExclusive(...)`.
- `finishRefreshMaterializedView`: full → `newDelete().deleteFromRowFilter(alwaysTrue())` + append; incremental → `AppendFiles.validateWith(SnapshotAncestryValidator)`; в summary пишутся зависимости (`DEPENDS_ON_TABLES`, `TRINO_QUERY_START_TIME`); после коммита — opportunistic expire snapshots.

### ANALYZE
- `getStatisticsCollectionMetadata`: только скалярные не-hidden колонки; статистика — `NUMBER_OF_DISTINCT_VALUES` через theta-sketch; `finishStatisticsCollection` → `TableStatisticsWriter` (puffin `.stats`-файл, blob `APACHE_DATASKETCHES_THETA_V1`, компрессия ZSTD) → `transaction.updateStatistics().setStatistics(file)`.

## 7.6. Pushdown-методы

- `applyFilter` (`UtcConstraintExtractor`): домены делятся на `newEnforced` (партиционная колонка во **всех** спеках + метаданные-колонки `$partition`/`$path`/`$file_modified_time`), `newUnenforced` (проверка по file stats на чтении), `unsupported` (остаётся движку). Подробнее — [10](10-iceberg-statistics-optimization.md).
- `applyProjection`: простые переменные + dereference-цепочки (path из field id); выключается `projection_pushdown_enabled`.
- `applyLimit`: только при `unenforcedPredicate.isAll()`.
- `getTableProperties`: `predicate` (enforced), `discretePredicates` (identity-партиции во всех спеках — для `MetadataQueryOptimizer`), `tablePartitioning` (активный bucket execution).
- `validateScan`: enforcement `query_partition_filter_required`.

## 7.7. Системные таблицы (диспетчеризация)

`getRawSystemTable` по `TableType`: `$history` → `HistoryTable`, `$metadata_log_entries` →
`MetadataLogEntriesTable`, `$snapshots` → `SnapshotsTable`, `$all_manifests` → `AllManifestsTable`,
`$manifests` → `ManifestsTable`, `$files` → `FilesTable`, `$entries`/`$all_entries` → `EntriesTable`,
`$properties` → `PropertiesTable`, `$refs` → `RefsTable`; `partitions` (view) → `PartitionsView`
(SQL-агрегат над `table$files`). Подробности — [11](11-iceberg-maintenance.md).
Табличная функция `table_changes` — changelog между снапшотами (`newIncrementalChangelogScan`).

## 7.8. FileIO и файловая система

- `ForwardingFileIoFactory` → `ForwardingFileIo` — Iceberg `FileIO` поверх `TrinoFileSystem` (bulk-delete батчами по 1000, `SupportsStorageCredentials` для vended credentials); при шифровании — `EncryptingFileIO.combine(...)`.
- `IcebergTableCredentialsProvider` — пуш пер-табличных credentials в воркеры (`getTableCredentials`).
- Подробности — [12-storage-filesystem.md](12-storage-filesystem.md).
