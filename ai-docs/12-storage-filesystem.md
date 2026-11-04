# 12. Хранилище: TrinoFileSystem, S3/HDFS/облака, кэши, object store layout

Код: `lib/trino-filesystem` (SPI), `lib/trino-filesystem-manager` (сборка фабрики),
`lib/trino-filesystem-s3|gcs|azure`, `lib/trino-hdfs`, `lib/trino-cache`,
плагины `trino-blob-cache-*`; стыковка с Iceberg — `fileio/ForwardingFileIo*`,
`DefaultIcebergFileSystemFactory`, `IcebergRestCatalogFileSystemFactory` (REST vended credentials).

## 12.1. Архитектура файловой подсистемы

```mermaid
flowchart TB
    subgraph ICE["Iceberg-коннектор"]
        DIFSF["DefaultIcebergFileSystemFactory<br/>create(identity, tableCredentials)"]
        FIO["ForwardingFileIo<br/>(Iceberg FileIO поверх TrinoFileSystem)<br/>bulk-delete батчами 1000"]
    end
    subgraph MGR["lib/trino-filesystem-manager<br/>FileSystemModule (per catalog)"]
        SW["SwitchingFileSystemFactory<br/>(роутинг по scheme)"] --> TR["TracingFileSystemFactory"]
        TR --> TK["TrackingFileSystemFactory<br/>(leak detection, fs.tracking.enabled)"]
        TK --> CF["CacheFileSystemFactory<br/>(если fs.cache.enabled)"]
    end
    subgraph IMPL["Реализации (lib/trino-filesystem-*)"]
        S3F["S3FileSystemFactory<br/>(s3/s3a/s3n)"]
        AZF["Azure (abfs/abfss/wasb...)"]
        GCSF["GCS (gs)"]
        LOC["local/file"]
        HDFS["HdfsFileSystemFactory<br/>(lib/trino-hdfs, hadoop fs, kerberos)"]
    end
    ICE --> MGR --> IMPL
    CF -.->|BlobCache<br/>(trino-blob-cache-memory / alluxio)| CACHE["Кэш данных"]
```

- `TrinoFileSystem` (SPI): `newInputFile(location[, length[, lastModified]])`, `newEncryptedInputFile`, `newOutputFile`, `listFiles`, `deleteFile(s)`, `renameFile`, `directoryExists`...; `TrinoInputFile/TrinoInput/TrinoOutputFile`, `Location`.
- Включение схем в каталоге: `fs.s3.enabled`, `fs.azure.enabled`, `fs.gcs.enabled`, `fs.local.enabled`, `fs.hadoop.enabled` (в Iceberg-каталоге — с префиксом `iceberg.`: `iceberg.fs.s3.enabled=true` и т.д.).
- **HDFS**: `lib/trino-hdfs` — адаптер Hadoop `FileSystem` + Kerberos (`HdfsKerberosConfig`, `CachingKerberosHadoopAuthentication`), эмуляция hadoop-путей (`HadoopPaths`).
- **Шифрование**: `IcebergEncryptionModule`/`EncryptingFileIO.combine(fileIo, encryptionManager)` — table encryption keys (Parquet modular encryption, `ParquetFileDecryptionData` в сплитах).

## 12.2. ForwardingFileIo

Iceberg-библиотеке нужен `org.apache.iceberg.io.FileIO`; Trino не использует `HadoopFileIO`/`AwsFileIO`,
а адаптирует свою ФС (`fileio/ForwardingFileIo.java`):
- `newInputFile/newOutputFile` → обёртки над `TrinoInputFile/TrinoOutputFile`;
- `deleteFile` — bulk батчами по 1000 (`SupportsBulkOperations`, пул `@ForIcebergFileDelete`);
- `SupportsStorageCredentials.setCredentials` — прокидывание временных кред в `IcebergRestCatalogFileSystem` (vended credentials от REST-каталога);
- `use_file_size_from_metadata` — брать ли размер файла из манифеста (меньше запросов HEAD к S3).

## 12.3. S3 (lib/trino-filesystem-s3)

Ключевые свойства (в Iceberg-каталоге с префиксом `iceberg.s3.*`; **старые `hive.s3.*` к Iceberg не применяются** — это Hive-коннектор):

| Свойство | Default | Смысл |
|---|---|---|
| `s3.endpoint`, `s3.region` | — | Endpoint (MinIO/собственный S3) и регион |
| `s3.aws-access-key` / `s3.aws-secret-key` | — | Статические креды |
| `s3.iam-role`, `s3.role-session-name`, `s3.external-id` | — | Assume-role |
| `s3.path-style-access` | false | Path-style (нужно для MinIO) |
| `s3.streaming.part-size` | 32MB | Multipart upload |
| `s3.max-connections` | 500 | HTTP-пул |
| `s3.sse.type` (+`kms-key-id`, `customer-key`) | NONE | SSE-S3/SSE-KMS/SSE-C |
| `s3.storage-class`, `s3.checksum-algorithm`, `s3.requester-pays` | — | Прочее |
| `s3.cross-region-access`, `s3.retry-mode`, `s3.max-error-retries` | | |

Загрузка — потоковая через `S3OutputStream` (multipart), никаких staging-директорий.
Для мультитенантных кредов — `S3SecurityMapping*` (config-files). Для vended credentials
(REST-каталог) креды приходят от сервера на `loadTable` и действуют ограниченное время.

## 12.4. Кэширование

- **Blob-кэш данных**: `CacheFileSystem`/`CacheInputFile` (`lib/trino-filesystem/.../cache`) —
  чтение через `BlobCache` (SPI `io.trino.spi.cache.BlobCache`); реализации — плагины
  `trino-blob-cache-memory` (heap) и `trino-blob-cache-alluxio`. Включается `fs.cache.enabled`
  на каталог. Ключ — `CacheKeyProvider` (у Iceberg `IcebergCacheKeyProvider`: компоненты location,
  т.к. **файлы Iceberg иммутабельны** — location стабильный идентификатор).
- **Split affinity**: `CacheSplitAffinityProvider` — сплиты с одним ключом (`path:offset:length`)
  направляются на воркер, где данные уже в кэше (consistent-hash ring движка).
- **Кэш метаданных** (координатор): `iceberg.metadata-cache-enabled=true` — blob-кэш
  «metadata»-класса (`METADATA_CACHE_REQUIREMENTS`, LOW_LATENCY) для metadata.json/манифестов.
- **Parquet footer cache**: `iceberg.parquet-footer-cache.type=MEMORY`, `...memory-max-size` (10MB default).

## 12.5. Object store layout

`util/ObjectStoreLocationProvider.java` — оптимизация путей для объектных хранилищ
(prefix-per-file вместо «плоских» листингов):
- включается табличным `object_store_layout_enabled` (→ `write.object-storage.enabled`) или
  конфигом `iceberg.object-store-layout.enabled`;
- путь: `<storageLocation>/<murmur3_32-base64url(hash filename)>/[<partition>/]<filename>`
  (контекст БД/таблицы — только если storage вне префикса таблицы);
- `write.object-storage.partitioned-paths` (default true) — включать ли партиционные сегменты.

`DefaultLocationProvider` (по умолчанию): `write.data.location` (или `<table>/data`) + partition path + filename.

## 12.6. Практический чек-лист конфигурации Iceberg over S3

```properties
# etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore          # или rest/jdbc/glue/nessie/snowflake
hive.metastore.uri=thrift://hms:9083          # для HMS-каталога

# нативная ФС (обязательно явно включить схему)
iceberg.fs.s3.enabled=true
iceberg.s3.endpoint=https://s3.example.com
iceberg.s3.region=us-east-1
iceberg.s3.path-style-access=true             # MinIO
iceberg.s3.aws-access-key=...
iceberg.s3.aws-secret-key=...

# производительность
iceberg.parquet-footer-cache.type=MEMORY
# fs.cache.enabled=true + плагин blob-кэша (alluxio/memory) — опционально
```

Типичные параметры, влияющие на производительность чтения/записи (session-уровень):
`max_split_size`, `dynamic_filtering_wait_timeout`, `use_file_size_from_metadata`,
`max_partitions_per_writer`, `idle_writer_min_file_size`, `merge_manifests_on_write`,
`sorted_writing_enabled`, plus Parquet/ORC reader/writer свойства
(`parquet_max_read_block_size`, `orc_tiny_stripe_threshold`, ...).
