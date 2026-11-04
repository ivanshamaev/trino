# 1. Архитектура Trino

Код: `core/trino-server-main`, `core/trino-server-core`, `core/trino-main`.

## 1.1. Модули репозитория

| Модуль | Назначение |
|---|---|
| `core/trino-server-main` | Точка входа: `io.trino.server.TrinoServer.main()` — проверка версии JDK и запуск `Server` |
| `core/trino-server-core` | Сборочный дистрибутив (provisio-дескриптор): launcher, `trino-server-main.jar`, плагины |
| `core/trino-main` | Ядро: `io.trino.server`, `io.trino.execution`, `io.trino.dispatcher`, `io.trino.node`, `io.trino.connector`, `io.trino.memory`, `io.trino.transaction`, ... |
| `core/trino-parser`, `core/trino-grammar` | ANTLR-грамматика (`SqlBase.g4`) и парсер SQL |
| `core/trino-spi` | Публичный SPI для коннекторов/плагинов (`io.trino.spi.*`: Page, Block, Type, Connector*) |
| `core/trino-web-ui` | Web UI |
| `client/*` | JDBC-драйвер, CLI |
| `lib/*` | Общие библиотеки: `trino-filesystem*`, `trino-orc`, `trino-parquet`, `trino-hdfs`, `trino-cache`, `trino-hive-formats`, `trino-matching` и др. |
| `plugin/*` | Коннекторы; наш фокус — `plugin/trino-iceberg` |
| `service/*`, `testing/*` | Trino proxy/verifier, тестовая инфраструктура |

## 1.2. Запуск сервера

`core/trino-main/src/main/java/io/trino/server/Server.java` (`doStart()`):

1. Проверка системных требований (`TrinoSystemRequirements`).
2. Сборка Guice-модулей: `ServerMainModule`, `NodeModule` (airlift `NodeInfo` → `node.id`, environment), `TransactionManagerModule`, `AccessControlModule`, `CatalogManagerModule`, `ExchangeManagerModule`, HTTP/JAX-RS/JMX-модули и т.д.
3. `Bootstrap("io.trino.bootstrap.engine", modules).loadSecretsPlugins().initialize()` — чтение `config.properties` (механизм airlift `@Config`).
4. Строго по порядку: загрузка плагинов (`PluginInstaller.loadPlugins()` → `PluginManager`), catalog store (`etc/catalog`, `ConnectorServicesProvider.loadInitialCatalogs()`), session-property/resource-group/access-control/exchange/spooling конфигураций; event listeners — только на координаторе; `Announcer.start()` — регистрация узла в кластере.

Ошибка конфигурации → `System.exit(100)`.

## 1.3. Координатор и воркер

Роль определяется одним флагом `ServerConfig.coordinator` (property `coordinator`, по умолчанию `true`).
В `ServerMainModule.setup()`: `if (serverConfig.isCoordinator()) install(new CoordinatorModule()); else install(new WorkerModule());`.

**CoordinatorModule** биндит всё, что относится к приёму и планированию запросов:
- JAX-RS ресурсы: `QueuedStatementResource` (`/v1/statement`), `ExecutingStatementResource` (`/v1/statement/executing`), `QueryResource`, `WorkerResource`, `StatusResource`, `NodeResource`...;
- `QueryManager`, `QueryPreparer`, `QueryIdGenerator`, `QueryMonitor`;
- диспетчер: `DispatchManager`, `LocalDispatchQueryFactory`, `SessionPropertyResolver`;
- resource groups: `InternalResourceGroupManager`;
- кластерная память: `ClusterMemoryManager` + политики `LowMemoryKiller`;
- планирование: `NodeScheduler`, `NodeTaskMap`, `NodePartitioningManager`, `ClusterSizeMonitor`, `ExecutionPolicy` (`all-at-once`, `phased` — по умолчанию `query.execution-policy=phased`);
- стоимость: `StatsCalculatorModule`, `CostCalculatorUsingExchanges`, `CostComparator`, `TaskCountEstimator`;
- `RemoteTaskFactory → HttpRemoteTaskFactory` — HTTP-клиент для постановки задач воркерам;
- планировщик/оптимизаторы: `AnalyzerFactory`, `PlanOptimizers`, `PlanFragmenter`, `QueryExplainerFactory`.

**WorkerModule** — только заглушки (`NoOpSessionSupplier`, `NoOpResourceGroupManager`, `NoOpFailureDetector`).
Всё общее (task execution, exchange clients, spill, память) биндится в `ServerMainModule` для обеих ролей:
`SqlTaskManager`, `TaskExecutor` (`ThreadPerDriverTaskExecutor` по умолчанию либо `TimeSharingTaskExecutor`),
`LocalMemoryManager`, `MemoryRevokingScheduler`, `LocalSpillManager`, `DirectExchangeClientFactory`,
`TaskResource` (`/v1/task`, `@ResourceSecurity(INTERNAL_ONLY)`).

## 1.4. Поток SQL-запроса (инфраструктурный уровень)

```
Клиент (JDBC/CLI)
  │  POST /v1/statement  (SQL в теле, контекст в X-Trino-* заголовках)
  ▼
QueuedStatementResource ── HttpRequestSessionContextFactory (заголовки → SessionContext)
  │  dispatchManager.createQuery()  (в отдельном пуле DispatchExecutor)
  ▼
DispatchManager.createQueryInternal:
  1. лимит длины текста (query.max-query-length = 1M)
  2. session = QuerySessionSupplier.createSession()  (identity, impersonation, properties)
  3. accessControl.checkCanExecuteQuery()
  4. queryPreparer.prepareQuery()  (парсинг, EXECUTE/EXECUTE IMMEDIATE)
  5. resourceGroupManager.selectGroup()  → постановка в очередь resource group
  ▼
LocalDispatchQuery.startWaitingForResources()
  │  ClusterSizeMonitor.waitForMinimumWorkers()  (query.required-workers=1, wait ≤ 5m)
  ▼
LocalDispatchQuery.startExecution() → QueryManager.createQuery → SqlQueryExecution.start()
  │  (состояния: QUEUED → WAITING_FOR_RESOURCES → DISPATCHING → PLANNING → STARTING
  │   → RUNNING → FINISHING → FINISHED | FAILED)
  ▼
scheduler.start()  (PipelinedQueryScheduler / EventDrivenFaultTolerantQueryScheduler)
  │  POST /v1/task/{taskId}  → воркеры создают задачи, координатор поллит статусы
  ▼
Результат: координатор тянет страницы output-стадии через DirectExchangeDataSource
  и отдаёт клиенту по GET /v1/statement/executing/{queryId}/{slug}/{token}
```

Ключевые классы:
- `.../dispatcher/QueuedStatementResource.java` — приём SQL, `QueryIdGenerator` (формат `YYYYMMDD_NNNNNNNN`), `Slug` (защита URL от перебора), long-poll с `MAX_WAIT_TIME = 1s`;
- `.../dispatcher/DispatchManager.java` — «creation must never fail»: любая ошибка → `FailedDispatchQueryFactory`;
- `.../dispatcher/LocalDispatchQuery.java` — связка `QueryStateMachine` + `QueryExecution`;
- `.../execution/QueryManager.java` — фоновые проверки (каждую секунду): `enforceMemoryLimits` (через `ClusterMemoryManager`), CPU/scan/write/output лимиты;
- `.../execution/QueryTracker.java` — heartbeat (`clientTimeout = 5m`), лимиты времени (`query-max-run-time` и др.), история (`query.max-history=100`, `min-expire-age=15m`).

### Session и транзакции

- `io.trino.Session` (immutable): queryId, `Optional<TransactionId>`, identity, catalog/schema/path/timezone, системные и каталоговые session-свойства, prepared statements. Сериализуемая проекция `SessionRepresentation` отправляется воркерам в `TaskUpdateRequest` и восстанавливается `SessionRepresentation.toSession(...)`.
- `io.trino.metadata.SessionPropertyManager` — реестр свойств (system + per-catalog, включая `IcebergSessionProperties`).
- Транзакции: `io.trino.transaction.InMemoryTransactionManager` (`transaction.idle-timeout = 5m`). `START TRANSACTION/COMMIT/ROLLBACK` — DDL-задачи (`StartTransactionTask`, `CommitTask`, `RollbackTask`); вне явной транзакции каждый запрос идёт в autocommit-транзакции. ID новой транзакции возвращается клиенту заголовком `X-Trino-Started-Transaction-Id`.
- Для Iceberg важно: каждая транзакция движка создаёт **отдельный экземпляр `IcebergMetadata` с собственным `TrinoCatalog`** (см. [07-iceberg-metadata-transactions.md](07-iceberg-metadata-transactions.md)).

## 1.5. Discovery и членство в кластере

Пакет `io.trino.node` (`.../node/`). Тип инвентаря — `NodeInventoryConfig.discovery.type`, enum `NodeInventoryType { ANNOUNCE, DNS, AIRLIFT_DISCOVERY }`, по умолчанию **ANNOUNCE**:

- **ANNOUNCE**: `AnnounceNodeAnnouncer` каждые 5 секунд делает `POST /v1/announce` на все `discovery.uri` (координаторы, кроме себя); координатор складывает URI в `AnnounceNodeInventory` с TTL 30 с.
- **AIRLIFT_DISCOVERY**: встраиваемый discovery-сервер airlift (`EmbeddedDiscoveryModule`, property `discovery-server.enabled`); объявления через `bindHttpAnnouncement("trino")`.

Видение воркеров координатором — `CoordinatorNodeManager`: каждые 5 с `refreshNodes`, для каждого узла ленивый `RemoteNodeState` (`GET {node}/v1/info`); несовпадение `environment`/`nodeVersion` → узел `INVALID`; нет ответа 30 с → узел пропадает из `activeNodes`. Результат — `AllNodes(active, inactive, draining, drained, shuttingDown)` + слушатели изменений.

**Failure detection**: `HeartbeatFailureDetector` (только координатор) — `HEAD /v1/status` с периодом `failureDetectorConfig.heartbeatInterval`; узел считается failed при доле неудач ≥ порога; `/v1/node/failed` — для UI.

**Graceful shutdown / drain**: `NodeStateManager` — машина состояний `ACTIVE → SHUTTING_DOWN | DRAINING → DRAINED → SHUTTING_DOWN`; координатор не может быть выключен через drain; `requestGracefulShutdown()` ждёт `shutdown.grace-period = 2m`, затем завершения активных задач и `System.exit(0)`.

## 1.6. Плагины и каталоги

**PluginManager** (`.../server/PluginManager.java`):
- плагины ищутся в `plugin.dir` (по умолчанию `plugin/`), каждый подкаталог — отдельный `PluginClassLoader` со списком шаримых с движком пакетов (`io.trino.spi.`, `io.airlift.slice.`, ...);
- `ServiceLoader.load(Plugin.class)`; `installPluginInternal` регистрирует всё, что умеет `Plugin`: connector factories, типы, функции, event listeners, exchange/spooling manager factories и т.д.

**Каталоги** (`io.trino.connector`):
- `FileCatalogStore` читает `.properties` из `etc/catalog` (`catalog.config-dir`); имя файла = имя каталога; `system` зарезервирован;
- режим `catalog-manager.kind`: `STATIC` (все каталоги грузятся на старте) или `DYNAMIC` (`CREATE CATALOG`, версионированные `CatalogHandle`, ленивая материализация на воркерах через `WorkerDynamicCatalogManager.ensureCatalogsLoaded` из `SqlTaskManager.updateTask` + периодическая чистка `CatalogPruneTask`);
- `CatalogFactory → DefaultCatalogFactory` создаёт `CatalogConnector` из `ConnectorFactory` плагина (для Iceberg — `IcebergConnectorFactory`, см. [06-iceberg-connector-overview.md](06-iceberg-connector-overview.md)).

Типовая конфигурация Iceberg-каталога `etc/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://...
fs.s3.enabled=true
s3.endpoint=...
s3.aws-access-key=...
s3.region=...
```

(префикс `iceberg.` обязателен: коннектор экспортирует свойства FS с этим префиксом; `connector.name` указывает на `IcebergConnectorFactory.getName() = "iceberg"`).

## 1.7. Безопасность

- **Системный уровень**: `io.trino.security.AccessControlManager` — цепочка `SystemAccessControl` из `etc/access-control.properties` (`access-control.name`); проверки `checkCanExecuteQuery`, `checkCanSetUser`, impersonation, row filters/column masks.
- **Уровень коннектора**: каждый коннектор может дать свой `ConnectorAccessControl` (у Iceberg — `IcebergSecurityModule`: ALLOW_ALL / READ_ONLY / FILE / SYSTEM).
- Аутентификация HTTP — `io.trino.server.security.*` (password/certificate/header/OAuth2).

## 1.8. Наблюдаемость

- `io.trino.event.QueryMonitor` — события `QueryCreatedEvent`/`QueryCompletedEvent` всем event listeners (грузятся только на координаторе).
- JMX: `QueryManager`, `SqlTaskManager`, `ClusterMemoryManager`, `MultilevelSplitQueue` и т.д.
- Tracing (OpenTelemetry): span'ы `query`, `dispatch`, `query-start`, `planner`, `analyze`...
