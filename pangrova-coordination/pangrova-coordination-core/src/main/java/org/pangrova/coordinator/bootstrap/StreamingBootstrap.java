package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.streaming.coordinator.ConnectorPageSinkCdcWriter;
import org.pangrova.streaming.coordinator.PageSinkStreamWriterContext;
import org.pangrova.streaming.coordinator.KafkaCdcSourceReader;
import org.pangrova.streaming.coordinator.StreamConfig;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.streaming.coordinator.StreamScheduler;
import org.pangrova.streaming.coordinator.StreamSchedulerContext;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.streaming.store.jdbc.offset.JdbcStreamOffsetStore;
import org.pangrova.streaming.store.jdbc.checkpoint.JdbcStreamStateStore;
import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.commit.JdbcCdcCommitStateStore;
import org.pangrova.streaming.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.cdcformat.canal.CanalJsonConverter;
import org.pangrova.streaming.cdcformat.debezium.DebeziumJsonConverter;
import org.pangrova.streaming.cdcformat.maxwell.MaxwellJsonConverter;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bootstraps the CDC ingestion stream subsystem: StreamDdlExecutor, StreamScheduler,
 * and their dependencies.
 *
 * <p>Lifecycle mirrors {@link IndexingBootstrap}:</p>
 * <ul>
 *   <li>{@link #getDdlExecutor()} — provides the executor for handler construction</li>
 *   <li>{@link #onLeaderElected()} — activates reconciliation of persisted streams</li>
 *   <li>{@link #onLeadershipLost()} — stops all active pipelines</li>
 *   <li>{@link #close()} — shuts down the scheduler and thread pool</li>
 * </ul>
 *
 * <p>Shutdown: {@link #close()} delegates to {@link StreamScheduler#close()}
 * which stops all pipelines and shuts down the thread pool.</p>
 */
final class StreamingBootstrap implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingBootstrap.class);

    private final StreamDdlExecutor ddlExecutor;
    private final StreamScheduler scheduler;
    private final ScheduledExecutorService reconciliationExecutor;

    StreamingBootstrap(StreamDdlExecutor ddlExecutor, StreamScheduler scheduler) {
        this.ddlExecutor = ddlExecutor;
        this.scheduler = scheduler;
        scheduler.stopAll();
        reconciliationExecutor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("stream-reconciliation");
        reconciliationExecutor.scheduleWithFixedDelay(this::reconcileSafely, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Builds the streaming subsystem and returns a ready-to-use bootstrap instance.
     *
     * <p>BUG-D27 fix: accepts {@link CatalogRegistry} so the write sink factory
     * creates real {@link ConnectorPageSinkCdcWriter} instances instead of the
     * no-op placeholder that silently discarded all CDC data.</p>
     *
     * @param dataSource           JDBC data source for stream state and offset persistence
     * @param watermarkTracker     shared watermark tracker from the streaming module
     * @param maintenanceScheduler shared maintenance scheduler from the streaming module
     * @param catalogRegistry      catalog registry for connector resolution (required for CDC writes)
     * @param coordinatorId        durable owner identity of this coordinator process
     * @return initialized bootstrap
     */
    static StreamingBootstrap build(DataSource dataSource,
                                    WatermarkTracker watermarkTracker,
                                    MaintenanceScheduler maintenanceScheduler,
                                    CatalogRegistry catalogRegistry, String coordinatorId) {
        StreamConfig config = StreamConfig.withDefaults();
        StreamStateStore stateStore = new JdbcStreamStateStore(dataSource);
        StreamOffsetStore offsetStore = new JdbcStreamOffsetStore(dataSource);
        CdcRecordConverterRegistry converterRegistry = new CdcRecordConverterRegistry(List.of(
                new DebeziumJsonConverter(),
                new CanalJsonConverter(),
                new MaxwellJsonConverter()));

        StreamSchedulerContext schedulerDeps = new StreamSchedulerContext(
                stateStore, offsetStore,
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource)), converterRegistry,
                watermarkTracker, maintenanceScheduler, config,
                state -> createSourceReader(state),
                state -> createWriteSink(state, catalogRegistry));
        StreamScheduler scheduler = new StreamScheduler(schedulerDeps, coordinatorId);

        StreamDdlContext ddlDeps = new StreamDdlContext(
                stateStore, offsetStore, scheduler, config);
        StreamDdlExecutor ddlExecutor = new StreamDdlExecutor(ddlDeps);

        LOG.info("event=streaming_bootstrap_built");
        return new StreamingBootstrap(ddlExecutor, scheduler);
    }

    /**
     * Activates reconciliation without blocking maintenance-lease renewal on source initialization.
     * The periodic pass also discovers DDL accepted by another coordinator.
     */
    void onLeaderElected() {
        scheduler.resume();
        reconciliationExecutor.execute(this::reconcileSafely);
        LOG.info("event=streaming_leader_elected");
    }

    /**
     * Stops all active pipelines.
     * Called when this coordinator loses HA leadership.
     */
    void onLeadershipLost() {
        scheduler.stopAll();
        LOG.info("event=streaming_leadership_lost");
    }

    @Override
    public void close() {
        reconciliationExecutor.shutdownNow();
        scheduler.close();
        LOG.info("event=streaming_bootstrap_closed");
    }

    private void reconcileSafely() {
        try {
            scheduler.reconcile();
        } catch (RuntimeException failure) {
            LOG.error("event=stream_reconciliation_failed", failure);
        }
    }

    StreamScheduler getScheduler() {
        return scheduler;
    }

    StreamDdlExecutor getDdlExecutor() {
        return ddlExecutor;
    }

    // ── Factory methods ─────────────────────────────────────────────

    static org.pangrova.streaming.api.cdc.CdcSourceReader createSourceReader(StreamState state) {
        Map<String, String> sourceProps = state.sourceProperties();
        String bootstrapServers = sourceProps.get("bootstrap.servers");
        String topic = sourceProps.get("topic");
        String groupId = sourceProps.getOrDefault("group.id", "pangrova-cdc-" + state.checkpointId());
        return new KafkaCdcSourceReader(bootstrapServers, topic, groupId, sourceProps);
    }

    /**
     * Creates a real connector-backed write sink via {@link ConnectorPageSinkCdcWriter}.
     *
     * <p>BUG-D27 fix: resolves the target table through the catalog registry to obtain
     * the connector metadata, page sink provider, session, and output table handle needed
     * for real CDC writes. The previous no-op placeholder silently discarded all data.</p>
     *
     * <p>Resolution path: targetTable (catalog.schema.table) → CatalogRegistry.getCatalog
     * → ConnectorMetadata.getTableHandle → ConnectorPageSinkCdcWriter. The writer obtains a
     * fresh operation-specific handle only when a batch is staged.</p>
     */
    static CdcWriteSink createWriteSink(StreamState state, CatalogRegistry catalogRegistry) {
        String targetTable = state.targetTable();
        QualifiedTableName ref = QualifiedTableName.resolve(List.of(targetTable.split("\\.")));
        if (state.tenantId().isBlank()) {
            throw new IllegalArgumentException("CDC stream tenant identity must not be blank");
        }
        ConnectorSession session = new StreamConnectorSession(
                ref.catalog(), ref.schema(), state.tenantId());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = catalogHandle.getMetadata(session);
        TableResolver.requireTableHandle(metadata, session, ref);

        PageSinkStreamWriterContext writerDeps = new PageSinkStreamWriterContext(
                metadata,
                catalogHandle.getPageSinkProvider(),
                session,
                targetTable);
        // CDC currently executes in the coordinator JVM. No Worker dispatch is wired;
        // retain an explicit operational warning until the canonical task path exists.
        LOG.warn("event=cdc_write_sink_local_processing target={} catalog={} "
                + "detail=worker_dispatch_unavailable", targetTable, ref.catalog());
        return new ConnectorPageSinkCdcWriter(writerDeps);
    }

    /** Carries the persisted stream tenant into connector write and commit authorization. */
    private record StreamConnectorSession(
            String catalog, String schema, String tenantId) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getUser() {
            return "system";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of(catalog);
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of(schema);
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of("tenant_id", tenantId);
        }
    }
}
