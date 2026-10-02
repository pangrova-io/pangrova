package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.streaming.ConnectorPageSinkCdcWriter;
import org.pangrova.coordinator.streaming.PageSinkStreamWriterContext;
import org.pangrova.coordinator.streaming.KafkaCdcSourceReader;
import org.pangrova.coordinator.streaming.StreamConfig;
import org.pangrova.coordinator.streaming.StreamDdlContext;
import org.pangrova.coordinator.streaming.StreamDdlExecutor;
import org.pangrova.coordinator.streaming.StreamScheduler;
import org.pangrova.coordinator.streaming.StreamSchedulerContext;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.streaming.JdbcStreamOffsetStore;
import org.pangrova.metadata.streaming.JdbcStreamStateStore;
import org.pangrova.metadata.streaming.StreamOffsetStore;
import org.pangrova.metadata.streaming.StreamState;
import org.pangrova.metadata.streaming.StreamStateStore;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.streaming.CdcWriteSink;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.cdc.format.CanalJsonConverter;
import org.pangrova.streaming.cdc.format.DebeziumJsonConverter;
import org.pangrova.streaming.cdc.format.MaxwellJsonConverter;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Bootstraps the CDC ingestion stream subsystem: StreamDdlExecutor, StreamScheduler,
 * and their dependencies.
 *
 * <p>Lifecycle mirrors {@link IndexingBootstrap}:</p>
 * <ul>
 *   <li>{@link #getDdlExecutor()} — provides the executor for handler construction</li>
 *   <li>{@link #onLeaderElected()} — resumes all RUNNING streams</li>
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

    private StreamingBootstrap(StreamDdlExecutor ddlExecutor, StreamScheduler scheduler) {
        this.ddlExecutor = ddlExecutor;
        this.scheduler = scheduler;
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
     * @return initialized bootstrap
     */
    static StreamingBootstrap build(DataSource dataSource,
                                    WatermarkTracker watermarkTracker,
                                    MaintenanceScheduler maintenanceScheduler,
                                    CatalogRegistry catalogRegistry) {
        StreamConfig config = StreamConfig.withDefaults();
        StreamStateStore stateStore = new JdbcStreamStateStore(dataSource);
        StreamOffsetStore offsetStore = new JdbcStreamOffsetStore(dataSource);
        CdcRecordConverterRegistry converterRegistry = new CdcRecordConverterRegistry(List.of(
                new DebeziumJsonConverter(),
                new CanalJsonConverter(),
                new MaxwellJsonConverter()));

        StreamSchedulerContext schedulerDeps = new StreamSchedulerContext(
                stateStore, offsetStore, converterRegistry,
                watermarkTracker, maintenanceScheduler, config,
                state -> createSourceReader(state),
                state -> createWriteSink(state, catalogRegistry));
        StreamScheduler scheduler = new StreamScheduler(schedulerDeps);

        // Start the continuous query consumer with a log-only executor as default.
        // Production deployments replace this via DynamicTableDdlExecutor registration,
        // which provides a real incremental refresh executor bound to the planner.
        scheduler.startContinuousQueryConsumer(
                cqName -> LOG.debug("event=continuous_query_refresh_tick cqName={}", cqName),
                java.time.Duration.ofSeconds(30));

        StreamDdlContext ddlDeps = new StreamDdlContext(
                stateStore, offsetStore, scheduler, config);
        StreamDdlExecutor ddlExecutor = new StreamDdlExecutor(ddlDeps);

        LOG.info("event=streaming_bootstrap_built");
        return new StreamingBootstrap(ddlExecutor, scheduler);
    }

    /**
     * Resumes all RUNNING streams from the state store.
     * Called when this coordinator is elected as HA leader.
     */
    void onLeaderElected() {
        scheduler.resumeAll();
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
        scheduler.close();
        LOG.info("event=streaming_bootstrap_closed");
    }

    StreamScheduler getScheduler() {
        return scheduler;
    }

    StreamDdlExecutor getDdlExecutor() {
        return ddlExecutor;
    }

    // ── Factory methods ─────────────────────────────────────────────

    private static org.pangrova.spi.streaming.CdcSourceReader createSourceReader(StreamState state) {
        Map<String, String> sourceProps = state.sourceProperties();
        String bootstrapServers = sourceProps.get("bootstrap.servers");
        String topic = sourceProps.get("topic");
        String groupId = sourceProps.getOrDefault("group.id", "pangrova-cdc-" + state.streamName());
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
