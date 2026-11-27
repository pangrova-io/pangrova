package org.pangrova.server.coordinator.bootstrap;

import javax.sql.DataSource;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamOffsetStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Binds stream DDL and transfers maintenance leadership to the supervised CDC runtime. */
final class StreamingBootstrap {
    private static final int MAX_STREAMS_PER_TENANT = 10;

    private final StreamDdlExecutor ddlExecutor;
    private final StreamStateStore stateStore;
    private final JdbcStreamOffsetStore offsetStore;
    private final CdcSourceRegistry sources;
    private final CdcRecordConverterRegistry converters;
    private JournaledCdcRuntime journaledRuntime;
    private boolean leaderActive;

    StreamingBootstrap(DataSource dataSource) {
        var loader = StreamingBootstrap.class.getClassLoader();
        stateStore = new JdbcStreamStateStore(dataSource);
        offsetStore = new JdbcStreamOffsetStore(dataSource);
        sources = CdcSourceRegistry.discover(loader);
        converters = CdcRecordConverterRegistry.discover(loader);
        ddlExecutor = new StreamDdlExecutor(new StreamDdlContext(
                stateStore, converters, MAX_STREAMS_PER_TENANT, sources));
    }

    synchronized void onLeaderElected() {
        leaderActive = true;
        if (journaledRuntime != null) journaledRuntime.setActive(true);
    }

    synchronized void onLeadershipLost() {
        leaderActive = false;
        if (journaledRuntime != null) journaledRuntime.setActive(false);
    }

    synchronized void attachJournaledRuntime(JournaledCdcRuntime runtime) {
        journaledRuntime = runtime;
        runtime.start();
        runtime.setActive(leaderActive);
    }

    StreamStateStore stateStore() { return stateStore; }
    JdbcStreamOffsetStore offsetStore() { return offsetStore; }
    CdcSourceRegistry sourceRegistry() { return sources; }
    CdcRecordConverterRegistry converterRegistry() { return converters; }

    /** Creates the connector session used by Worker-dispatched CDC writes for a stream. */
    static ConnectorSession sessionFor(StreamState state) {
        if (state.tenantId().isBlank()) {
            throw new IllegalArgumentException("CDC stream tenant identity must not be blank");
        }
        if (state.creatorIdentity() == null || state.creatorIdentity().isBlank()) {
            throw new IllegalArgumentException("CDC stream creator identity must not be blank");
        }
        QualifiedTableName ref = QualifiedTableName.resolve(List.of(state.targetTable().split("\\.")));
        return new StreamConnectorSession(ref.catalog(), ref.schema(), state.tenantId(), state.creatorIdentity());
    }

    StreamDdlExecutor getDdlExecutor() {
        return ddlExecutor;
    }

    /** Carries the persisted stream subject into connector write and commit authorization. */
    private record StreamConnectorSession(
            String catalog, String schema, String tenantId, String principal) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getUser() {
            return principal;
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
