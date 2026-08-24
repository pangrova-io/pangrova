package org.pangrova.metadata.coordination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.spi.coordination.HeartbeatSnapshot;
import org.pangrova.spi.coordination.WorkerRegistry;
import org.pangrova.spi.coordination.WorkerSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed worker registry using a {@code worker_registry} table.
 *
 * <p>Persists worker topology to the metadata database so that a new
 * coordinator leader can recover the full worker set immediately after
 * failover, without waiting for heartbeat cycles to rebuild it.</p>
 *
 * <p>Follows the same patterns as {@link JdbcDistributedLease}:
 * static SQL constants, DataSource constructor injection, SLF4J
 * structured logging, try-with-resources for all JDBC resources,
 * and SQLException wrapping into RuntimeException.</p>
 */
public final class JdbcWorkerRegistry implements WorkerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWorkerRegistry.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TypeReference<List<String>> LIST_TYPE = new TypeReference<>() {};

    private static final String SQL_REGISTER =
            "INSERT INTO worker_registry "
                    + "(worker_id, host, http_port, transport_port, protocol_version, "
                    + "capabilities, rack_id, data_locality, memory_pressure, cpu_utilization, "
                    + "running_task_count, available_memory_bytes, last_heartbeat_ms, registered_at_ms) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (worker_id) DO UPDATE SET "
                    + "host=EXCLUDED.host, http_port=EXCLUDED.http_port, "
                    + "transport_port=EXCLUDED.transport_port, protocol_version=EXCLUDED.protocol_version, "
                    + "capabilities=EXCLUDED.capabilities, rack_id=EXCLUDED.rack_id, "
                    + "data_locality=EXCLUDED.data_locality, memory_pressure=EXCLUDED.memory_pressure, "
                    + "cpu_utilization=EXCLUDED.cpu_utilization, running_task_count=EXCLUDED.running_task_count, "
                    + "available_memory_bytes=EXCLUDED.available_memory_bytes, "
                    + "last_heartbeat_ms=EXCLUDED.last_heartbeat_ms";

    private static final String SQL_HEARTBEAT =
            "UPDATE worker_registry SET memory_pressure=?, cpu_utilization=?, "
                    + "running_task_count=?, available_memory_bytes=?, last_heartbeat_ms=? "
                    + "WHERE worker_id=?";

    private static final String SQL_UNREGISTER =
            "DELETE FROM worker_registry WHERE worker_id=?";

    private static final String SQL_GET_ACTIVE =
            "SELECT * FROM worker_registry WHERE last_heartbeat_ms > ?";

    private static final String SQL_GET_WORKER =
            "SELECT * FROM worker_registry WHERE worker_id=?";

    private static final String SQL_EXPIRE_STALE =
            "DELETE FROM worker_registry WHERE last_heartbeat_ms < ?";

    // PreparedStatement parameter indices for SQL_REGISTER.
    private static final int PARAM_WORKER_ID = 1;
    private static final int PARAM_HOST = 2;
    private static final int PARAM_HTTP_PORT = 3;
    private static final int PARAM_TRANSPORT_PORT = 4;
    private static final int PARAM_PROTOCOL_VERSION = 5;
    private static final int PARAM_CAPABILITIES = 6;
    private static final int PARAM_RACK_ID = 7;
    private static final int PARAM_DATA_LOCALITY = 8;
    private static final int PARAM_MEMORY_PRESSURE = 9;
    private static final int PARAM_CPU_UTILIZATION = 10;
    private static final int PARAM_RUNNING_TASK_COUNT = 11;
    private static final int PARAM_AVAILABLE_MEMORY_BYTES = 12;
    private static final int PARAM_LAST_HEARTBEAT_MS = 13;
    private static final int PARAM_REGISTERED_AT_MS = 14;

    private final DataSource dataSource;

    public JdbcWorkerRegistry(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void register(WorkerSnapshot worker) {
        Objects.requireNonNull(worker, "worker must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_REGISTER)) {
            bindWorkerSnapshot(ps, worker);
            ps.executeUpdate();
            LOG.info("event=worker_registered workerId={} host={} httpPort={}",
                    worker.workerId(), worker.host(), worker.httpPort());
        } catch (SQLException e) {
            LOG.error("event=worker_register_error workerId={} error={}",
                    worker.workerId(), e.getMessage(), e);
            throw new RuntimeException("Failed to register worker: " + worker.workerId(), e);
        }
    }

    private static void bindWorkerSnapshot(PreparedStatement ps, WorkerSnapshot w) throws SQLException {
        ps.setString(PARAM_WORKER_ID, w.workerId());
        ps.setString(PARAM_HOST, w.host());
        ps.setInt(PARAM_HTTP_PORT, w.httpPort());
        ps.setInt(PARAM_TRANSPORT_PORT, w.transportPort());
        ps.setInt(PARAM_PROTOCOL_VERSION, w.protocolVersion());
        ps.setString(PARAM_CAPABILITIES, serializeList(w.capabilities()));
        ps.setString(PARAM_RACK_ID, w.rackId());
        ps.setString(PARAM_DATA_LOCALITY, serializeList(w.dataLocality()));
        ps.setString(PARAM_MEMORY_PRESSURE, w.memoryPressure());
        ps.setDouble(PARAM_CPU_UTILIZATION, w.cpuUtilization());
        ps.setInt(PARAM_RUNNING_TASK_COUNT, w.runningTaskCount());
        ps.setLong(PARAM_AVAILABLE_MEMORY_BYTES, w.availableMemoryBytes());
        ps.setLong(PARAM_LAST_HEARTBEAT_MS, w.lastHeartbeatMs());
        ps.setLong(PARAM_REGISTERED_AT_MS, w.registeredAtMs());
    }

    @Override
    public void heartbeat(String workerId, HeartbeatSnapshot snapshot) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_HEARTBEAT)) {
            ps.setString(1, snapshot.memoryPressure());
            ps.setDouble(2, snapshot.cpuUtilization());
            ps.setInt(3, snapshot.runningTaskCount());
            ps.setLong(4, snapshot.availableMemoryBytes());
            ps.setLong(5, System.currentTimeMillis());
            ps.setString(6, workerId);
            ps.executeUpdate();
            LOG.debug("event=worker_heartbeat workerId={}", workerId);
        } catch (SQLException e) {
            LOG.error("event=worker_heartbeat_error workerId={} error={}",
                    workerId, e.getMessage(), e);
            throw new RuntimeException("Failed to update heartbeat for worker: " + workerId, e);
        }
    }

    @Override
    public void unregister(String workerId) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_UNREGISTER)) {
            ps.setString(1, workerId);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=worker_unregistered workerId={}", workerId);
            }
        } catch (SQLException e) {
            LOG.error("event=worker_unregister_error workerId={} error={}",
                    workerId, e.getMessage(), e);
            throw new RuntimeException("Failed to unregister worker: " + workerId, e);
        }
    }

    @Override
    public List<WorkerSnapshot> getActiveWorkers(Duration staleThreshold) {
        Objects.requireNonNull(staleThreshold, "staleThreshold must not be null");
        long cutoff = System.currentTimeMillis() - staleThreshold.toMillis();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_ACTIVE)) {
            ps.setLong(1, cutoff);
            try (ResultSet rs = ps.executeQuery()) {
                List<WorkerSnapshot> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
                return Collections.unmodifiableList(result);
            }
        } catch (SQLException e) {
            LOG.error("event=worker_get_active_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to get active workers", e);
        }
    }

    @Override
    public Optional<WorkerSnapshot> getWorker(String workerId) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_WORKER)) {
            ps.setString(1, workerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error("event=worker_get_error workerId={} error={}",
                    workerId, e.getMessage(), e);
            throw new RuntimeException("Failed to get worker: " + workerId, e);
        }
    }

    @Override
    public int expireStale(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        long cutoff = System.currentTimeMillis() - threshold.toMillis();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_EXPIRE_STALE)) {
            ps.setLong(1, cutoff);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=worker_stale_expired count={}", deleted);
            }
            return deleted;
        } catch (SQLException e) {
            LOG.error("event=worker_expire_stale_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to expire stale workers", e);
        }
    }

    private WorkerSnapshot mapRow(ResultSet rs) throws SQLException {
        return new WorkerSnapshot(
                rs.getString("worker_id"),
                rs.getString("host"),
                rs.getInt("http_port"),
                rs.getInt("transport_port"),
                rs.getInt("protocol_version"),
                deserializeList(rs.getString("capabilities")),
                rs.getString("rack_id"),
                deserializeList(rs.getString("data_locality")),
                rs.getString("memory_pressure"),
                rs.getDouble("cpu_utilization"),
                rs.getInt("running_task_count"),
                rs.getLong("available_memory_bytes"),
                rs.getLong("last_heartbeat_ms"),
                rs.getLong("registered_at_ms")
        );
    }

    private static String serializeList(List<String> list) {
        if (list == null || list.isEmpty()) {
            return "[]";
        }
        try {
            return MAPPER.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            LOG.error("event=json_serialize_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize list to JSON", e);
        }
    }

    private static List<String> deserializeList(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return MAPPER.readValue(json, LIST_TYPE);
        } catch (JsonProcessingException e) {
            LOG.error("event=json_deserialize_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to deserialize JSON to list", e);
        }
    }
}
