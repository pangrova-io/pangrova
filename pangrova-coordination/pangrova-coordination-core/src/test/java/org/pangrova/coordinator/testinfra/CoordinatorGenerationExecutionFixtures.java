package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.generation.GenerationExecutionOwnership;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.memory.coordination.InMemoryDistributedLease;
import org.pangrova.generation.api.job.GenerationJobKey;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore;
import org.pangrova.spi.query.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/** Real HA processes sharing PostgreSQL execution ownership for artifact tests. */
public final class CoordinatorGenerationExecutionFixtures implements AutoCloseable {
    private final DataSource dataSource;
    private final JdbcExecutionAnchorStore anchors;
    private final Map<String, HighAvailabilityCoordinationService> processes = new HashMap<>();
    private final Map<String, GenerationExecutionOwner> reportedOwners = new HashMap<>();

    public CoordinatorGenerationExecutionFixtures(DataSource dataSource) {
        this.dataSource = dataSource;
        anchors = new JdbcExecutionAnchorStore(dataSource);
    }

    public synchronized GenerationExecutionOwnership ownership(String owner) {
        var service = processes.computeIfAbsent(owner, id -> {
            var started = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(),
                    HighAvailabilityCoordinationService.CoordinationConfig.defaults());
            started.setExecutionAnchorStore(new JdbcExecutionAnchorStore(dataSource));
            started.start(id);
            return started;
        });
        return new GenerationExecutionOwnership(service, owner);
    }

    /** Performs an explicit real backend yield/takeover and retains the original report evidence. */
    public synchronized ExecutionAnchor takeOwnership(String tenant, String job, String owner, long controlEpoch) {
        var key = new GenerationJobKey(tenant, job);
        var ownership = ownership(owner);
        anchors.current(key.executionId()).filter(current -> !current.ownerId().equals(owner)).ifPresent(anchors::yield);
        var anchor = ownership.acquire(key).orElseThrow();
        reportedOwners.put(key.executionId() + ":" + controlEpoch, GenerationExecutionOwner.from(anchor));
        return anchor;
    }

    public synchronized GenerationExecutionOwner reportedOwner(String tenant, String job, long controlEpoch) {
        var owner = reportedOwners.get(new GenerationJobKey(tenant, job).executionId() + ":" + controlEpoch);
        if (owner == null) {
            throw new IllegalStateException("fixture has no originally issued Native owner evidence");
        }
        return owner;
    }

    @Override
    public synchronized void close() {
        processes.values().forEach(HighAvailabilityCoordinationService::close);
    }
}
