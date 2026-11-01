package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.artifact.ArtifactExecutionOwnership;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.indexing.control.ArtifactBuildJobKey;
import org.pangrova.indexing.control.ArtifactExecutionOwner;
import org.pangrova.query.transaction.JdbcExecutionAnchorStore;
import org.pangrova.spi.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/** Real HA processes sharing PostgreSQL execution ownership for artifact tests. */
public final class CoordinatorArtifactExecutionFixtures implements AutoCloseable {
    private final DataSource dataSource;
    private final JdbcExecutionAnchorStore anchors;
    private final Map<String, HighAvailabilityCoordinationService> processes = new HashMap<>();
    private final Map<String, ArtifactExecutionOwner> reportedOwners = new HashMap<>();

    public CoordinatorArtifactExecutionFixtures(DataSource dataSource) {
        this.dataSource = dataSource;
        anchors = new JdbcExecutionAnchorStore(dataSource);
    }

    public synchronized ArtifactExecutionOwnership ownership(String owner) {
        var service = processes.computeIfAbsent(owner, id -> {
            var started = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(),
                    HighAvailabilityCoordinationService.CoordinationConfig.defaults());
            started.setExecutionAnchorStore(new JdbcExecutionAnchorStore(dataSource));
            started.start(id);
            return started;
        });
        return new ArtifactExecutionOwnership(service, owner);
    }

    /** Performs an explicit real backend yield/takeover and retains the original report evidence. */
    public synchronized ExecutionAnchor takeOwnership(String tenant, String job, String owner, long controlEpoch) {
        var key = new ArtifactBuildJobKey(tenant, job);
        var ownership = ownership(owner);
        anchors.current(key.executionId()).filter(current -> !current.ownerId().equals(owner)).ifPresent(anchors::yield);
        var anchor = ownership.acquire(key).orElseThrow();
        reportedOwners.put(key.executionId() + ":" + controlEpoch, ArtifactExecutionOwner.from(anchor));
        return anchor;
    }

    public synchronized ArtifactExecutionOwner reportedOwner(String tenant, String job, long controlEpoch) {
        var owner = reportedOwners.get(new ArtifactBuildJobKey(tenant, job).executionId() + ":" + controlEpoch);
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
