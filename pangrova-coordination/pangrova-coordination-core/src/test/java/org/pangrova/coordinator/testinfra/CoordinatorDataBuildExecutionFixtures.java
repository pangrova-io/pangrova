package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.databuild.DataBuildExecutionOwnership;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;
import org.pangrova.query.store.jdbc.anchor.JdbcExecutionAnchorStore;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/** Real HA processes sharing PostgreSQL execution ownership for artifact tests. */
public final class CoordinatorDataBuildExecutionFixtures implements AutoCloseable {
    private final DataSource dataSource;
    private final JdbcExecutionAnchorStore anchors;
    private final Map<String, HighAvailabilityCoordinationService> processes = new HashMap<>();
    private final Map<String, DataBuildExecutionOwner> reportedOwners = new HashMap<>();

    public CoordinatorDataBuildExecutionFixtures(DataSource dataSource) {
        this.dataSource = dataSource;
        anchors = new JdbcExecutionAnchorStore(dataSource);
    }

    public synchronized DataBuildExecutionOwnership ownership(String owner) {
        var service = processes.computeIfAbsent(owner, id -> {
            var started = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(),
                    HighAvailabilityCoordinationService.CoordinationConfig.defaults());
            started.setExecutionAnchorStore(new JdbcExecutionAnchorStore(dataSource));
            started.start(id);
            return started;
        });
        return new DataBuildExecutionOwnership(service, owner);
    }

    /** Performs an explicit real backend yield/takeover and retains the original report evidence. */
    public synchronized ExecutionAnchor takeOwnership(String tenant, String job, String owner, long controlEpoch) {
        var key = new DataBuildJobKey(tenant, job);
        var ownership = ownership(owner);
        anchors.current(key.executionId()).filter(current -> !current.ownerId().equals(owner)).ifPresent(anchors::yield);
        var anchor = ownership.acquire(key).orElseThrow();
        reportedOwners.put(key.executionId() + ":" + controlEpoch, DataBuildExecutionOwner.from(anchor));
        return anchor;
    }

    public synchronized DataBuildExecutionOwner reportedOwner(String tenant, String job, long controlEpoch) {
        var owner = reportedOwners.get(new DataBuildJobKey(tenant, job).executionId() + ":" + controlEpoch);
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
