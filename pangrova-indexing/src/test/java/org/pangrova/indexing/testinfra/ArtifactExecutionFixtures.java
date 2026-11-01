package org.pangrova.indexing.testinfra;

import org.pangrova.indexing.control.ArtifactBuildJobKey;
import org.pangrova.indexing.control.ArtifactExecutionOwner;
import org.pangrova.query.transaction.JdbcExecutionAnchorStore;
import org.pangrova.spi.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Real PostgreSQL ownership transitions and immutable report identities for artifact tests. */
public final class ArtifactExecutionFixtures {
    private final JdbcExecutionAnchorStore anchors;
    private final Map<String, ArtifactExecutionOwner> reportedOwners = new HashMap<>();

    public ArtifactExecutionFixtures(DataSource dataSource) {
        anchors = new JdbcExecutionAnchorStore(dataSource);
    }

    /** Explicitly yields the previous process and acquires a new real backend token for a fixture transition. */
    public synchronized ExecutionAnchor takeOwnership(String tenant, String job, String owner, long controlEpoch) {
        var key = new ArtifactBuildJobKey(tenant, job);
        String incarnation = "fixture-process-" + owner;
        anchors.current(key.executionId()).filter(current -> !current.ownerId().equals(owner)
                || !current.ownerIncarnation().equals(incarnation)).ifPresent(anchors::yield);
        var anchor = anchors.current(key.executionId()).filter(current -> current.ownerId().equals(owner)
                && current.ownerIncarnation().equals(incarnation)).orElseGet(() ->
                        anchors.tryAcquire(key.executionId(), owner, incarnation, Duration.ofMinutes(10)).orElseThrow());
        reportedOwners.put(reportKey(tenant, job, controlEpoch), ArtifactExecutionOwner.from(anchor));
        return anchor;
    }

    /** Returns the original issued identity, including after a replacement owner takes over. */
    public synchronized ArtifactExecutionOwner reportedOwner(String tenant, String job, long controlEpoch) {
        var owner = reportedOwners.get(reportKey(tenant, job, controlEpoch));
        if (owner == null) {
            throw new IllegalStateException("fixture has no originally issued Native owner evidence");
        }
        return owner;
    }

    private static String reportKey(String tenant, String job, long epoch) {
        return new ArtifactBuildJobKey(tenant, job).executionId() + ":" + epoch;
    }
}
