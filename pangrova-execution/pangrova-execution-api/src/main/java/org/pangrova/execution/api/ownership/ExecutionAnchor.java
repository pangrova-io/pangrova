package org.pangrova.execution.api.ownership;

import org.pangrova.common.api.annotation.Stable;

import java.time.Instant;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Durable ownership identity for one query or background execution.
 *
 * <p>The coordination backend allocates {@code fencingToken}. The token is
 * never reused and is monotonically increasing for an {@code executionId}. It must be verified by
 * every side-effecting sink before publication. Coordinator process restarts
 * are distinguished by {@code ownerIncarnation}; a reused node name is never
 * sufficient to renew or release an older process's anchor.</p>
 *
 * @param executionId execution identifier shared by all of its tasks
 * @param ownerId logical coordinator identifier
 * @param ownerIncarnation unique identity of the owning coordinator process
 * @param fencingToken backend-issued monotonically increasing token
 * @param acquiredAt backend-clock acquisition time
 * @param expiresAt backend-clock lease deadline
 */
@Stable(since = "1.0")
public record ExecutionAnchor(
        String executionId,
        String ownerId,
        String ownerIncarnation,
        long fencingToken,
        Instant acquiredAt,
        Instant expiresAt) {

    public ExecutionAnchor {
        requireText(executionId, "executionId");
        requireText(ownerId, "ownerId");
        requireText(ownerIncarnation, "ownerIncarnation");
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
        Objects.requireNonNull(acquiredAt, "acquiredAt must not be null");
        Objects.requireNonNull(expiresAt, "expiresAt must not be null");
        if (!expiresAt.isAfter(acquiredAt)) {
            throw new IllegalArgumentException("expiresAt must be after acquiredAt");
        }
    }
}
