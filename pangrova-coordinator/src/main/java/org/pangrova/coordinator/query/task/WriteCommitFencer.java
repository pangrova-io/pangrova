package org.pangrova.coordinator.query.task;

/**
 * A-FENCE-01: leadership fencing gate for the distributed-write catalog-commit path.
 *
 * <p>Before a coordinator performs the single atomic catalog commit
 * ({@code finishDistributedWrite}), it consults this gate. A coordinator that has been DEMOTED
 * (lost its lease; another node became leader) must NOT commit — doing so would be a
 * "zombie leader" write that races the new leader. When fenced, the durable {@code PENDING} intent
 * is left in place so the new leader's {@link WriteCommitReconciler} auto-finishes it
 * (A-COMMIT-01), composing the two fixes.</p>
 *
 * <p>Also supplies the owning coordinator id and current term stamped onto the durable intent for
 * owner-scoped recovery and observability. Fully partition-tolerant fencing (comparing against an
 * <em>authoritative</em> term read from the shared lease store) is folded into the A-HA-01
 * quorum-store migration; this gate covers the in-process demoted-leader case.</p>
 */
public interface WriteCommitFencer {

    /** The current leadership term (epoch) as this coordinator sees it. */
    long currentTerm();

    /** This coordinator's node id, stamped onto durable write intents. */
    String coordinatorId();

    /** Whether this coordinator is still the active leader (false once demoted). */
    boolean isActiveLeader();
}
