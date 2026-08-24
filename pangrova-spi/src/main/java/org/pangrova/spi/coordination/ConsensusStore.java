package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI that formalizes the coordinator state replication contract for HA.
 *
 * <p>Pangrova's HA model is single-leader with JDBC lease-based election (ADR decision (a)):
 * coordinator state is replicated via an external RDBMS (PostgreSQL), not an embedded
 * consensus protocol (Raft/Paxos). This is an explicit architectural choice, documented in
 * docs/architecture/01 and docs/architecture/08.</p>
 *
 * <h3>Design rationale</h3>
 * <ul>
 *   <li>The external RDBMS provides linearizable reads/writes via row-level locking, which
 *       is sufficient for single-leader election with epoch-based fencing.</li>
 *   <li>TTL-based lease expiration relies on the RDBMS clock, not inter-node clock agreement.
 *       Clock skew between coordinator nodes is bounded by the lease TTL margin
 *       ({@code leaseTimeoutMs - heartbeatIntervalMs}).</li>
 *   <li>No quorum is needed because there is at most one leader at any time, enforced by
 *       the RDBMS row-level lock + epoch monotonicity.</li>
 * </ul>
 *
 * <h3>Capabilities</h3>
 * <p>A {@code ConsensusStore} bundles three concerns that together define the coordinator
 * state replication surface:</p>
 * <ol>
 *   <li>{@link DistributedLease} — leader election via time-bounded leases with epoch fencing</li>
 *   <li>{@link QueryStateJournal} — write-ahead journal for query/task state transitions</li>
 *   <li>{@link CoordinatorRegistry} — coordinator self-registration and discovery</li>
 * </ol>
 *
 * <p>Implementations must be thread-safe. The default in-tree implementation delegates
 * to JDBC-backed stores ({@code JdbcDistributedLease}, {@code JdbcQueryStateJournal},
 * {@code JdbcCoordinatorRegistry}). Alternative backends (etcd, ZooKeeper) may be
 * provided via this SPI.</p>
 *
 * @see DistributedLease
 * @see QueryStateJournal
 * @see CoordinatorRegistry
 */
@Experimental(reason = "G-HIGH_AVAILABILITY-03: formalizing coordinator state replication contract; "
        + "JDBC-lease model is the current implementation, alternative backends may evolve")
public interface ConsensusStore {

    /**
     * The consensus model this store implements.
     *
     * <p>Used for observability and documentation — the scheduler and HA service
     * never branch on this value; they consult the individual capability SPIs instead.</p>
     */
    enum Model {
        /** Single external RDBMS (current default): JDBC lease + journal + registry. */
        JDBC_LEASE,
        /** Embedded Raft consensus (future option): self-contained, no external dependency. */
        EMBEDDED_RAFT,
        /** In-memory only: single-node / test deployments, no persistence. */
        IN_MEMORY
    }

    /** Returns the consensus model backing this store. */
    Model model();

    /** Returns the distributed lease for leader election. */
    DistributedLease lease();

    /** Returns the query state journal for HA recovery. */
    QueryStateJournal journal();

    /**
     * Returns the coordinator registry for self-registration and discovery.
     *
     * <p>May return {@code null} if the store does not support coordinator registration
     * (e.g., single-node in-memory deployments).</p>
     */
    CoordinatorRegistry registry();

    /**
     * Returns the write commit journal for distributed write idempotency.
     *
     * <p>May return {@code null} if the store does not support write commit journaling
     * (e.g., in-memory deployments where crash recovery is not needed).</p>
     */
    default WriteCommitJournal writeCommitJournal() {
        return null;
    }

    /**
     * Returns a human-readable description of the backing store for diagnostics.
     * Example: {@code "JDBC (postgresql://host:5432/pangrova)"} or {@code "in-memory"}.
     */
    String describe();
}
