package org.pangrova.server.coordinator.bootstrap.lifecycle;

import java.util.function.Supplier;
import java.util.function.Consumer;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.persistence.query.api.state.QueryStateJournal;

/** Query-side dependency barriers. Accepted Flight readers retain their resources through actual exit. */
@ThreadSafe
public final class QueryShutdownOwnership {
    private static final long QUERY_DRAIN_TIMEOUT_MILLIS = 30_000;
    private final Supplier<QueryManager> queries;
    private final Supplier<QueryStateJournal> journal;
    private final Runnable submissions;
    private final Runnable callbacks;
    private final Consumer<CoordinatorLifecycleAdmission> resources;
    private final CoordinatorLifecycleAdmission progress = new CoordinatorLifecycleAdmission();
    private volatile boolean drained;

    /** Captures live assembly owners, including resources installed before a failed startup. */
    public QueryShutdownOwnership(Supplier<QueryManager> queries, Supplier<QueryStateJournal> journal,
            Runnable submissions, Runnable callbacks, Consumer<CoordinatorLifecycleAdmission> resources) {
        this.queries = java.util.Objects.requireNonNull(queries);
        this.journal = java.util.Objects.requireNonNull(journal);
        this.submissions = java.util.Objects.requireNonNull(submissions);
        this.callbacks = java.util.Objects.requireNonNull(callbacks);
        this.resources = java.util.Objects.requireNonNull(resources);
    }

    /** Stops new frontend admission; existing execution/publication owners remain live. */
    public void sealAdmission() {
        QueryManager owner = queries.get();
        if (owner != null) owner.setAcceptingQueries(false);
    }

    /** Waits for accepted frontend callbacks and actual query physical cleanup, not logical terminal states. */
    public void drainQueries() {
        sealAdmission();
        progress.step("submissions", submissions);
        progress.step("queries", () -> {
            QueryManager owner = queries.get();
            if (owner != null && !owner.awaitAllQueries(QUERY_DRAIN_TIMEOUT_MILLIS)) {
                throw new IllegalStateException("Queries still own execution or publication resources");
            }
        });
    }

    /** Joins actual scheduler/callback owners after the physical query barrier. */
    public void joinSchedulingOwners() {
        drainQueries();
        progress.step("callbacks", callbacks);
    }

    /** Flushes and joins journal ownership before retiring query transport/cache resources. */
    public void close() {
        joinSchedulingOwners();
        QueryStateJournal owner = journal.get();
        Throwable semanticFailure = null;
        if (owner != null) {
            try {
                progress.step("journal", owner::close);
            } catch (RuntimeException | Error failure) {
                if (!owner.isDrained()) throw failure;
                semanticFailure = failure;
            }
        }
        try {
            resources.accept(progress);
            drained = true;
        } catch (RuntimeException | Error failure) {
            if (semanticFailure == null) throw failure;
            if (semanticFailure != failure) semanticFailure.addSuppressed(failure);
        }
        if (semanticFailure instanceof RuntimeException runtime) throw runtime;
        if (semanticFailure instanceof Error error) throw error;
    }

    /** Reports actual dependency exits, independent of a retained sticky semantic journal failure. */
    public boolean isDrained() { return drained; }
}
