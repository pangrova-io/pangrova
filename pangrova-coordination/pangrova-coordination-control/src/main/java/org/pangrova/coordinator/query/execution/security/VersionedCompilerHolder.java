package org.pangrova.coordinator.query.execution.security;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.planner.analysis.SqlCompiler;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Thread-safe holder for a versioned SqlCompiler reference.
 * Uses {@link AtomicReference} to ensure atomic reads and updates,
 * eliminating the BUG-D45 class of problems where a volatile field
 * could be read at different versions within the same query execution.
 *
 * <p>Callers should snapshot the compiler at query start via {@link #snapshot()}
 * and use that snapshot consistently throughout the query lifecycle.</p>
 */
public final class VersionedCompilerHolder {

    /**
     * Immutable snapshot of a compiler and its version.
     * The version monotonically increases on each schema change.
     */
    public record CompilerSnapshot(SqlCompiler compiler, long version) {
        public CompilerSnapshot {
            checkNotNull(compiler, "compiler");
            if (version < 0) {
                throw new IllegalArgumentException("version must be non-negative, got: " + version);
            }
        }
    }

    private final AtomicReference<CompilerSnapshot> ref;

    public VersionedCompilerHolder(SqlCompiler initialCompiler) {
        checkNotNull(initialCompiler, "initialCompiler");
        this.ref = new AtomicReference<>(new CompilerSnapshot(initialCompiler, 0));
    }

    /**
     * Returns the current compiler snapshot atomically.
     * Use this at query start to pin a consistent compiler version.
     */
    public CompilerSnapshot snapshot() {
        return ref.get();
    }

    /**
     * Returns the current compiler (convenience for callers that
     * do not need version tracking).
     */
    public SqlCompiler compiler() {
        return ref.get().compiler();
    }

    /**
     * Atomically updates the compiler to a new version.
     * The version is incremented from the current snapshot's version.
     *
     * @param newCompiler the updated compiler after schema change
     */
    public void update(SqlCompiler newCompiler) {
        checkNotNull(newCompiler, "newCompiler");
        ref.updateAndGet(current ->
                new CompilerSnapshot(newCompiler, current.version() + 1));
    }
}
