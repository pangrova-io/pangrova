package org.pangrova.shuffle;

/** Resolves the durable owner fence for one query before remote shuffle RPCs. */
@FunctionalInterface
public interface ShuffleExecutionOwnerResolver {

    /** Returns the current durable owner; absence or stale ownership must fail closed. */
    ShuffleExecutionOwner resolve(String queryId);
}
