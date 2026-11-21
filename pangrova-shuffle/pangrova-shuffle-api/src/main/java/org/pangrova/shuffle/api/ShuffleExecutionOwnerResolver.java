package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/** Resolves the durable owner fence for one query before remote shuffle RPCs. */
@FunctionalInterface
@Experimental(reason = "Owner resolution will be scoped by the generic shuffle session contract")
public interface ShuffleExecutionOwnerResolver {

    /** Returns the current durable owner; absence or stale ownership must fail closed. */
    ShuffleExecutionOwner resolve(String queryId);
}
