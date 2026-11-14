package org.pangrova.query.api.write;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.query.api.write.WriteAuthorityRequest;

/**
 * Supplies current authority for every query write, ordinary or generated, before
 * connector allocation. The implementation must recheck current INVOKER permissions,
 * tenant and declaration on its authority transaction; a compiler observation alone
 * is not authorization. Failure must propagate and must not allocate or admit a write.
 *
 * Implementations must support concurrent calls. Resolution is synchronous; this
 * interface has no separate timeout/cancellation channel. The query lifetime owner
 * must still fence allocation and commit after resolution, including cancellation
 * during resolution. A receipt is scoped to this exact declaration.
 *
 * The caller owns the borrowed connector session/query scope. Implementations own and
 * close their temporary database/catalog resources. No receipt permits skipping the
 * existing fenced commit-time authorization. One configured implementation is used
 * for the query lifetime; this contract does not provide implicit fallback or retries.
 */
@Internal
@FunctionalInterface
public interface WriteAuthorityResolver {
    WriteAuthorizationReceipt resolve(WriteAuthorityRequest request);
}
