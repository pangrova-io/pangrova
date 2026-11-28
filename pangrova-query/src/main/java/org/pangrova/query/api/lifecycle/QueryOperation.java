package org.pangrova.query.api.lifecycle;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Retains one physical operation in the query authority that admitted it.
 *
 * <p>The caller owns this handle until its operation has actually exited, including
 * asynchronous callbacks and terminal metadata work. Logical completion, cancellation
 * requests and timeouts do not authorize early release. A handle does not grant new
 * permissions, admit new work, or replace an authorization/fencing check.</p>
 *
 * <p>Implementations retain the original admitted scope. Release must not look up a
 * replacement scope by query ID. Concurrent or repeated closes release that operation
 * at most once; distinct operations have independent handles. Owners must not report
 * the query physically drained while an admitted operation still retains its handle.</p>
 */
@Experimental
@ThreadSafe
public interface QueryOperation extends AutoCloseable {
    /**
     * Records actual exit against the original owner and invokes exit notifications.
     * Repeated closes do nothing, including after a first close throws from a
     * notification. Such an error does not mean the operation was admitted again;
     * callers must preserve it without acquiring a replacement handle to retry close.
     * This method declares no checked exceptions.
     */
    @Override
    void close();
}
