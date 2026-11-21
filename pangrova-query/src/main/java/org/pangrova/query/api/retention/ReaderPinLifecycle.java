package org.pangrova.query.api.retention;

import java.util.Collection;
import org.pangrova.common.api.annotation.Internal;

/**
 * Maintains generation reader leases for queries owned by this process.
 *
 * <p>The caller borrows this port; its provider and storage are closed by Server.
 * Release is idempotent and is called only after physical execution and issuance
 * have drained, not merely when the query becomes logically terminal. Providers
 * must tolerate concurrent renewal/release without recreating released pins.</p>
 *
 * <p>Renewal may partially succeed across queries; failures must be reported by
 * throwing, never by silently switching storage. It does not acquire new pins or
 * confer authorization. Neither operation promises an unbounded lease during
 * process pauses or storage outages. Implementations must bound storage calls.</p>
 */
@Internal
public interface ReaderPinLifecycle {
    /** Releases all reader pins held by this process for the physically drained query. */
    void release(String queryId);

    /** Renews existing pins for queries whose physical cleanup remains outstanding. */
    void renew(Collection<String> queryIds);

    /** Positive, stable renewal interval in nanoseconds, safely within the provider lease. */
    long renewIntervalNanos();
}
