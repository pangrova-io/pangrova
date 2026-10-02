package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

import java.util.Optional;

/** Exact redo identity and acknowledgement of committed deferred maintenance. */
@Stable(since = "1.0")
public interface WriteCommitOutbox {
    /** Resolves the immutable authenticated write behind a connector event. */
    Optional<WriteCommitEntry> findByFingerprint(String commitFingerprint);

    /** Returns persistent acknowledgement; absence of the exact COMMITTED anchor is an error. */
    boolean isDeferredWorkAcknowledged(String commitFingerprint);

    /** Acknowledges an exact COMMITTED declaration with no generated columns, never a generated build. */
    void acknowledgeNoDeferredWork(String commitFingerprint);
}
