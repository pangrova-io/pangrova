package org.pangrova.security.session;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.pangrova.security.api.session.SessionConfig;

/**
 * Tracks active user sessions with configurable idle timeout, absolute timeout,
 * and concurrent session limits per user.
 *
 * <p>Sessions are created on successful authentication and invalidated on logout,
 * idle expiry, absolute expiry, or when concurrent session limits are exceeded.</p>
 */
@ThreadSafe
public class SessionTracker implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTracker.class);

    /** Interval for automatic expired session cleanup (5 minutes). */
    private static final long CLEANUP_INTERVAL_MINUTES = 5;

    private final SessionConfig config;
    private final Clock clock;
    private final ConcurrentMap<String, SessionEntry> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<String>> userSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    /**
     * Creates a session tracker using the UTC system clock.
     *
     * @param config session configuration
     */
    public SessionTracker(SessionConfig config) {
        this(config, Clock.systemUTC());
    }

    /**
     * Creates a session tracker using the supplied clock.
     *
     * @param config session configuration
     * @param clock session time source
     */
    public SessionTracker(SessionConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "session-cleanup");
            t.setDaemon(true);
            return t;
        });
        cleanupExecutor.scheduleAtFixedRate(
                this::evictExpired,
                CLEANUP_INTERVAL_MINUTES,
                CLEANUP_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    /**
     * Creates a new session for the given user. If the user already has the maximum
     * number of concurrent sessions, the oldest session is invalidated.
     *
     * @param principal user identity
     * @return new session ID
     */
    public String createSession(String principal) {
        evictExpired();
        enforceMaxConcurrent(principal);

        String sessionId = UUID.randomUUID().toString();
        Instant now = clock.instant();
        SessionEntry entry = new SessionEntry(sessionId, principal, now, now);
        sessions.put(sessionId, entry);

        userSessions.compute(principal, (key, existing) -> {
            var list = new CopyOnWriteArrayList<String>();
            if (existing != null) {
                list.addAll(existing);
            }
            list.add(sessionId);
            return list;
        });

        LOG.info("session_created sessionId={} principal={}", sessionId, principal);
        return sessionId;
    }

    /**
     * Validates a session: checks existence, idle timeout, and absolute timeout.
     * Refreshes last-access time on successful validation.
     *
     * @param sessionId session to validate
     * @return true if session is valid and active
     */
    public boolean validateSession(String sessionId) {
        SessionEntry entry = sessions.get(sessionId);
        if (entry == null) {
            return false;
        }

        Instant now = clock.instant();
        if (isExpired(entry, now)) {
            invalidateSession(sessionId);
            return false;
        }

        entry.refreshLastAccess(now);
        return true;
    }

    /**
     * Invalidates a session explicitly (logout).
     */
    public void invalidateSession(String sessionId) {
        SessionEntry removed = sessions.remove(sessionId);
        if (removed != null) {
            userSessions.computeIfPresent(removed.principal(), (key, list) -> {
                list.remove(sessionId);
                return list.isEmpty() ? null : list;
            });
            LOG.info("session_invalidated sessionId={} principal={}", sessionId, removed.principal());
        }
    }

    /**
     * Regenerates a session ID after successful authentication to prevent
     * session fixation attacks. Invalidates the old session and creates a
     * new one for the same principal, preserving session continuity.
     *
     * @param oldSessionId the pre-authentication session ID to replace
     * @return the new session ID, or null if the old session was not found
     */
    public String regenerateSession(String oldSessionId) {
        SessionEntry old = sessions.get(oldSessionId);
        if (old == null) {
            return null;
        }
        invalidateSession(oldSessionId);
        return createSession(old.principal());
    }

    /**
     * Returns the number of active sessions.
     */
    public int activeSessionCount() {
        return sessions.size();
    }

    @Override
    public void close() {
        cleanupExecutor.shutdownNow();
        LOG.info("event=session_tracker_closed activeSessions={}", sessions.size());
    }

    /**
     * Removes all expired sessions. Should be called periodically.
     */
    public void evictExpired() {
        Instant now = clock.instant();
        for (Map.Entry<String, SessionEntry> e : sessions.entrySet()) {
            if (isExpired(e.getValue(), now)) {
                invalidateSession(e.getKey());
            }
        }
    }

    private boolean isExpired(SessionEntry entry, Instant now) {
        long idleMs = now.toEpochMilli() - entry.lastAccessTime().toEpochMilli();
        if (idleMs > config.idleTimeout().toMillis()) {
            return true;
        }
        long absoluteMs = now.toEpochMilli() - entry.createdAt().toEpochMilli();
        return absoluteMs > config.absoluteTimeout().toMillis();
    }

    private void enforceMaxConcurrent(String principal) {
        List<String> existing = userSessions.get(principal);
        if (existing == null) {
            return;
        }
        while (existing.size() >= config.maxConcurrentPerUser()) {
            if (existing.isEmpty()) {
                break;
            }
            String oldest = existing.get(0);
            invalidateSession(oldest);
            LOG.info("session_evicted_concurrent sessionId={} principal={}", oldest, principal);
        }
    }

    /**
     * Mutable session entry tracking creation and last access times.
     */
    private static final class SessionEntry {
        private final String sessionId;
        private final String principal;
        private final Instant createdAt;
        private volatile Instant lastAccess;

        SessionEntry(String sessionId, String principal, Instant createdAt, Instant lastAccess) {
            this.sessionId = sessionId;
            this.principal = principal;
            this.createdAt = createdAt;
            this.lastAccess = lastAccess;
        }

        String sessionId() {
            return sessionId;
        }

        String principal() {
            return principal;
        }

        Instant createdAt() {
            return createdAt;
        }

        Instant lastAccessTime() {
            return lastAccess;
        }

        void refreshLastAccess(Instant now) {
            this.lastAccess = now;
        }
    }
}
