package org.pangrova.connector.paimon;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorQueryAuthority;
import org.pangrova.connector.spi.ConnectorQueryOperation;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Owns query-scoped SDK splits and readers without leaking SDK objects into SPI splits. */
@ThreadSafe
public final class PaimonSplitRegistry implements AutoCloseable {
    private static final int DEFAULT_MAX_SIZE = 100_000;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final int maxSize;
    private final long ttlNanos;
    private final Map<Long, Registration> registrations = new HashMap<>();
    private final Map<String, QueryScope> queries = new HashMap<>();
    private boolean sealed;

    /** Creates a bounded registry for this connector incarnation. */
    public PaimonSplitRegistry() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL);
    }

    /** Configures the trusted connector adapter's bounded split retention. */
    public PaimonSplitRegistry(int maxSize, Duration ttl) {
        if (maxSize <= 0 || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("Split capacity and TTL must be positive");
        }
        this.maxSize = maxSize;
        this.ttlNanos = ttl.toNanos();
    }

    static ConnectorQueryAuthority authority(ConnectorSession session) {
        return Objects.requireNonNull(session, "session").getQueryAuthority().orElseThrow(
                () -> new IllegalArgumentException("Paimon read requires the issued query lifetime"));
    }

    /** Opens the trusted adapter scope using the actual engine-issued lifetime. */
    public boolean beginQuery(ConnectorQueryAuthority authority) {
        try (var permit = authority.enterOperation()) {
            return beginActiveQuery(authority);
        }
    }

    private synchronized boolean beginActiveQuery(ConnectorQueryAuthority authority) {
        requireOpen();
        String queryId = requireQueryId(authority.queryId());
        QueryScope scope = queries.get(queryId);
        if (scope != null) {
            requireSame(scope, authority);
            requireActive(scope);
            return false;
        }
        queries.put(queryId, new QueryScope(authority));
        return true;
    }

    ConnectorReadBinding bindRead(ConnectorQueryAuthority authority, PaimonTableHandle selector,
            Supplier<ConnectorReadBinding> resolve) {
        try (var permit = authority.enterOperation()) {
            var proposed = new ReadBinding(selector);
            ReadBinding retained;
            synchronized (this) {
                requireOpen();
                QueryScope scope = requireScope(authority);
                requireActive(scope);
                var key = new SchemaTableName(selector.schemaName(), selector.tableName());
                retained = scope.bindings.putIfAbsent(key, proposed);
            }
            if (retained == null) {
                retained = proposed;
                try {
                    proposed.completed.complete(Objects.requireNonNull(resolve.get(), "read binding"));
                } catch (RuntimeException | Error failure) {
                    proposed.completed.completeExceptionally(failure);
                    throw failure;
                }
            }
            var binding = awaitBinding(retained.completed);
            requireSameSelector(retained.selector, selector, binding);
            var selected = (PaimonTableHandle) binding.handle();
            if (selected.pushedLimit() == selector.pushedLimit()) return binding;
            return new ConnectorReadBinding(selected.withLimit(selector.pushedLimit()), binding.declaration(),
                    binding.statistics(), binding.state(), binding.snapshotId(), binding.sourceIdentity(),
                    binding.schemaIdentity());
        }
    }

    private static ConnectorReadBinding awaitBinding(CompletableFuture<ConnectorReadBinding> completed) {
        try {
            return completed.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw failure;
        }
    }

    private static void requireSameSelector(PaimonTableHandle first, PaimonTableHandle requested,
            ConnectorReadBinding binding) {
        boolean sameSnapshot = first.snapshotId().equals(requested.snapshotId());
        boolean sameBoundSource = binding.sourceIdentity().equals(
                requested.properties().get(PaimonTableHandle.SOURCE_IDENTITY_PROPERTY))
                && requested.snapshotId().equals(((PaimonTableHandle) binding.handle()).snapshotId());
        PaimonTableHandle expected = sameBoundSource ? (PaimonTableHandle) binding.handle() : first;
        if ((!sameSnapshot && !sameBoundSource) || !expected.withoutSnapshotId().withLimit(PaimonTableHandle.NO_LIMIT)
                .equals(requested.withoutSnapshotId().withLimit(PaimonTableHandle.NO_LIMIT))) {
            throw new IllegalStateException("Paimon table read selector differs from its first-access binding");
        }
    }

    /** Retains the selected SDK handles; this is not SQL authorization or an immutable SDK copy. */
    public long register(ConnectorQueryAuthority authority, Table table, Split split) {
        try (var permit = authority.enterOperation()) {
            return registerActive(authority, table, split);
        }
    }

    private synchronized long registerActive(ConnectorQueryAuthority authority, Table table, Split split) {
        requireOpen();
        QueryScope scope = requireScope(authority);
        requireActive(scope);
        removeExpiredRegistrations();
        if (registrations.size() >= maxSize) {
            throw new IllegalStateException("Paimon split registry capacity exhausted: " + maxSize);
        }
        long id = ID_GENERATOR.incrementAndGet();
        if (id <= 0) throw new IllegalStateException("Paimon split identifier exhausted");
        registrations.put(id, new Registration(scope, Objects.requireNonNull(table, "table"),
                Objects.requireNonNull(split, "split"), System.nanoTime()));
        scope.ids.add(id);
        return id;
    }

    /** Transfers a retained payload to the real PageSource adapter and its operation permit. */
    public ReadLease claim(ConnectorQueryAuthority authority, long splitId) {
        ConnectorQueryOperation permit = authority.enterOperation();
        try {
            return claimActive(authority, splitId, permit);
        } catch (RuntimeException | Error failure) {
            try {
                permit.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private synchronized ReadLease claimActive(ConnectorQueryAuthority authority, long splitId,
            ConnectorQueryOperation permit) {
        requireOpen();
        if (splitId <= 0) {
            throw new IllegalStateException("native-only split: JVM read path should not be invoked: " + splitId);
        }
        QueryScope scope = requireScope(authority);
        removeExpiredRegistrations();
        Registration registration = registrations.get(splitId);
        if (registration == null) throw notFound(splitId);
        requireActive(scope);
        if (registration.scope != scope) {
            throw new IllegalStateException("Paimon SDK Split belongs to another query: " + splitId);
        }
        if (registration.split == null) {
            throw new IllegalStateException("Paimon SDK Split already consumed: " + splitId);
        }
        ReadLease lease = new ReadLease(this, scope, splitId, registration.table, registration.split, permit);
        scope.readers.put(splitId, lease);
        registration.table = null;
        registration.split = null;
        return lease;
    }

    /** Seals query admission; cleanup fails until its actual readers have exited. */
    public void releaseQuery(ConnectorQueryAuthority authority) {
        if (!authority.isClosed()) throw new IllegalStateException("Paimon cleanup requires closed query admission");
        authority.requireDrained();
        releaseClosedQuery(authority);
    }

    private synchronized void releaseClosedQuery(ConnectorQueryAuthority authority) {
        String queryId = requireQueryId(authority.queryId());
        QueryScope scope = queries.get(queryId);
        if (scope == null) return;
        requireSame(scope, authority);
        scope.closing = true;
        for (long id : scope.ids) registrations.remove(id);
        scope.ids.clear();
        if (!scope.readers.isEmpty()) {
            throw new IllegalStateException("Paimon query still owns active readers: " + queryId);
        }
        queries.remove(queryId, scope);
    }

    /** Counts pending SDK payloads without granting access to them. */
    public synchronized int size() {
        removeExpiredRegistrations();
        return (int) registrations.values().stream().filter(entry -> entry.split != null).count();
    }

    synchronized PaimonConnector.LifecycleDiagnostics lifecycleDiagnostics() {
        return new PaimonConnector.LifecycleDiagnostics(size(),
                queries.values().stream().mapToInt(scope -> scope.readers.size()).sum());
    }

    /** Seals all admission before closing the catalog, retaining failed reader ownership. */
    @Override
    public synchronized void close() {
        sealed = true;
        registrations.clear();
        for (QueryScope scope : queries.values()) {
            scope.closing = true;
            scope.ids.clear();
            if (scope.bindings.values().stream().anyMatch(binding -> !binding.completed.isDone())) {
                throw new IllegalStateException("Paimon connector still owns active read binding resolution: "
                        + scope.queryId);
            }
            if (!scope.readers.isEmpty()) {
                throw new IllegalStateException("Paimon connector still owns active readers: " + scope.queryId);
            }
        }
        queries.clear();
    }

    private synchronized void readerClosed(QueryScope scope, long id, ReadLease lease) {
        if (!scope.readers.remove(id, lease)) {
            throw new IllegalStateException("Paimon reader lease ownership mismatch: " + id);
        }
    }

    private void removeExpiredRegistrations() {
        long now = System.nanoTime();
        registrations.entrySet().removeIf(entry -> {
            Registration registration = entry.getValue();
            if (registration.scope.readers.containsKey(entry.getKey())) return false;
            if (now - registration.registeredNanos < ttlNanos) return false;
            registration.scope.ids.remove(entry.getKey());
            return true;
        });
    }

    private QueryScope requireScope(ConnectorQueryAuthority authority) {
        String queryId = requireQueryId(authority.queryId());
        QueryScope scope = queries.get(queryId);
        if (scope == null) throw new IllegalStateException("No active Paimon query scope: " + queryId);
        requireSame(scope, authority);
        return scope;
    }

    private static void requireSame(QueryScope scope, ConnectorQueryAuthority authority) {
        if (scope.authority != authority) throw new IllegalStateException("Paimon query lifetime owner mismatch");
    }

    private void requireOpen() {
        if (sealed) throw new IllegalStateException("Paimon split registry is closed");
    }

    private static void requireActive(QueryScope scope) {
        if (scope.closing) throw new IllegalStateException("Paimon query scope is closing: " + scope.queryId);
    }

    private static String requireQueryId(String queryId) {
        if (queryId == null || queryId.isBlank()) throw new IllegalArgumentException("Paimon read requires queryId");
        return queryId;
    }

    private static IllegalStateException notFound(long splitId) {
        return new IllegalStateException("No Paimon SDK Split registered for id=" + splitId);
    }

    private static final class QueryScope {
        private final String queryId;
        private final ConnectorQueryAuthority authority;
        private final Set<Long> ids = new HashSet<>();
        private final Map<Long, ReadLease> readers = new HashMap<>();
        private final Map<SchemaTableName, ReadBinding> bindings = new HashMap<>();
        private boolean closing;

        private QueryScope(ConnectorQueryAuthority authority) {
            this.authority = authority;
            this.queryId = authority.queryId();
        }
    }

    private static final class ReadBinding {
        private final PaimonTableHandle selector;
        private final CompletableFuture<ConnectorReadBinding> completed = new CompletableFuture<>();

        private ReadBinding(PaimonTableHandle selector) {
            this.selector = selector;
        }
    }

    private static final class Registration {
        private final QueryScope scope;
        private final long registeredNanos;
        private Table table;
        private Split split;

        private Registration(QueryScope scope, Table table, Split split, long registeredNanos) {
            this.scope = scope;
            this.table = table;
            this.split = split;
            this.registeredNanos = registeredNanos;
        }
    }

    /** Owns one SDK reader until close succeeds; failed closes retain the query lease. */
    public static final class ReadLease implements java.io.Closeable {
        private final PaimonSplitRegistry registry;
        private final QueryScope scope;
        private final long splitId;
        private final Table table;
        private final Split split;
        private final ConnectorQueryOperation permit;
        private final AtomicBoolean closing = new AtomicBoolean();
        private RecordReader<InternalRow> reader;
        private volatile boolean closed;

        private ReadLease(PaimonSplitRegistry registry, QueryScope scope, long splitId, Table table, Split split,
                ConnectorQueryOperation permit) {
            this.registry = registry;
            this.scope = scope;
            this.splitId = splitId;
            this.table = table;
            this.split = split;
            this.permit = permit;
        }

        /** Returns the actual table selected when this split was planned. */
        Table table() { return table; }

        /** Borrows the retained SDK payload; this does not grant a new read or extend the lease. */
        public Split split() { return split; }

        RecordReader<InternalRow> openReader() throws IOException {
            if (reader != null || closed) throw new IllegalStateException("Paimon reader already opened or closed");
            reader = table.newReadBuilder().newRead().createReader(split);
            return reader;
        }

        /** Releases the SDK reader before releasing its query ownership. */
        @Override
        public void close() throws IOException {
            if (closed) return;
            if (!closing.compareAndSet(false, true)) throw new IllegalStateException("Paimon reader close in progress");
            try {
                if (reader != null) reader.close();
                registry.readerClosed(scope, splitId, this);
                permit.close();
                closed = true;
            } finally {
                closing.set(false);
            }
        }
    }
}
