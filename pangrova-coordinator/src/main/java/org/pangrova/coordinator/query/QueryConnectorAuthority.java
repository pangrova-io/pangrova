package org.pangrova.coordinator.query;

import org.pangrova.spi.connector.ConnectorQueryAuthority;
import org.pangrova.spi.connector.ConnectorQueryOperation;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorQueryCleanupResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Binds connector admission to the actual query state and its issued-operation owner. */
@org.pangrova.common.concurrent.ThreadSafe
final class QueryConnectorAuthority implements ConnectorQueryAuthority {
    private final QueryStateMachine state;
    private final ConnectorQueryAuthority issued;
    private final Map<String, MetadataBegin> metadata = new LinkedHashMap<>();
    private boolean cleaning;

    QueryConnectorAuthority(QueryStateMachine state, ConnectorQueryAuthority issued) {
        this.state = state;
        this.issued = issued;
    }

    boolean matches(ConnectorQueryAuthority other) {
        return issued == other;
    }

    @Override public String queryId() {
        return issued.queryId();
    }

    @Override public boolean isClosed() {
        return state.isTerminal() || issued.isClosed();
    }

    @Override public void requireDrained() {
        issued.requireDrained();
    }

    @Override public ConnectorQueryOperation enterOperation() {
        return state.enterConnectorOperation(issued);
    }

    @Override
    public ConnectorMetadata beginMetadata(String catalog, java.util.function.Supplier<ConnectorMetadata> lookup) {
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("Resolved catalog is required");
        }
        java.util.Objects.requireNonNull(lookup, "lookup");
        try (var permit = enterOperation()) {
            var candidate = new MetadataBegin();
            MetadataBegin existing;
            synchronized (this) {
                existing = metadata.putIfAbsent(catalog, candidate);
            }
            if (existing != null) {
                return awaitBegin(existing.completed);
            }
            try {
                ConnectorMetadata resolved = java.util.Objects.requireNonNull(lookup.get(), "resolved metadata");
                synchronized (this) {
                    candidate.owner = resolved;
                }
                resolved.beginQuery(this);
                candidate.completed.complete(resolved);
                return resolved;
            } catch (RuntimeException | Error failure) {
                candidate.completed.completeExceptionally(failure);
                throw failure;
            }
        }
    }

    private static ConnectorMetadata awaitBegin(CompletableFuture<ConnectorMetadata> began) {
        try {
            return began.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof Error error) {
                throw error;
            }
            if (failure.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw failure;
        }
    }

    @Override
    public ConnectorQueryCleanupResult cleanupMetadata() {
        if (!isClosed()) {
            throw new IllegalStateException("Metadata cleanup requires closed query admission");
        }
        requireDrained();
        Map<String, MetadataBegin> retained;
        synchronized (this) {
            if (cleaning) {
                throw new IllegalStateException("Metadata cleanup is already active");
            }
            retained = new LinkedHashMap<>(metadata);
            cleaning = true;
        }
        Error error = null;
        var released = new ArrayList<String>();
        Map<String, RuntimeException> failures = new LinkedHashMap<>();
        try {
            for (var entry : retained.entrySet()) {
                try {
                    if (entry.getValue().owner != null) {
                        entry.getValue().owner.cleanupQuery(this);
                    }
                    synchronized (this) {
                        metadata.remove(entry.getKey(), entry.getValue());
                    }
                    released.add(entry.getKey());
                } catch (RuntimeException rejected) {
                    failures.put(entry.getKey(), rejected);
                } catch (Error rejected) {
                    if (error == null) {
                        error = rejected;
                    } else if (error != rejected) {
                        error.addSuppressed(rejected);
                    }
                }
            }
        } finally {
            synchronized (this) {
                cleaning = false;
            }
        }
        if (error != null) {
            for (var failure : failures.values()) {
                error.addSuppressed(failure);
            }
            throw error;
        }
        return new ConnectorQueryCleanupResult(released, failures);
    }

    private static final class MetadataBegin {
        private final CompletableFuture<ConnectorMetadata> completed = new CompletableFuture<>();
        private ConnectorMetadata owner;
    }
}
