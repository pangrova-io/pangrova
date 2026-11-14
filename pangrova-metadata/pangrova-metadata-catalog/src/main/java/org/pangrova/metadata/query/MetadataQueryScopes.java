package org.pangrova.metadata.query;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.metadata.ConnectorMetadataScope;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.DefaultConnectorSession;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Indexes actual query owners and projects their catalog lifecycle outcomes for service metrics. */
@ThreadSafe
public final class MetadataQueryScopes {
    private final CatalogRegistry catalogs;
    private final Map<String, Owner> owners = new ConcurrentHashMap<>();

    private final AtomicLong beginQuerySuccessTotal = new AtomicLong();
    private final AtomicLong beginQueryFailureTotal = new AtomicLong();
    private final AtomicLong cleanupQuerySuccessTotal = new AtomicLong();
    private final AtomicLong cleanupQueryFailureTotal = new AtomicLong();

    public MetadataQueryScopes(CatalogRegistry catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    public void begin(MetadataSessionContext session) {
        var result = open(session);
        beginQuerySuccessTotal.addAndGet(result.succeeded());
        beginQueryFailureTotal.addAndGet(result.failures().size());
        result.requireSuccess(session.queryId(), "beginQuery");
    }

    public void cleanup(ConnectorQueryAuthority authority) {
        var result = release(authority);
        cleanupQuerySuccessTotal.addAndGet(result.succeeded());
        cleanupQueryFailureTotal.addAndGet(result.failures().size());
        result.requireSuccess(authority.queryId(), "cleanupQuery");
    }

    public long beginQuerySuccessTotal() { return beginQuerySuccessTotal.get(); }
    public long beginQueryFailureTotal() { return beginQueryFailureTotal.get(); }
    public long cleanupQuerySuccessTotal() { return cleanupQuerySuccessTotal.get(); }
    public long cleanupQueryFailureTotal() { return cleanupQueryFailureTotal.get(); }

    private CatalogOutcomes open(MetadataSessionContext session) {
        var authority = ConnectorMetadataScope.require(session.queryAuthority().orElseThrow(
                () -> new IllegalArgumentException("Metadata begin requires the issued query lifetime")));
        try (var permit = authority.enterOperation()) {
            Owner candidate = new Owner(authority);
            Owner previous = owners.putIfAbsent(authority.queryId(), candidate);
            if (previous != null) {
                previous.requireCompletedBegin(authority);
                return new CatalogOutcomes(0, Map.of());
            }
            return beginCatalogs(session, candidate);
        }
    }

    private CatalogOutcomes beginCatalogs(MetadataSessionContext session, Owner owner) {
        int succeeded = 0;
        Map<String, Exception> failed = new LinkedHashMap<>();
        boolean completed = false;
        try {
            for (String name : catalogs.listCatalogNames()) {
                try {
                    var actor = DefaultConnectorSession.fromSessionContext(session, name, session.currentSchema());
                    catalogs.metadataForQuery(name, actor);
                    succeeded++;
                } catch (Exception failure) {
                    failed.put(name, failure);
                }
            }
            completed = failed.isEmpty();
            return new CatalogOutcomes(succeeded, failed);
        } finally {
            owner.finishBegin(completed);
        }
    }

    private CatalogOutcomes release(ConnectorQueryAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        if (!authority.isClosed()) throw new IllegalStateException("Metadata cleanup requires closed query admission");
        authority.requireDrained();
        Owner owner = owners.get(authority.queryId());
        if (owner == null) {
            return new CatalogOutcomes(0, Map.of());
        }
        owner.claimCleanup(authority);
        try {
            var outcome = owner.authority.cleanupMetadata();
            if (outcome.failures().isEmpty()) owners.remove(authority.queryId(), owner);
            return new CatalogOutcomes(outcome.releasedCatalogs().size(), new LinkedHashMap<>(outcome.failures()));
        } finally {
            owner.finishCleanup();
        }
    }

    public int activeQueryCount() { return owners.size(); }

    private record CatalogOutcomes(int succeeded, Map<String, Exception> failures) {
        void requireSuccess(String queryId, String operation) {
            if (!failures.isEmpty()) {
                throw new MetadataResolutionException(queryId, operation, failures);
            }
        }
    }

    private static final class Owner {
        private final ConnectorMetadataScope authority;
        private boolean beginning = true;
        private boolean beganSuccessfully;
        private boolean cleaning;

        Owner(ConnectorMetadataScope authority) { this.authority = authority; }

        synchronized void requireCompletedBegin(ConnectorQueryAuthority requested) {
            requireSame(requested);
            if (beginning || !beganSuccessfully || cleaning) {
                throw new IllegalStateException("Metadata query begin is incomplete or cleanup is active");
            }
        }

        synchronized void finishBegin(boolean success) {
            beginning = false;
            beganSuccessfully = success;
        }

        synchronized void claimCleanup(ConnectorQueryAuthority requested) {
            requireSame(requested);
            if (beginning || cleaning) throw new IllegalStateException("Metadata lifecycle operation remains active");
            cleaning = true;
        }

        synchronized void finishCleanup() {
            cleaning = false;
        }

        private void requireSame(ConnectorQueryAuthority requested) {
            if (authority != requested) {
                throw new IllegalStateException("Metadata query lifetime differs from its owner");
            }
        }
    }
}
