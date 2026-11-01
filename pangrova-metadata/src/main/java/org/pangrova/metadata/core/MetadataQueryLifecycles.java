package org.pangrova.metadata.core;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.spi.connector.ConnectorQueryAuthority;

import org.pangrova.common.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Indexes actual query owners and projects their catalog lifecycle outcomes for service metrics. */
@ThreadSafe
final class MetadataQueryLifecycles {
    private final CatalogRegistry catalogs;
    private final Map<String, Owner> owners = new ConcurrentHashMap<>();

    MetadataQueryLifecycles(CatalogRegistry catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    QueryLifecycleReport begin(MetadataSessionContext session) {
        var authority = session.queryAuthority().orElseThrow(
                () -> new IllegalArgumentException("Metadata begin requires the issued query lifetime"));
        try (var permit = authority.enterOperation()) {
            Owner candidate = new Owner(authority);
            Owner previous = owners.putIfAbsent(authority.queryId(), candidate);
            if (previous != null) {
                previous.requireCompletedBegin(authority);
                return report(authority.queryId(), QueryLifecycleReport.Operation.BEGIN, List.of(), Map.of());
            }
            return beginCatalogs(session, candidate);
        }
    }

    private QueryLifecycleReport beginCatalogs(MetadataSessionContext session, Owner owner) {
        List<String> succeeded = new ArrayList<>();
        Map<String, Exception> failed = new LinkedHashMap<>();
        boolean completed = false;
        try {
            for (String name : catalogs.listCatalogNames()) {
                try {
                    var actor = DefaultConnectorSession.fromSessionContext(session, name, session.currentSchema());
                    owner.authority.beginMetadata(name, () -> catalogs.getCatalog(name).getMetadata(actor));
                    succeeded.add(name);
                } catch (Exception failure) {
                    failed.put(name, failure);
                }
            }
            completed = failed.isEmpty();
            return report(owner.authority.queryId(), QueryLifecycleReport.Operation.BEGIN, succeeded, failed);
        } finally {
            owner.finishBegin(completed);
        }
    }

    QueryLifecycleReport cleanup(ConnectorQueryAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        if (!authority.isClosed()) throw new IllegalStateException("Metadata cleanup requires closed query admission");
        authority.requireDrained();
        Owner owner = owners.get(authority.queryId());
        if (owner == null) {
            return report(authority.queryId(), QueryLifecycleReport.Operation.CLEANUP, List.of(), Map.of());
        }
        owner.claimCleanup(authority);
        try {
            var outcome = authority.cleanupMetadata();
            if (outcome.failures().isEmpty()) owners.remove(authority.queryId(), owner);
            Map<String, Exception> failed = new LinkedHashMap<>(outcome.failures());
            return report(authority.queryId(), QueryLifecycleReport.Operation.CLEANUP,
                    outcome.releasedCatalogs(), failed);
        } finally {
            owner.finishCleanup();
        }
    }

    ConnectorQueryAuthority retainedAuthority(String queryId) {
        Owner owner = owners.get(queryId);
        return owner == null ? null : owner.authority;
    }

    int size() { return owners.size(); }

    private static QueryLifecycleReport report(String id, QueryLifecycleReport.Operation operation,
            List<String> succeeded, Map<String, Exception> failed) {
        return new QueryLifecycleReport(id, operation, succeeded, failed);
    }

    private static final class Owner {
        private final ConnectorQueryAuthority authority;
        private boolean beginning = true;
        private boolean beganSuccessfully;
        private boolean cleaning;

        Owner(ConnectorQueryAuthority authority) { this.authority = authority; }

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
