package org.pangrova.coordinator.query.task;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;

/** Catalog fixture for testing writes bound to a particular deployment revision. */
final class TaskWriteCatalogDeployments implements CatalogDeploymentRepository {
    private final Map<String, CatalogDescriptor> current = new LinkedHashMap<>();
    private final Map<CatalogDeploymentIdentity, CatalogDescriptor> revisions = new LinkedHashMap<>();

    TaskWriteCatalogDeployments(List<CatalogDescriptor> descriptors) {
        descriptors.forEach(this::saveCatalogDescriptor);
    }

    @Override public synchronized List<CatalogDescriptor> listAllCatalogDescriptors() {
        return List.copyOf(current.values());
    }

    @Override public synchronized Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
        return Optional.ofNullable(current.get(name));
    }

    @Override public synchronized void saveCatalogDescriptor(CatalogDescriptor descriptor) {
        var previous = revisions.putIfAbsent(descriptor.identity(), descriptor);
        if (previous != null && !previous.equals(descriptor)) {
            throw new IllegalStateException("Conflicting catalog deployment revision: " + descriptor.identity());
        }
        current.put(descriptor.name(), descriptor);
    }

    @Override public synchronized boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) {
        var descriptor = current.get(expected.catalogName());
        return descriptor != null && descriptor.identity().equals(expected)
                && current.remove(expected.catalogName(), descriptor);
    }

    @Override public synchronized Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        return Optional.ofNullable(revisions.get(new CatalogDeploymentIdentity(name, incarnationId, revision)));
    }
}
