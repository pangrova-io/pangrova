package org.pangrova.coordinator.query.execution.split;

import org.pangrova.coordinator.metadata.CatalogExecutionBindingFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.SplitConstraint;
import org.pangrova.connector.api.metadata.SplitProperties;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Binds Native object-store reads to a real catalog deployment, never session-supplied credentials. */
public final class NativeReadSplitBinding {

    private static final Set<String> NATIVE_STORAGE_PROVIDERS = Set.of("paimon", "lance");
    private static final NativeReadSplitBinding PASS_THROUGH = new NativeReadSplitBinding(null, null, null);

    private final CatalogHandle catalog;
    private final CatalogDescriptor descriptor;
    private final CatalogExecutionBinding binding;

    private NativeReadSplitBinding(CatalogHandle catalog, CatalogDescriptor descriptor,
            CatalogExecutionBinding binding) {
        this.catalog = catalog;
        this.descriptor = descriptor;
        this.binding = binding;
    }

    /** Authorizes one scan against an immutable deployment and computes its wire identity once. */
    public static NativeReadSplitBinding prepare(CatalogHandle catalog, ConnectorSession session) {
        var descriptor = catalog.getDescriptor();
        boolean icebergS3 = "iceberg".equals(descriptor.provider())
                && descriptor.properties().getOrDefault("warehouse", "").startsWith("s3://");
        if (!NATIVE_STORAGE_PROVIDERS.contains(descriptor.provider()) && !icebergS3) {
            return PASS_THROUGH;
        }
        requireSession(session);
        requireCatalog(catalog, descriptor, session);
        return new NativeReadSplitBinding(catalog, descriptor, CatalogExecutionBindingFactory.from(descriptor));
    }

    /** Retains connector metadata while capturing the trusted Native execution split. */
    public ConnectorSplit bind(ConnectorSplit split) {
        return binding == null ? split : new BoundSplit(split, bind(CanonicalConnectorSplit.from(split)));
    }

    /** Adds or verifies the catalog identity without changing a connector's physical read unit. */
    public DataSplit bind(DataSplit split) {
        if (binding == null) {
            return split;
        }
        if (catalog.getState() != CatalogState.ACTIVE || catalog.getDescriptor() != descriptor) {
            throw new AccessDeniedException("Native split catalog deployment changed during enumeration");
        }
        if (!split.getUnknownFields().asMap().isEmpty()
                || !split.getConnectorId().equals(descriptor.provider())
                || (split.hasCatalog() && !split.getCatalog().equals(binding))) {
            throw new AccessDeniedException("Native split conflicts with the trusted catalog deployment");
        }
        return split.hasCatalog() ? split : split.toBuilder().setCatalog(binding).build();
    }

    private static void requireSession(ConnectorSession session) {
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank() || session.getUser() == null || session.getUser().isBlank()) {
            throw new AccessDeniedException("Native split requires an authenticated tenant and principal");
        }
    }

    private static void requireCatalog(CatalogHandle catalog, CatalogDescriptor descriptor, ConnectorSession session) {
        String tenant = session.getProperties().get("tenant_id");
        if (catalog.getState() != CatalogState.ACTIVE
                || !session.getCatalog().equals(java.util.Optional.of(descriptor.name()))
                || (!descriptor.allowedTenants().isEmpty() && !descriptor.allowedTenants().contains(tenant))) {
            throw new AccessDeniedException("Native split requires an active, tenant-authorized catalog");
        }
    }

    private record BoundSplit(ConnectorSplit delegate, DataSplit data) implements CanonicalConnectorSplit {
        @Override public DataSplit toDataSplit() { return data; }
        @Override public Map<String, String> splitProperties() { return delegate.splitProperties(); }
        @Override public SplitProperties typedSplitProperties() { return delegate.typedSplitProperties(); }
        @Override public SplitConstraint splitConstraint() { return delegate.splitConstraint(); }
        @Override public List<String> dataFilePaths() { return delegate.dataFilePaths(); }
        @Override public long estimatedScanBytes() { return delegate.estimatedScanBytes(); }
        @Override public long estimatedScanRows() { return delegate.estimatedScanRows(); }
    }
}
