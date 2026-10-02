package org.pangrova.coordinator.query.execution.split;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.SplitConstraint;
import org.pangrova.spi.connector.metadata.SplitProperties;
import org.pangrova.spi.security.AccessDeniedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Binds Native object-store reads to a real catalog deployment, never session-supplied credentials. */
public final class NativeReadSplitBinding {

    private static final Set<String> NATIVE_STORAGE_PROVIDERS = Set.of("paimon", "lance");

    private NativeReadSplitBinding() {}

    /** Retains connector metadata while capturing trusted Native execution bytes. */
    public static ConnectorSplit bind(ConnectorSplit split, CatalogHandle catalog, ConnectorSession session) {
        String provider = catalog.getDescriptor().provider();
        if (!NATIVE_STORAGE_PROVIDERS.contains(provider)) {
            return split;
        }
        byte[] bytes = bind(split.toSplitInfoProto(), catalog, session);
        return new BoundSplit(split, bytes);
    }

    /** Adds or verifies the catalog identity without changing a connector's physical read unit. */
    public static byte[] bind(byte[] bytes, CatalogHandle catalog, ConnectorSession session) {
        var descriptor = catalog.getDescriptor();
        if (!NATIVE_STORAGE_PROVIDERS.contains(descriptor.provider())) {
            return bytes;
        }
        requireSession(session);
        requireCatalog(catalog, session);
        try {
            DataSplit split = DataSplit.parseFrom(bytes);
            var binding = CatalogExecutionBindings.from(descriptor);
            if (bytes.length == 0 || !split.getUnknownFields().asMap().isEmpty()
                    || !split.getConnectorId().equals(descriptor.provider())
                    || (split.hasCatalog() && !split.getCatalog().equals(binding))) {
                throw new AccessDeniedException("Native split conflicts with the trusted catalog deployment");
            }
            return split.toBuilder().setCatalog(binding).build().toByteArray();
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Native read split is not canonical DataSplit", malformed);
        }
    }

    private static void requireSession(ConnectorSession session) {
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank() || session.getUser() == null || session.getUser().isBlank()) {
            throw new AccessDeniedException("Native split requires an authenticated tenant and principal");
        }
    }

    private static void requireCatalog(CatalogHandle catalog, ConnectorSession session) {
        var descriptor = catalog.getDescriptor();
        String tenant = session.getProperties().get("tenant_id");
        if (catalog.getState() != CatalogState.ACTIVE
                || !session.getCatalog().equals(java.util.Optional.of(descriptor.name()))
                || (!descriptor.allowedTenants().isEmpty() && !descriptor.allowedTenants().contains(tenant))) {
            throw new AccessDeniedException("Native split requires an active, tenant-authorized catalog");
        }
    }

    private record BoundSplit(ConnectorSplit delegate, byte[] wire) implements ConnectorSplit {
        private BoundSplit {
            wire = wire.clone();
        }

        @Override public byte[] toSplitInfoProto() { return wire.clone(); }
        @Override public Map<String, String> splitProperties() { return delegate.splitProperties(); }
        @Override public SplitProperties typedSplitProperties() { return delegate.typedSplitProperties(); }
        @Override public SplitConstraint splitConstraint() { return delegate.splitConstraint(); }
        @Override public List<String> dataFilePaths() { return delegate.dataFilePaths(); }
        @Override public byte[] wire() { return wire.clone(); }
    }
}
