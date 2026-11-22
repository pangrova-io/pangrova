package org.pangrova.coordinator.query.execution.split;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DataSplit;
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
        if (!NATIVE_STORAGE_PROVIDERS.contains(descriptor.provider())) {
            return PASS_THROUGH;
        }
        requireSession(session);
        requireCatalog(catalog, descriptor, session);
        return new NativeReadSplitBinding(catalog, descriptor, CatalogExecutionBindings.from(descriptor));
    }

    /** Retains connector metadata while capturing trusted Native execution bytes. */
    public ConnectorSplit bind(ConnectorSplit split) {
        return binding == null ? split : new BoundSplit(split, bind(split.toSplitInfoProto()));
    }

    /** Adds or verifies the catalog identity without changing a connector's physical read unit. */
    public byte[] bind(byte[] bytes) {
        if (binding == null) {
            return bytes;
        }
        if (catalog.getState() != CatalogState.ACTIVE || catalog.getDescriptor() != descriptor) {
            throw new AccessDeniedException("Native split catalog deployment changed during enumeration");
        }
        try {
            DataSplit split = DataSplit.parseFrom(bytes);
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

    private static void requireCatalog(CatalogHandle catalog, CatalogDescriptor descriptor, ConnectorSession session) {
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
