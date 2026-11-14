package org.pangrova.governance.lineage.openlineage;

import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageBackendFactory;

public final class OpenLineageHttpBackendFactory implements LineageBackendFactory {
    @Override
    public String providerId() { return "HTTP"; }

    @Override
    public int apiVersion() { return API_VERSION; }

    @Override
    public Delivery delivery() { return Delivery.DURABLE_OUTBOX; }

    @Override
    public LineageBackend create(String namespace) { return new OpenLineageHttpTransport(namespace); }
}
