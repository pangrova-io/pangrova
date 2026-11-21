package org.pangrova.governance.lineage.log;

import org.pangrova.governance.api.enums.LineageDeliveryMode;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageBackendFactory;

public final class LogLineageBackendFactory implements LineageBackendFactory {
    @Override
    public String providerId() { return "LOG"; }

    @Override
    public int apiVersion() { return API_VERSION; }

    @Override
    public LineageDeliveryMode delivery() { return LineageDeliveryMode.TRANSIENT; }

    @Override
    public LineageBackend create(String namespace) { return new LogLineageBackend(); }
}
