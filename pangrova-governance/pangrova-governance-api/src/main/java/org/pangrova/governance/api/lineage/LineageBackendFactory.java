package org.pangrova.governance.api.lineage;

import org.pangrova.governance.api.enums.LineageDeliveryMode;
import org.pangrova.common.api.annotation.Experimental;

/** Creates one transport owned by a Coordinator lineage emitter. Factory discovery acquires no resources. */
@Experimental
public interface LineageBackendFactory {
    int API_VERSION = 1;

    String providerId();

    int apiVersion();

    /** Outbox-backed delivery must persist facts before acknowledging the producer. */
    LineageDeliveryMode delivery();

    /** Returns a new backend. If initialization fails, the factory releases resources it acquired. */
    LineageBackend create(String namespace);

}
