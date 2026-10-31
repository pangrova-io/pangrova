package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;

/** Newly released catalog resources and retained failures from one actual query cleanup attempt. */
@Experimental
public record ConnectorQueryCleanupResult(List<String> releasedCatalogs, Map<String, RuntimeException> failures) {
    public ConnectorQueryCleanupResult {
        releasedCatalogs = List.copyOf(releasedCatalogs);
        var retained = new LinkedHashMap<String, RuntimeException>();
        failures.forEach((catalog, failure) -> retained.put(
                Objects.requireNonNull(catalog, "catalog"), Objects.requireNonNull(failure, "failure")));
        failures = Collections.unmodifiableMap(retained);
    }

    /** Propagates failed cleanup without claiming that its retained resources exited. */
    public void requireSuccess() {
        IllegalStateException aggregate = null;
        for (var failure : failures.values()) {
            if (aggregate == null) {
                aggregate = new IllegalStateException("Connector query cleanup failed: " + failures.keySet(), failure);
            } else {
                aggregate.addSuppressed(failure);
            }
        }
        if (aggregate != null) throw aggregate;
    }
}
