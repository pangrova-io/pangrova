package org.pangrova.coordinator.columnar;

import org.pangrova.spi.execution.SessionContext;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable session context for queries submitted via the columnar protocol.
 * Carries the authenticated user identity from the gRPC peer credentials.
 */
public record ColumnarSessionContext(String user) implements SessionContext {

    public ColumnarSessionContext {
        Objects.requireNonNull(user, "user");
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getSchema() {
        return Optional.empty();
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }
}
