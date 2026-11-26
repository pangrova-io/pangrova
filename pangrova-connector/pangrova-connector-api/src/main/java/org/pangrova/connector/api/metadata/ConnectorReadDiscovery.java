package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.session.ConnectorSession;

import java.util.Optional;
import java.util.function.Consumer;

/** Finds a named source and defers its read binding to the same compilation. */
public record ConnectorReadDiscovery(ConnectorMetadata metadata, ConnectorTableHandle handle, Binder binder) {

    @FunctionalInterface
    public interface Binder {
        /** A non-null identity check must run before the connector binds the selected read. */
        ConnectorReadBinding bind(ConnectorSession session, ConnectorTableHandle selector,
                Consumer<Optional<TableIdentity>> beforeSnapshotBinding);
    }
}
