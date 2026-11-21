package org.pangrova.coordinator.metadata;

import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import java.util.List;
import java.util.Optional;

/** Connector-only fixture: a projection access indicates an unexpected dependency. */
public final class UnsupportedTableProjectionRepository implements TableProjectionRepository {
    @Override public Optional<TableMetadata> loadTable(String catalog, String schema, String table) { throw unsupported(); }
    @Override public List<QualifiedTableRef> searchTables(String pattern) { throw unsupported(); }
    private UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("This connector-only fixture does not implement local table projections");
    }
}
