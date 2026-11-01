package org.pangrova.metadata.api.mutation;

import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.session.MetadataSessionContext;

import java.util.List;

/** Table DDL use cases. */
public interface TableDdlPort {

    void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata);

    void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef);

    void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes);
}
