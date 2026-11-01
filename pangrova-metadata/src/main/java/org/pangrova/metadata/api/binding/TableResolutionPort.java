package org.pangrova.metadata.api.binding;

import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.session.MetadataSessionContext;

import java.util.List;

/** Table and schema resolution use cases. */
public interface TableResolutionPort {

    List<String> listSchemas(MetadataSessionContext session, String catalogName);

    ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef);
}
