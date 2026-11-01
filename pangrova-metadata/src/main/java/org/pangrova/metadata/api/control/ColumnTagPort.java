package org.pangrova.metadata.api.control;

import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.session.MetadataSessionContext;

import java.util.Map;
import java.util.UUID;

/** Column governance tag use cases bound to stable table identity. */
public interface ColumnTagPort {

    void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                String columnName, String tagKey, String value);

    void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                  String columnName, String tagKey);

    Map<String, String> listTags(MetadataSessionContext session,
                                 QualifiedTableRef tableRef, String columnName);

    int clearTableTags(MetadataSessionContext session, UUID tableId);

    int renameTableTags(MetadataSessionContext session, QualifiedTableRef from,
                        QualifiedTableRef to);
}
