package org.pangrova.connector.api.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.types.value.TypedRow;

/** A connector sink that preserves CDC row-kind semantics in one commit fragment. */
@Experimental
public interface ConnectorCdcPageSink extends ConnectorPageSink {

    void appendRow(TypedRow row, CdcRowKind kind);
}
