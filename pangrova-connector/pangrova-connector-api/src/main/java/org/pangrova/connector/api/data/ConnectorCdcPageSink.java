package org.pangrova.connector.api.data;

import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.types.value.TypedRow;

/** A connector sink that preserves CDC row-kind semantics in one commit fragment. */
public interface ConnectorCdcPageSink extends ConnectorPageSink {

    void appendRow(TypedRow row, CdcRowKind kind);
}
