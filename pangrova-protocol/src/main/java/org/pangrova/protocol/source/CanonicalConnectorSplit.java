package org.pangrova.protocol.source;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.protocol.v2.DataSplit;

/** Connector split that supplies the typed execution contract to the Coordinator. */
@Experimental
public interface CanonicalConnectorSplit extends ConnectorSplit {

    DataSplit toDataSplit();

    /** Rejects connector splits without an execution contract. */
    static DataSplit from(ConnectorSplit split) {
        if (!(split instanceof CanonicalConnectorSplit canonical)) {
            throw new IllegalArgumentException("Connector split must supply a canonical DataSplit");
        }
        DataSplit data = canonical.toDataSplit();
        if (data == null || data.equals(DataSplit.getDefaultInstance())) {
            throw new IllegalArgumentException("Connector supplied an empty DataSplit");
        }
        return data;
    }
}
