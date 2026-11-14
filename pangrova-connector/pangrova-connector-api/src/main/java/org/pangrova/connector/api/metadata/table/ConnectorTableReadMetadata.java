package org.pangrova.connector.api.metadata.table;

import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.exception.ConnectorException;

import java.util.Objects;

/** Read metadata distinguishes a proven declaration (including absence) from unavailable historical evidence. */
@Experimental
public record ConnectorTableReadMetadata(ConnectorTableMetadata metadata, TableDeclarationEvidence declarationEvidence) {

    public ConnectorTableReadMetadata {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(declarationEvidence, "declarationEvidence");
    }

    /** Historical declaration consumers cannot interpret missing evidence as an ordinary empty declaration. */
    public ConnectorTableMetadata requireExactDeclaration() {
        if (declarationEvidence != TableDeclarationEvidence.EXACT) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Historical generated declaration evidence is unavailable for " + metadata.table());
        }
        return metadata;
    }
}
