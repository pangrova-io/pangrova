package org.pangrova.connector.paimon.handle;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.connector.base.ConnectorException;

/**
 * Stable physical identity of a Paimon table.
 *
 * <p>The SDK may synthesize {@code uuid()} from the table name and earliest schema time, so
 * the identity combines the UUID, the location, and a schema fingerprint. Comparing it before a
 * write or replay fences a same-name table that was dropped and recreated.
 */
public final class PaimonTableIdentity {

    private PaimonTableIdentity() {}

    /** Fingerprints the schema fields, primary keys and partition keys. */
    public static String schemaFingerprint(TableSchema schema) {
        StringBuilder canonical = new StringBuilder("paimon-schema:").append(schema.id());
        for (DataField field : schema.fields()) {
            canonical.append('|').append(field.id()).append(':').append(field.name())
                    .append(':').append(field.type().asSQLString());
        }
        for (String primaryKey : schema.primaryKeys()) {
            canonical.append("|pk:").append(primaryKey);
        }
        for (String partition : schema.partitionKeys()) {
            canonical.append("|p:").append(partition);
        }
        return DigestUtils.sha256Hex(canonical.toString());
    }

    /**
     * Requires the freshly loaded table to be the exact table captured by the write handle.
     *
     * @param table             freshly loaded SDK table
     * @param tableUuid         identity captured when the write was planned
     * @param tableLocation     location captured when the write was planned
     * @param schemaFingerprint schema captured when the write was planned
     * @param operation         operation name used in the failure message
     * @return the fresh table as a {@link FileStoreTable}
     * @throws ConnectorException if the table was replaced, moved, or had its schema changed
     */
    public static FileStoreTable requireSame(Table table, String tableUuid, String tableLocation,
            String schemaFingerprint, String operation) {
        if (!(table instanceof FileStoreTable fileStore)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    operation + " requires a physical Paimon FileStoreTable");
        }
        if (!fileStore.uuid().equals(tableUuid)
                || !fileStore.location().toString().equals(tableLocation)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    operation + " target table identity changed since the write was planned: "
                            + fileStore.location());
        }
        if (!schemaFingerprint(fileStore.schema()).equals(schemaFingerprint)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    operation + " target schema changed since the write was planned: "
                            + fileStore.location());
        }
        return fileStore;
    }
}
