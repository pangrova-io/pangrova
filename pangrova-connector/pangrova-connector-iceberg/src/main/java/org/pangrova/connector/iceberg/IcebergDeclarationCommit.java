package org.pangrova.connector.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.api.exception.ConnectorException;

import java.net.URI;

/** Only verified whole-metadata CAS backends may publish a declaration-fenced write. */
final class IcebergDeclarationCommit {
    private IcebergDeclarationCommit() {}

    static void requireSupported(Catalog catalog, Table table) {
        if (!(table instanceof HasTableOperations sdkTable)) {
            throw unsupported("SDK TableOperations are unavailable");
        }
        // The exact SDK JDBC catalog creates its own pointer-CAS operations;
        // accepting a caller-supplied TableOperations implementation is unsafe.
        if (catalog.getClass() == JdbcCatalog.class) return;
        if (catalog.getClass() == HadoopCatalog.class
                && sdkTable.operations().getClass() == HadoopTableOperations.class
                && local(table.location())) return;
        throw unsupported("whole-metadata CAS has not been verified for this catalog");
    }

    static Table bind(Catalog catalog, Table table, IcebergOutputTableHandle handle) {
        requireSupported(catalog, table);
        var operations = ((HasTableOperations) table).operations();
        return new BaseTable(new IcebergDeclarationTableOperations(operations, handle), table.name());
    }

    static Table bindExact(Catalog catalog, Table table, Runnable requireAuthority) {
        requireSupported(catalog, table);
        var operations = ((HasTableOperations) table).operations();
        return new BaseTable(new IcebergDeclarationTableOperations(
                operations, operations.current(), requireAuthority), table.name());
    }

    static Table bindCdcBase(Catalog catalog, Table table, IcebergOutputTableHandle handle) {
        Table exact = bindExact(catalog, table, () -> {});
        var operations = ((HasTableOperations) exact).operations();
        // Both declarations must be read at the same exact catalog pointer.
        var original = new IcebergDeclarationTableOperations(operations, handle).current();
        long snapshot = original.currentSnapshot() == null
                ? IcebergOutputTableHandle.NO_SNAPSHOT : original.currentSnapshot().snapshotId();
        if (snapshot != handle.baseSnapshotId()) {
            throw new org.apache.iceberg.exceptions.ValidationException("Iceberg CDC base snapshot changed");
        }
        return exact;
    }

    private static boolean local(String location) {
        // SDK locations may be raw absolute paths, including spaces and quotes.
        if (location.startsWith("/")) return true;
        URI uri = URI.create(location);
        return "file".equalsIgnoreCase(uri.getScheme()) && uri.getAuthority() == null
                && uri.getPath() != null && uri.getPath().startsWith("/");
    }

    private static ConnectorException unsupported(String detail) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                "Iceberg declaration-fenced write is unsupported: " + detail);
    }
}
