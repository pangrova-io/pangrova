package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.pangrova.connector.paimon.fs.PaimonColumnValueReader;
import org.pangrova.spi.connector.index.ColumnValueReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Resolves a physical data file's owning table without retaining a stale schema. */
final class PaimonCatalogColumnValueReader implements ColumnValueReader {

    private final Catalog catalog;

    PaimonCatalogColumnValueReader(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
    }

    @Override
    public Iterator<List<Comparable<?>>> readColumns(
            String dataFilePath, List<String> columnNames) throws IOException {
        try {
            Path file = new Path(dataFilePath);
            for (String database : catalog.listDatabases()) {
                for (String tableName : catalog.listTables(database)) {
                    Table table = PaimonTableIdentity.loadFresh(catalog, Identifier.create(database, tableName));
                    if (table instanceof FileStoreTable fileStoreTable
                            && belongsTo(file, fileStoreTable.location())) {
                        return new PaimonColumnValueReader(table)
                                .readColumns(dataFilePath, columnNames);
                    }
                }
            }
        } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException failure) {
            throw new IOException("Cannot resolve Paimon table for " + dataFilePath, failure);
        }
        throw new IOException("Cannot resolve Paimon table for " + dataFilePath);
    }

    private static boolean belongsTo(Path file, Path table) {
        for (Path parent = file.getParent(); parent != null; parent = parent.getParent()) {
            if (parent.equals(table)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        // Catalog owns table and filesystem lifetimes.
    }
}
