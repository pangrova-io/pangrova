/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.connector.iceberg.fs;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads column values from Iceberg data files using catalog-backed schema resolution.
 *
 * <p>This reader lazily resolves the schema by matching the
 * data file path against table locations in the catalog. The resolved schema
 * is cached for subsequent reads within the same table.</p>
 */
public final class IcebergCatalogColumnValueReader implements ColumnValueReader {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogColumnValueReader.class);

    private final Catalog catalog;
    private final FileIO fileIO;
    private volatile LocatedSchema cached;

    private record LocatedSchema(String filePrefix, Schema schema) {}

    /**
     * Creates a reader that lazily resolves table schemas from the given catalog.
     */
    public IcebergCatalogColumnValueReader(Catalog catalog, FileIO fileIO) {
        if (catalog == null) throw new IllegalArgumentException("catalog must not be null");
        if (fileIO == null) throw new IllegalArgumentException("fileIO must not be null");
        this.catalog = catalog;
        this.fileIO = fileIO;
    }

    @Override
    public Iterator<List<Comparable<?>>> readColumns(String dataFilePath, List<String> columnNames)
            throws IOException {
        Schema tableSchema = resolveSchema(dataFilePath);
        Schema projectedSchema = tableSchema.select(columnNames);
        InputFile inputFile = fileIO.newInputFile(dataFilePath);

        CloseableIterable<Record> records = Parquet.read(inputFile)
                .project(projectedSchema)
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(projectedSchema, fileSchema))
                .build();

        return createRecordIterator(records, columnNames);
    }

    private Schema resolveSchema(String dataFilePath) {
        LocatedSchema current = cached;
        if (current != null && dataFilePath.startsWith(current.filePrefix())) {
            return current.schema();
        }
        return findAndCacheSchema(dataFilePath);
    }

    private Schema findAndCacheSchema(String dataFilePath) {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            for (Namespace ns : nsCatalog.listNamespaces()) {
                for (TableIdentifier tid : catalog.listTables(ns)) {
                    Table table;
                    try {
                        table = catalog.loadTable(tid);
                    } catch (NoSuchTableException missing) {
                        // A namespace listing can still contain a concurrently dropped table.
                        LOG.debug("event=index_column_reader_table_vanished table={}", tid, missing);
                        continue;
                    }
                    String location = table.location();
                    String prefix = location.endsWith("/") ? location : location + "/";
                    if (dataFilePath.startsWith(prefix)) {
                        LocatedSchema resolved = new LocatedSchema(prefix, table.schema());
                        cached = resolved;
                        return resolved.schema();
                    }
                }
            }
        }
        throw new IllegalStateException(
                "Cannot resolve table schema for data file: " + dataFilePath);
    }

    private Iterator<List<Comparable<?>>> createRecordIterator(
            CloseableIterable<Record> records, List<String> columnNames) {
        Iterator<Record> recordIterator = records.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                boolean has = recordIterator.hasNext();
                if (!has) {
                    ResourceCloser.closeQuietly(records);
                }
                return has;
            }

            @Override
            public List<Comparable<?>> next() {
                if (!recordIterator.hasNext()) {
                    ResourceCloser.closeQuietly(records);
                    throw new NoSuchElementException();
                }
                return extractRow(recordIterator.next(), columnNames);
            }
        };
    }

    private static List<Comparable<?>> extractRow(Record record, List<String> columnNames) {
        List<Comparable<?>> row = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            Object value = record.get(i);
            row.add(value instanceof Comparable<?> c ? c : null);
        }
        return row;
    }

    @Override
    public void close() {
        // The connector owns the shared FileIO.
    }
}
