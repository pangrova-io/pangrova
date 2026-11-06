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
package org.pangrova.connector.iceberg.read;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.api.error.ConnectorException;
import org.pangrova.connector.runtime.codec.ConnectorBinaryPageCodec;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.api.data.PageOutputFormat;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.NativePage;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Iceberg Parquet/ORC data files and produces Pages of rows.
 * Wraps Iceberg's CloseableIterable&lt;Record&gt; from the generic reader API.
 *
 * <p>Preferred output is columnar {@link NativePage} via {@link #getNextNativePage()}.
 * Arrow-free serialization uses {@link ConnectorBinaryPageCodec} (CUSTOM_BINARY format)
 * so this connector has no Arrow dependency.</p>
 *
 * <p>The row-based {@link #getNextPage()} is retained for coordinator-local
 * DML operations that do not route through the native engine.</p>
 */
public class IcebergPageSource implements ConnectorPageSource {

    private static final int BATCH_SIZE = 1024;

    private final CloseableIterable<Record> records;
    private final List<IcebergColumnHandle> projectedColumns;
    private CloseableIterator<Record> iterator;
    private boolean finished;

    public IcebergPageSource(
            CloseableIterable<Record> records,
            List<IcebergColumnHandle> projectedColumns) {
        this.records = records;
        this.projectedColumns = List.copyOf(projectedColumns);
        this.finished = false;
    }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }
        List<TypedRow> rows = readBatchSafe();
        if (rows.isEmpty()) {
            finished = true;
            return null;
        }
        List<String> columnNames = projectedColumns.stream()
                .map(IcebergColumnHandle::name)
                .toList();
        return new Page(columnNames, rows);
    }

    @Override
    public PageOutputFormat preferredFormat() {
        return PageOutputFormat.NATIVE;
    }

    @Override
    public NativePage getNextNativePage() {
        if (finished) {
            return null;
        }
        List<TypedRow> rows = readBatchSafe();
        if (rows.isEmpty()) {
            finished = true;
            return null;
        }
        List<String> columnNames = projectedColumns.stream()
                .map(IcebergColumnHandle::name)
                .toList();
        return ConnectorBinaryPageCodec.fromPage(new Page(columnNames, rows));
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void close() {
        finished = true;
        ResourceCloser.closeQuietly(iterator);
        ResourceCloser.closeQuietly(records);
    }

    private List<TypedRow> readBatchSafe() {
        try {
            return readBatch();
        } catch (ConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read Iceberg page: " + e.getMessage(), e);
        }
    }

    private List<TypedRow> readBatch() {
        CloseableIterator<Record> iter = ensureIterator();
        List<TypedRow> rows = new ArrayList<>();
        while (rows.size() < BATCH_SIZE && iter.hasNext()) {
            rows.add(extractRow(iter.next()));
        }
        if (!iter.hasNext()) {
            finished = true;
        }
        return rows;
    }

    /**
     * Lazily initializes the iterator on first use, avoiding eager file handle
     * acquisition in the constructor (BUG-IC01).
     */
    private CloseableIterator<Record> ensureIterator() {
        if (iterator == null) {
            iterator = records.iterator();
        }
        return iterator;
    }

    private TypedRow extractRow(Record record) {
        TypedRow.Builder builder = TypedRow.builder();
        for (int i = 0; i < projectedColumns.size(); i++) {
            builder.putRaw(projectedColumns.get(i).name(), record.get(i));
        }
        return builder.build();
    }

}
