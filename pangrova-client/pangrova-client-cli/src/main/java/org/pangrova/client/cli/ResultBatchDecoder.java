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
package org.pangrova.client.cli;

import org.pangrova.transport.ArrowBatchDecoder;
import org.pangrova.transport.ResultBatch;
import org.pangrova.transport.TransportException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Decodes Arrow IPC stream bytes into column names and row data for terminal display.
 *
 * <p>Delegates Arrow decoding to {@link ArrowBatchDecoder} (in {@code pangrova-transport})
 * and formats each value as a display string. Null values render as "NULL".
 * Values exceeding {@link #MAX_CELL_WIDTH} are truncated with "..." suffix.</p>
 */
final class ResultBatchDecoder {

    static final int MAX_CELL_WIDTH = 64;

    private ResultBatchDecoder() {}

    /**
     * Decodes Arrow IPC bytes into a {@link DecodedResult}.
     *
     * @param batchData Arrow IPC stream format byte array
     * @return decoded column names and row data
     */
    static DecodedResult decode(byte[] batchData) {
        if (batchData == null || batchData.length == 0) {
            return DecodedResult.EMPTY;
        }
        ResultBatch batch;
        try {
            batch = ArrowBatchDecoder.decode(batchData);
        } catch (TransportException e) {
            throw new ResultDecodeException("Failed to decode Arrow IPC response: " + e.getMessage(), e);
        }
        if (batch.columns().isEmpty()) {
            return DecodedResult.EMPTY;
        }
        List<String> columnNames = batch.columnNames();
        List<Object[]> decodedRows = batch.rows();
        List<List<String>> rows = new ArrayList<>(decodedRows.size());
        for (Object[] row : decodedRows) {
            List<String> formatted = new ArrayList<>(row.length);
            for (Object value : row) {
                formatted.add(formatCell(value));
            }
            rows.add(Collections.unmodifiableList(formatted));
        }
        return new DecodedResult(
                Collections.unmodifiableList(columnNames),
                Collections.unmodifiableList(rows));
    }

    private static String formatCell(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof float[] vector) {
            return truncate(Arrays.toString(vector));
        }
        return truncate(value.toString());
    }

    private static String truncate(String value) {
        if (value.length() <= MAX_CELL_WIDTH) {
            return value;
        }
        return value.substring(0, MAX_CELL_WIDTH - 3) + "...";
    }

    /**
     * Immutable result of Arrow IPC decoding.
     */
    static final class DecodedResult {
        static final DecodedResult EMPTY = new DecodedResult(List.of(), List.of());

        private final List<String> columnNames;
        private final List<List<String>> rows;

        DecodedResult(List<String> columnNames, List<List<String>> rows) {
            this.columnNames = columnNames;
            this.rows = rows;
        }

        List<String> columnNames() {
            return columnNames;
        }

        List<List<String>> rows() {
            return rows;
        }

        boolean isEmpty() {
            return rows.isEmpty();
        }
    }
}
