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
package org.pangrova.ai.worker.function.file;

import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.types.value.ColumnValue;

import java.time.LocalDateTime;
import java.util.List;

/** Maps governed blob references to and from the SQL FILE row (uri, MIME, size, checksum, timestamp). */
public final class FileStruct {
    private FileStruct() {}

    public static ColumnValue.RowValue encode(BlobReference reference, LocalDateTime createdAt) {
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt required");
        }
        return new ColumnValue.RowValue(List.of(
                reference.uri(),
                reference.mimeType(),
                reference.size(),
                reference.checksum(),
                createdAt
        ));
    }

    /**
     * Extracts a {@link BlobReference} from a FILE column value.
     *
     * @param fileValue the ColumnValue (expected to be RowValue representing FILE struct)
     * @return BlobReference, or null if the value is null
     * @throws IllegalArgumentException if the value is not a valid FILE struct
     */
    public static BlobReference decodeReference(ColumnValue fileValue) {
        if (fileValue instanceof ColumnValue.NullValue) {
            return null;
        }
        if (!(fileValue instanceof ColumnValue.RowValue rv)) {
            throw new IllegalArgumentException(
                    "Expected FILE (ROW) value, got: " + fileValue.getClass().getSimpleName());
        }
        List<?> fields = rv.fields();
        if (fields.size() < 4) {
            throw new IllegalArgumentException(
                    "Invalid FILE struct: expected at least 4 fields, got " + fields.size());
        }

        String uri = toString(fields.get(0), "uri");
        String mimeType = toString(fields.get(1), "mime_type");
        long size = toLong(fields.get(2), "size");
        String checksum = toString(fields.get(3), "checksum");

        return new BlobReference(uri, mimeType, size, checksum);
    }

    private static String toString(Object field, String name) {
        if (field == null) {
            throw new IllegalArgumentException("Invalid FILE struct: " + name + " is null");
        }
        if (field instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException(
                "Invalid FILE struct: " + name + " must be VARCHAR");
    }

    private static long toLong(Object field, String name) {
        if (field == null) {
            throw new IllegalArgumentException("Invalid FILE struct: " + name + " is null");
        }
        if (field instanceof Byte || field instanceof Short
                || field instanceof Integer || field instanceof Long) {
            return ((Number) field).longValue();
        }
        try {
            return Long.parseLong(field.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid FILE struct: " + name + " must be BIGINT", e);
        }
    }
}
