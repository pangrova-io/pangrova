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
package org.pangrova.worker.ai.function;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.spi.function.ScalarFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

/**
 * Scalar SQL function: {@code FILE_FROM_BYTES(content, mime_type) -> FILE}.
 *
 * <p>Constructs a FILE struct from raw bytes with an explicit MIME type.
 * The bytes are stored using a content-addressable scheme (SHA-256 as key)
 * and the resulting FILE struct references the storage URI.
 *
 * <p>Example:
 * <pre>
 *   INSERT INTO docs (id, doc) VALUES (1, FILE_FROM_BYTES(X'255044462D', 'application/pdf'))
 * </pre>
 */
public final class FileFromBytesFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA = new FunctionMetadata(
            "file_from_bytes",
            "ROW",
            List.of("VARBINARY", "VARCHAR"),
            true,
            "Constructs a FILE reference from raw bytes and MIME type");

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.size() < 2) {
            throw new IllegalArgumentException(
                    "FILE_FROM_BYTES requires 2 arguments: content (VARBINARY), mime_type (VARCHAR)");
        }

        ColumnValue contentArg = arguments.get(0);
        ColumnValue mimeArg = arguments.get(1);

        if (contentArg instanceof ColumnValue.NullValue || mimeArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }

        byte[] content;
        if (contentArg instanceof ColumnValue.BytesValue bv) {
            content = bv.value();
        } else if (contentArg instanceof ColumnValue.StringValue sv) {
            // Allow string input as raw bytes for convenience
            content = sv.value().getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException(
                    "FILE_FROM_BYTES: content must be VARBINARY, got: "
                            + contentArg.getClass().getSimpleName());
        }

        if (!(mimeArg instanceof ColumnValue.StringValue mimeStr)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_BYTES: mime_type must be VARCHAR, got: "
                            + mimeArg.getClass().getSimpleName());
        }

        String mimeType = mimeStr.value();
        String checksum = computeSha256(content);

        // Generate content-addressable URI using checksum
        String uri = "blob://inline/" + checksum;

        return FileStructHelper.createFileStruct(
                uri, mimeType, content.length, checksum, LocalDateTime.now());
    }

    private static String computeSha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
