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

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.function.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.spi.function.scalar.ScalarFunction;
import org.pangrova.ai.worker.blob.GovernedBlobStore;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

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

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("file", "file_from_bytes");

    private final GovernedBlobStore blobStore;

    public FileFromBytesFunction(GovernedBlobStore blobStore) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore required");
    }

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

        byte[] content = decodeContent(contentArg);
        String mimeType = decodeMimeType(mimeArg);
        BlobReference reference = blobStore.persist(content, mimeType);

        return FileStructHelper.createFileStruct(
                reference.uri(), reference.mimeType(), reference.size(),
                reference.checksum(), LocalDateTime.now());
    }

    private static byte[] decodeContent(ColumnValue content) {
        if (content instanceof ColumnValue.BytesValue bytes) {
            return bytes.value();
        }
        throw new IllegalArgumentException(
                "FILE_FROM_BYTES: content must be VARBINARY, got: "
                        + content.getClass().getSimpleName());
    }

    private static String decodeMimeType(ColumnValue mimeType) {
        if (mimeType instanceof ColumnValue.StringValue text) {
            return text.value();
        }
        throw new IllegalArgumentException(
                "FILE_FROM_BYTES: mime_type must be VARCHAR, got: "
                        + mimeType.getClass().getSimpleName());
    }
}
