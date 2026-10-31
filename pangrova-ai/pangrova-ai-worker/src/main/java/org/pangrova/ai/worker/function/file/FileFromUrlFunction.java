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
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.function.api.scalar.ScalarFunction;
import org.pangrova.ai.worker.blob.HttpsBlobImporter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Scalar SQL function: {@code FILE_FROM_URL(url) -> FILE}.
 *
 * <p>Constructs a FILE struct (Claim-Check reference) from a storage URL.
 * The URL must use HTTPS and pass Worker host, DNS, size and MIME policy. Content
 * is imported into the governed store before a FILE reference is returned.
 *
 * <p>Example:
 * <pre>
 *   INSERT INTO docs (id, img)
 *   VALUES (1, FILE_FROM_URL('https://media.example/photo.jpg'))
 * </pre>
 *
 */
public final class FileFromUrlFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("file", "file_from_url");

    private final HttpsBlobImporter importer;

    public FileFromUrlFunction(HttpsBlobImporter importer) {
        this.importer = Objects.requireNonNull(importer, "importer required");
    }

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL requires 1 argument: url");
        }

        ColumnValue urlArg = arguments.get(0);
        if (urlArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }
        if (!(urlArg instanceof ColumnValue.StringValue sv)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL: url must be VARCHAR, got: " + urlArg.getClass().getSimpleName());
        }

        String url = sv.value();
        BlobReference reference = importer.importUrl(url);

        return FileStructHelper.createFileStruct(
                reference.uri(), reference.mimeType(), reference.size(),
                reference.checksum(), LocalDateTime.now());
    }
}
