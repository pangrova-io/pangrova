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

import java.time.LocalDateTime;
import java.util.List;

/**
 * Scalar SQL function: {@code FILE_FROM_URL(url) -> FILE}.
 *
 * <p>Constructs a FILE struct (Claim-Check reference) from a storage URL.
 * The URL must point to an existing object in object storage (s3://, oss://, gs://, etc.).
 * The function does not copy or validate the content — it constructs metadata only.
 *
 * <p>Example:
 * <pre>
 *   INSERT INTO docs (id, img) VALUES (1, FILE_FROM_URL('s3://bucket/photo.jpg'))
 * </pre>
 *
 * <p>Returns a FILE struct with:
 * <ul>
 *   <li>uri = the input URL</li>
 *   <li>mime_type = inferred from file extension</li>
 *   <li>size = 0 (deferred resolution)</li>
 *   <li>checksum = "" (deferred computation)</li>
 *   <li>created_at = current timestamp</li>
 * </ul>
 */
public final class FileFromUrlFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA = new FunctionMetadata(
            "file_from_url",
            "ROW",
            List.of("VARCHAR"),
            true,
            "Constructs a FILE reference from a storage URL");

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
        FileStructHelper.validateFileUrl(url);

        String mimeType = FileStructHelper.inferMimeType(url);

        return FileStructHelper.createFileStruct(
                url, mimeType, 0L, "", LocalDateTime.now());
    }
}
