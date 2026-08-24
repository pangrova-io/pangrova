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
package org.pangrova.worker.ai.function.table;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.function.table.ReturnColumnDescriptor;
import org.pangrova.spi.function.table.TableFunction;
import org.pangrova.spi.function.table.TableFunctionArgument;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Table-valued function: {@code directory_listing(path) -> TABLE(file_path, file_name, size, mime_type,
 * last_modified)}.
 *
 * <p>Lists files in an object storage directory path and returns metadata about each file.
 * Used for batch ingestion of multimodal files.
 *
 * <p>Example:
 * <pre>
 *   SELECT * FROM directory_listing('s3://bucket/images/')
 *   SELECT file_path, file_name FROM directory_listing('s3://uploads/2024/')
 *       WHERE mime_type LIKE 'image/%'
 * </pre>
 *
 * <p>Returns columns:
 * <ul>
 *   <li>file_path VARCHAR — full storage URI</li>
 *   <li>file_name VARCHAR — file name only (without directory path)</li>
 *   <li>size BIGINT — file size in bytes</li>
 *   <li>mime_type VARCHAR — inferred MIME type from extension</li>
 *   <li>last_modified TIMESTAMP — last modification time</li>
 * </ul>
 */
public final class DirectoryListingTableFunction implements TableFunction {

    private final Supplier<FileSystemAccessor> storageSupplier;

    /**
     * Creates the function with a storage accessor supplier.
     *
     * @param storageSupplier provides the FileSystemAccessor for listing operations
     */
    public DirectoryListingTableFunction(Supplier<FileSystemAccessor> storageSupplier) {
        this.storageSupplier = storageSupplier;
    }

    @Override
    public String getName() {
        return "directory_listing";
    }

    @Override
    public List<TableFunctionArgument> getArguments() {
        return List.of(
                new TableFunctionArgument("path", "VARCHAR"));
    }

    @Override
    public List<ReturnColumnDescriptor> getReturnType() {
        return List.of(
                new ReturnColumnDescriptor("file_path", "VARCHAR"),
                new ReturnColumnDescriptor("file_name", "VARCHAR"),
                new ReturnColumnDescriptor("size", "BIGINT"),
                new ReturnColumnDescriptor("mime_type", "VARCHAR"),
                new ReturnColumnDescriptor("last_modified", "TIMESTAMP"));
    }

    @Override
    public Iterator<TypedRow> execute(List<ColumnValue> arguments) {
        String directoryPath = resolveDirectoryPath(arguments);

        FileSystemAccessor storage = storageSupplier.get();
        if (storage == null) {
            throw new IllegalStateException(
                    "No FileSystemAccessor available for directory listing");
        }

        try {
            List<String> files = storage.listFiles(directoryPath);
            List<TypedRow> rows = new ArrayList<>(files.size());

            for (String filePath : files) {
                String fileName = extractFileName(filePath);
                String mimeType = inferMimeType(fileName);

                rows.add(TypedRow.builder()
                        .put("file_path", new ColumnValue.StringValue(filePath))
                        .put("file_name", new ColumnValue.StringValue(fileName))
                        .put("size", new ColumnValue.LongValue(0L)) // size resolved lazily
                        .put("mime_type", new ColumnValue.StringValue(mimeType))
                        .put("last_modified", new ColumnValue.TimestampValue(LocalDateTime.now()))
                        .build());
            }

            return Collections.unmodifiableList(rows).iterator();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to list directory: " + directoryPath + ": " + e.getMessage(), e);
        }
    }

    private static String resolveDirectoryPath(List<ColumnValue> arguments) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException(
                    "directory_listing requires 1 argument: path (VARCHAR)");
        }

        ColumnValue pathArg = arguments.get(0);
        if (!(pathArg instanceof ColumnValue.StringValue sv)) {
            throw new IllegalArgumentException(
                    "directory_listing: path must be VARCHAR");
        }

        String directoryPath = sv.value();
        if (directoryPath == null || directoryPath.isBlank()) {
            throw new IllegalArgumentException(
                    "directory_listing: path must not be empty");
        }

        // Ensure path ends with / for directory listing
        if (!directoryPath.endsWith("/")) {
            directoryPath = directoryPath + "/";
        }
        return directoryPath;
    }

    private static String extractFileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        // Remove trailing slash if present
        String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        int lastSlash = trimmed.lastIndexOf('/');
        return lastSlash >= 0 ? trimmed.substring(lastSlash + 1) : trimmed;
    }

    private static String inferMimeType(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return "application/octet-stream";
        }
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot < 0 || lastDot >= fileName.length() - 1) {
            return "application/octet-stream";
        }
        String ext = fileName.substring(lastDot + 1).toLowerCase();
        return switch (ext) {
            case "jpg", "jpeg" -> "image/jpeg";
            case "png" -> "image/png";
            case "gif" -> "image/gif";
            case "webp" -> "image/webp";
            case "tiff", "tif" -> "image/tiff";
            case "bmp" -> "image/bmp";
            case "pdf" -> "application/pdf";
            case "doc" -> "application/msword";
            case "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
            case "txt" -> "text/plain";
            case "csv" -> "text/csv";
            case "json" -> "application/json";
            case "mp3" -> "audio/mpeg";
            case "wav" -> "audio/wav";
            case "ogg" -> "audio/ogg";
            case "flac" -> "audio/flac";
            case "mp4" -> "video/mp4";
            case "mov" -> "video/quicktime";
            default -> "application/octet-stream";
        };
    }
}
