package org.pangrova.file.adapter.worker;

import org.pangrova.spi.file.access.FileUrlClient;
import org.pangrova.spi.file.access.FileDeletionClient;
import org.pangrova.spi.file.access.FileListingClient;
import org.pangrova.spi.file.access.FileAccessGrant;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.filesystem.api.object.FileListing;

/**
 * Performs Worker file-plane calls that the Coordinator delegates instead of imitating.
 *
 * <p>The Coordinator never loads storage credentials or media bytes. It issues a signed
 * {@link FileAccessGrant} and asks the Worker named by that grant
 * to execute the operation.</p>
 */
public final class WorkerFileClient implements FileListingClient, FileDeletionClient,
        FileUrlClient {

    private static final int HTTP_OK = 200;
    private static final int HTTP_NO_CONTENT = 204;
    private static final int DEFAULT_LIST_PAGE_SIZE = 1000;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String INTERNAL_AUTH_HEADER = "X-Internal-Auth";

    private final HttpClient httpClient;
    private final String internalAuthSecret;
    private final ObjectMapper mapper = JsonMapperFactory.create();

    public WorkerFileClient(HttpClient httpClient, String internalAuthSecret) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.internalAuthSecret = internalAuthSecret;
    }

    /** Creates a client with the standard connection timeout. */
    public static WorkerFileClient create(String internalAuthSecret) {
        return new WorkerFileClient(
                HttpClient.newBuilder().connectTimeout(TIMEOUT).build(), internalAuthSecret);
    }

    /** Lists one page under the grant's path prefix. */
    @Override
    public FileListing list(FileAccessGrant grant, String pageToken) {
        return list(grant, pageToken, DEFAULT_LIST_PAGE_SIZE);
    }

    /** Lists one page under the grant's path prefix. */
    public FileListing list(FileAccessGrant grant, String pageToken,
            int pageSize) {
        Objects.requireNonNull(grant, "grant must not be null");
        URI uri = URI.create(trimTrailingSlash(grant.workerUrl()) + "/v1/file-space/list");
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        body(new ListRequest(grant.descriptor(), grant.signature(),
                                pageToken, pageSize))));
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != HTTP_OK) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "Worker listing failed with HTTP " + response.statusCode()
                                + " at " + grant.workerUrl());
            }
            return mapper.readValue(response.body(), FileListing.class);
        } catch (IOException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker listing failed at " + grant.workerUrl() + ": "
                            + failure.getMessage(), failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker listing was interrupted", failure);
        }
    }

    /** Deletes the grant's object, optionally pinned to an observed provider version. */
    @Override
    public void delete(FileAccessGrant grant, String expectedVersion) {
        Objects.requireNonNull(grant, "grant must not be null");
        URI uri = URI.create(trimTrailingSlash(grant.workerUrl()) + "/v1/file-space/object?descriptor="
                + URLEncoder.encode(grant.descriptor(), StandardCharsets.UTF_8)
                + "&signature=" + URLEncoder.encode(grant.signature(), StandardCharsets.UTF_8));
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(TIMEOUT)
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(
                        body(new DeleteRequest(expectedVersion))));
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != HTTP_NO_CONTENT
                    && response.statusCode() != HTTP_OK) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "Worker delete failed with HTTP " + response.statusCode()
                                + " at " + grant.workerUrl());
            }
        } catch (IOException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker delete failed at " + grant.workerUrl() + ": "
                            + failure.getMessage(), failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker delete was interrupted", failure);
        }
    }

    /** Mints a provider-signed URL through the Worker that holds the storage credentials. */
    @Override
    public FileUrl presign(FileAccessGrant grant, boolean read,
            long ttlSeconds) {
        Objects.requireNonNull(grant, "grant must not be null");
        URI uri = URI.create(trimTrailingSlash(grant.workerUrl()) + "/v1/file-space/presign");
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        body(new PresignRequest(grant.descriptor(), grant.signature(), read,
                                ttlSeconds))));
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            builder.header(INTERNAL_AUTH_HEADER, internalAuthSecret);
        }
        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != HTTP_OK) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "Worker presign failed with HTTP " + response.statusCode()
                                + " at " + grant.workerUrl());
            }
            return providerUrl(response.body());
        } catch (IOException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker presign failed at " + grant.workerUrl() + ": "
                            + failure.getMessage(), failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker presign was interrupted", failure);
        }
    }

    private FileUrl providerUrl(byte[] response) {
        try {
            FileUrl result = mapper.readValue(response, FileUrl.class);
            if (result == null) throw new IllegalArgumentException("Missing Worker URL receipt");
            return result;
        } catch (IOException | IllegalArgumentException invalid) {
            // Jackson diagnostics can retain the credential-bearing response. Neither
            // its message nor its cause may cross this response-validation boundary.
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Worker presign response is invalid");
        }
    }

    private byte[] body(Object request) {
        try {
            return mapper.writeValueAsBytes(request);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Worker file request is not serializable", failure);
        }
    }

    private static String trimTrailingSlash(String endpoint) {
        String trimmed = endpoint.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    /** Wire shape; field names must match the Worker {@code ListRequest} record. */
    private record ListRequest(String descriptor, String signature, String pageToken,
            int pageSize) {
    }

    /** Wire shape; field names must match the Worker {@code DeleteRequest} record. */
    private record DeleteRequest(String expectedVersion) {
    }

    /** Wire shape; field names must match the Worker {@code PresignRequest} record. */
    private record PresignRequest(String descriptor, String signature, boolean read,
            long ttlSeconds) {
    }
}
