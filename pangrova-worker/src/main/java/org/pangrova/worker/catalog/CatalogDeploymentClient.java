package org.pangrova.worker.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.util.UriComponents;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Authenticated client for exact Coordinator catalog deployment revisions. */
public final class CatalogDeploymentClient {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int HTTP_OK = 200;
    private final URI coordinatorBaseUri;
    private final String internalAuthSecret;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    /** Creates an authenticated client for the trusted Coordinator endpoint. */
    public CatalogDeploymentClient(String coordinatorUrl, String internalAuthSecret) {
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            throw new IllegalArgumentException("coordinatorUrl must not be blank");
        }
        if (internalAuthSecret == null || internalAuthSecret.isBlank()) {
            throw new IllegalArgumentException("internalAuthSecret must not be blank");
        }
        this.coordinatorBaseUri = URI.create(coordinatorUrl.endsWith("/")
                ? coordinatorUrl : coordinatorUrl + "/");
        this.internalAuthSecret = internalAuthSecret;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        this.mapper = JsonMapperFactory.create();
    }

    /** Fetches one exact immutable catalog incarnation and revision. */
    public CatalogDeploymentResponse fetch(
            String catalogName, String incarnationId, long revision) {
        URI uri = coordinatorBaseUri.resolve(
                "v1/internal/catalog-deployments/" + UriComponents.encode(catalogName)
                        + "?incarnation=" + UriComponents.encode(incarnationId)
                        + "&revision=" + revision);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header("Authorization", "Bearer " + internalAuthSecret)
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != HTTP_OK) {
                throw new IllegalStateException(
                        "Catalog deployment fetch failed with status " + response.statusCode());
            }
            return mapper.readValue(response.body(), CatalogDeploymentResponse.class);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Catalog deployment fetch interrupted", interrupted);
        } catch (IOException failure) {
            throw new IllegalStateException("Catalog deployment fetch failed", failure);
        }
    }
}
