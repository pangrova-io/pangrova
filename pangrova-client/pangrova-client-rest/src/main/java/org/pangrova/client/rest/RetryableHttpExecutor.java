package org.pangrova.client.rest;

import org.pangrova.common.concurrent.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Executes HTTP requests with retry, exponential backoff, and endpoint failover.
 * Package-private helper extracted from {@link PangrovaRestClient}.
 */
final class RetryableHttpExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(RetryableHttpExecutor.class);

    private final RestHttpClientOwner httpOwner;
    private final RetryPolicy retryPolicy;
    private final EndpointRouting routing;
    private final Duration requestTimeout;

    RetryableHttpExecutor(RestHttpClientOwner httpOwner, RetryPolicy retryPolicy,
                          EndpointRouting routing, Duration requestTimeout) {
        this.httpOwner = httpOwner;
        this.retryPolicy = retryPolicy;
        this.routing = routing;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Executes an HTTP request with retry and endpoint failover.
     * On retryable failures, applies exponential backoff per {@link RetryPolicy}
     * and rotates to the next endpoint if failover is enabled.
     * After exhausting retries, returns the last response for normal error handling
     * (connection failures are thrown directly).
     */
    HttpResponse<String> executeWithRetry(EndpointRouting.SelectedRequest selectedRequest) {
        routing.validate(selectedRequest);
        HttpRequest request = selectedRequest.request();
        PangrovaConnectionException lastConnectionException = null;
        HttpResponse<String> lastRetryableResponse = null;
        long maxAttempts = (long) retryPolicy.maxRetries() + 1;
        EndpointRouting.Selection selection = selectedRequest.selection();

        for (long attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                Duration delay = retryPolicy.delayForAttemptWithJitter(Math.toIntExact(attempt - 1));
                sleepForRetry(delay);
                request = rebuildRequestForEndpoint(request, selection);
            }
            try {
                HttpResponse<String> response = executeOnce(request);
                int statusCode = response.statusCode();
                if (HttpStatusCodes.isRetryable(statusCode)) {
                    lastRetryableResponse = response;
                    lastConnectionException = null;
                    selection = routing.failed(selection);
                    LOG.warn("event=retryable_server_error status={} attempt={} maxAttempts={}",
                            statusCode, attempt + 1, maxAttempts);
                    continue;
                }
                return response;
            } catch (PangrovaConnectionException e) {
                lastConnectionException = e;
                lastRetryableResponse = null;
                selection = routing.failed(selection);
                LOG.warn("event=retryable_connection_error errorCode={} attempt={} maxAttempts={}",
                        e.errorCode(), attempt + 1, maxAttempts);
            }
        }
        if (lastRetryableResponse != null) {
            return lastRetryableResponse;
        }
        throw lastConnectionException;
    }

    /**
     * Executes a single HTTP request without retry, mapping I/O failures
     * to {@link PangrovaConnectionException}.
     */
    HttpResponse<String> execute(EndpointRouting.SelectedRequest selectedRequest) {
        routing.validate(selectedRequest);
        return executeOnce(selectedRequest.request());
    }

    private HttpResponse<String> executeOnce(HttpRequest request) {
        try (var operation = httpOwner.enter()) {
            return httpOwner.client().send(request, HttpResponse.BodyHandlers.ofString());
        } catch (java.net.http.HttpConnectTimeoutException e) {
            throw new PangrovaConnectionException("CONNECTION_TIMEOUT",
                    "Connection timeout to " + request.uri().getHost() + ":" + request.uri().getPort(), e);
        } catch (java.net.http.HttpTimeoutException e) {
            throw new PangrovaConnectionException("REQUEST_TIMEOUT",
                    "Request timeout to " + request.uri().getHost() + ":" + request.uri().getPort(), e);
        } catch (java.net.ConnectException e) {
            throw new PangrovaConnectionException("CONNECTION_REFUSED",
                    "Connection refused by " + request.uri().getHost() + ":" + request.uri().getPort(), e);
        } catch (java.net.UnknownHostException e) {
            throw new PangrovaConnectionException("DNS_RESOLUTION_FAILED",
                    "Cannot resolve host " + request.uri().getHost(), e);
        } catch (IOException e) {
            throw new PangrovaConnectionException("CONNECTION_FAILED",
                    "Connection to " + request.uri().getHost() + ":" + request.uri().getPort() + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaConnectionException("CONNECTION_INTERRUPTED",
                    "Request interrupted to " + request.uri().getHost() + ":" + request.uri().getPort(), e);
        }
    }

    private HttpRequest rebuildRequestForEndpoint(HttpRequest original, EndpointRouting.Selection selection) {
        String newBaseUrl = selection.baseUrl();
        URI originalUri = original.uri();
        String path = originalUri.getRawPath();
        String query = originalUri.getRawQuery();
        String newUriStr = newBaseUrl + path + (query != null ? "?" + query : "");

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(newUriStr))
                .timeout(original.timeout().orElse(requestTimeout));
        original.headers().map().forEach((name, values) ->
                values.forEach(value -> builder.header(name, value)));
        builder.method(original.method(),
                original.bodyPublisher().orElse(HttpRequest.BodyPublishers.noBody()));
        return builder.build();
    }

    private void sleepForRetry(Duration delay) {
        ThreadUtils.sleepQuietly(delay);
        if (Thread.currentThread().isInterrupted()) {
            throw new PangrovaConnectionException(
                    "RETRY_INTERRUPTED", "Retry sleep interrupted", null);
        }
    }
}
