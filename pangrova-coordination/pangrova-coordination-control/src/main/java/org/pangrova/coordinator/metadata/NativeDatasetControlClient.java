package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.exception.NativeDatasetControlRpcException;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;

import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded, authenticated binary metadata RPC; never carries query rows or loads JNI. */
@ThreadSafe
public final class NativeDatasetControlClient implements AutoCloseable {

    private static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;
    // The bound applies to the entire binary RuntimeCommandResponse/Any envelope,
    // not the page proof alone. The canonical page is at most 8 MiB within this 16 MiB envelope.
    static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final int HTTP_THREADS = 4;
    private static final int HTTP_QUEUE_SIZE = 128;
    private static final int CONNECT_TIMEOUT_SECONDS = 5;
    private final ExecutorService executor = ManagedExecutorFactory.newBoundedThreadPool(
            "native-dataset-control-http", HTTP_THREADS, HTTP_QUEUE_SIZE);
    private final HttpClient client = HttpClient.newBuilder().executor(executor)
            .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECONDS))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final Set<CompletableFuture<?>> requests = new HashSet<>();
    private final String secret;
    private boolean closed;

    /** The internal credential is local configuration, never serialized into catalog metadata. */
    public NativeDatasetControlClient(String secret) {
        this.secret = secret;
    }

    /** One attempt only; uncertainty is reconciled using the same durable publication intent. */
    public RuntimeCommandResponse call(URI endpoint, RuntimeCommand command, long deadline) {
        HttpRequest request = request(endpoint, command, deadline);
        long remaining = deadline - System.currentTimeMillis();
        var future = send(request);
        try {
            var response = future.get(remaining, TimeUnit.MILLISECONDS);
            return decodeResponse(response.statusCode(), response.body());
        } catch (InterruptedException interrupted) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new NativeDatasetControlRpcException("Native metadata RPC interrupted", interrupted);
        } catch (ExecutionException | TimeoutException | CancellationException failure) {
            future.cancel(true);
            throw new NativeDatasetControlRpcException("Native metadata RPC outcome is unknown", failure);
        } finally {
            completed(future);
        }
    }

    private HttpRequest request(URI endpoint, RuntimeCommand command, long deadline) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException("Native metadata RPC requires internal authentication");
        }
        byte[] bytes = command.toByteArray();
        if (bytes.length > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException("Metadata control request exceeds hard limit");
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            throw new IllegalArgumentException("Metadata control deadline expired");
        }
        return HttpRequest.newBuilder(endpoint).timeout(Duration.ofMillis(remaining))
                .header("X-Internal-Auth", secret).header("Content-Type", "application/octet-stream")
                .header("Accept", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
    }

    private CompletableFuture<HttpResponse<byte[]>> send(HttpRequest request) {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Native metadata RPC client is closed");
            }
        }
        CompletableFuture<HttpResponse<byte[]>> future;
        try {
            future = client.sendAsync(request, ignored -> new BoundedResponse());
        } catch (RejectedExecutionException failure) {
            throw new NativeDatasetControlRpcException("Native metadata RPC handoff was interrupted", failure);
        }
        boolean cancel;
        synchronized (this) {
            cancel = closed;
            if (!cancel) {
                requests.add(future);
            }
        }
        if (cancel) {
            future.cancel(true);
        }
        return future;
    }

    private synchronized void completed(CompletableFuture<?> request) {
        requests.remove(request);
    }

    static RuntimeCommandResponse decodeResponse(int status, byte[] bytes) {
        if (status == HttpURLConnection.HTTP_UNAUTHORIZED || status == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new SecurityException("Native metadata RPC authentication denied: HTTP " + status);
        }
        RuntimeCommandResponse result;
        try {
            result = RuntimeCommandResponse.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            if (status >= HttpURLConnection.HTTP_MULT_CHOICE && status < HttpURLConnection.HTTP_INTERNAL_ERROR) {
                throw new IllegalStateException("Native metadata RPC rejected: HTTP " + status, malformed);
            }
            throw new NativeDatasetControlRpcException("Native metadata RPC response is malformed", malformed);
        }
        if (status != HttpURLConnection.HTTP_OK || !result.getSuccess()) {
            rejectResponse(status, result);
        }
        return result;
    }

    private static void rejectResponse(int status, RuntimeCommandResponse result) {
        String message = "Native metadata RPC failed: HTTP " + status
                + " code=" + result.getError().getCode() + " message=" + result.getError().getMessage();
        switch (result.getError().getErrorClass()) {
            case ERROR_CLASS_POLICY_DENIED -> throw new SecurityException(message);
            case ERROR_CLASS_INVALID_REQUEST, ERROR_CLASS_PROTOCOL_MISMATCH,
                    ERROR_CLASS_CAPABILITY_MISMATCH, ERROR_CLASS_ADMISSION ->
                    throw new IllegalStateException(message);
            default -> throw new NativeDatasetControlRpcException(
                    "Native publication cannot be inferred from the failed response",
                    new IllegalStateException(message));
        }
    }

    @Override
    public void close() {
        List<CompletableFuture<?>> pending;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            pending = List.copyOf(requests);
            requests.clear();
        }
        pending.forEach(request -> request.cancel(true));
        executor.shutdownNow();
    }

    /** Caps memory before copying each received buffer; transport cancellation terminates the body. */
    static final class BoundedResponse implements HttpResponse.BodySubscriber<byte[]> {

        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private Flow.Subscription subscription;

        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            value.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > MAX_RESPONSE_BYTES - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(new IllegalArgumentException("Metadata response exceeds hard limit"));
                    return;
                }
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                bytes.writeBytes(chunk);
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            result.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            result.complete(bytes.toByteArray());
        }
    }
}
