package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.worker.ai.context.AiExecutionContext;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.IDN;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/** Imports an allowlisted HTTPS object into the governed content-addressed FILE store. */
public final class HttpsBlobImporter implements AutoCloseable {

    private static final int MINIMUM_GZIP_BUFFER = 8 * 1024;
    private static final int HTTP_OK = 200;
    private static final int HTTP_REDIRECT_MIN = 300;
    private static final int HTTP_REDIRECT_MAX = 399;
    private static final int MAX_PORT = 65_535;

    private final GovernedBlobStore store;
    private final Set<String> allowedHosts;
    private final Set<String> allowedMimeTypes;
    private final Duration readTimeout;
    private final long maxWireBytes;
    private final long maxDecodedBytes;
    private final HostResolver hostResolver;
    private final HttpsTransport transport;
    private final AutoCloseable ownedTransport;
    private final Clock clock;

    /** Creates an importer with DNS resolution and a pinned pooled HTTPS transport. */
    public HttpsBlobImporter(
            GovernedBlobStore store,
            Settings settings) {
        this(
                store, settings,
                InetAddress::getAllByName,
                new PinnedHttpsTransport(settings), Clock.systemUTC());
    }

    HttpsBlobImporter(
            GovernedBlobStore store,
            Settings settings,
            HostResolver hostResolver,
            HttpsTransport transport) {
        this(store, settings, hostResolver, transport, Clock.systemUTC());
    }

    HttpsBlobImporter(
            GovernedBlobStore store,
            Settings settings,
            HostResolver hostResolver,
            HttpsTransport transport,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        Objects.requireNonNull(settings, "settings must not be null");
        this.allowedHosts = normalizeHosts(settings.allowedHosts());
        this.allowedMimeTypes = normalizeMimeTypes(settings.allowedMimeTypes());
        this.readTimeout = settings.readTimeout();
        this.maxWireBytes = settings.maxWireBytes();
        this.maxDecodedBytes = settings.maxDecodedBytes();
        this.hostResolver = Objects.requireNonNull(hostResolver, "hostResolver required");
        this.transport = Objects.requireNonNull(transport, "transport required");
        this.ownedTransport = transport instanceof AutoCloseable closeable
                ? closeable : null;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /** Downloads one object without redirects and returns only its governed identity. */
    public BlobReference importUrl(String value) {
        URI uri = validateUri(value);
        List<InetAddress> addresses = resolveAndRequirePublic(uri.getHost());
        try {
            DownloadResponse response = transport.fetch(
                    uri, addresses, remainingReadTimeout());
            return persistResponse(uri, response);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw downloadFailure(uri, "HTTPS FILE import was interrupted", failure);
        } catch (IOException failure) {
            throw downloadFailure(uri, "HTTPS FILE import failed", failure);
        }
    }

    private Duration remainingReadTimeout() {
        AiExecutionContext context = AiExecutionContext.current().orElseThrow(() ->
                new IllegalStateException("HTTPS FILE import requires task identity"));
        context.requireGovernedIdentity();
        long remaining = context.deadlineEpochMs() - clock.millis();
        if (remaining <= 0) {
            throw new IllegalStateException("HTTPS FILE import deadline has expired");
        }
        long configuredMillis = Math.max(1L, readTimeout.toMillis());
        return Duration.ofMillis(Math.min(configuredMillis, remaining));
    }

    private BlobReference persistResponse(URI uri, DownloadResponse response)
            throws IOException {
        try (InputStream body = response.body()) {
            if (response.statusCode() >= HTTP_REDIRECT_MIN
                    && response.statusCode() <= HTTP_REDIRECT_MAX) {
                throw downloadFailure(
                        uri, "HTTPS FILE redirects are disabled and were not followed", null);
            }
            if (response.statusCode() != HTTP_OK) {
                throw downloadFailure(
                        uri, "HTTPS FILE endpoint returned status " + response.statusCode(), null);
            }
            requireContentLength(response.headers(), uri);
            String mimeType = requireMimeType(response.headers(), uri);
            String encoding = header(response.headers(), "content-encoding")
                    .map(value -> value.trim().toLowerCase(Locale.ROOT))
                    .orElse("identity");
            try (InputStream wire = new LimitedInputStream(body, maxWireBytes);
                    InputStream decoded = decode(wire, encoding, uri)) {
                return store.persist(decoded, mimeType, maxDecodedBytes);
            }
        }
    }

    private URI validateUri(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("FILE_FROM_URL requires a non-blank HTTPS URL");
        }
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("FILE_FROM_URL requires a valid HTTPS URL", failure);
        }
        String host = canonicalHost(uri.getHost());
        if (!isCanonicalHttpsUri(uri, host)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL accepts only absolute HTTPS URLs without user-info or fragments");
        }
        if (!allowedHosts.contains(host)) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                    "FILE_FROM_URL host is not allowlisted: " + host);
        }
        return uri;
    }

    private static boolean isCanonicalHttpsUri(URI uri, String host) {
        return "https".equalsIgnoreCase(uri.getScheme())
                && host != null
                && uri.getUserInfo() == null
                && uri.getFragment() == null
                && uri.getPort() >= -1
                && uri.getPort() <= MAX_PORT;
    }

    private List<InetAddress> resolveAndRequirePublic(String host) {
        final InetAddress[] addresses;
        try {
            addresses = hostResolver.resolve(host);
        } catch (IOException failure) {
            throw downloadFailure(URI.create("https://" + host),
                    "FILE_FROM_URL host resolution failed", failure);
        }
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("FILE_FROM_URL host resolved to no addresses");
        }
        List<InetAddress> validated = new ArrayList<>(addresses.length);
        for (InetAddress address : addresses) {
            if (address == null || !isPublicAddress(address)) {
                throw new PangrovaException(
                        PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                        "FILE_FROM_URL host resolved to a non-public address");
            }
            validated.add(address);
        }
        return List.copyOf(validated);
    }

    static boolean isPublicAddress(InetAddress address) {
        return PublicNetworkAddressPolicy.isPublic(address);
    }

    private void requireContentLength(Map<String, List<String>> headers, URI uri) {
        header(headers, "content-length").ifPresent(value -> {
            final long length;
            try {
                length = Long.parseLong(value);
            } catch (NumberFormatException failure) {
                throw downloadFailure(uri, "HTTPS FILE Content-Length is invalid", failure);
            }
            if (length < 0 || length > maxWireBytes) {
                throw sizeFailure("compressed", length, maxWireBytes);
            }
        });
    }

    private String requireMimeType(Map<String, List<String>> headers, URI uri) {
        String value = header(headers, "content-type").orElseThrow(() ->
                downloadFailure(uri, "HTTPS FILE response has no Content-Type", null));
        String normalized = value.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
        if (!allowedMimeTypes.contains(normalized)) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                    "HTTPS FILE MIME type is not allowlisted: " + normalized);
        }
        return normalized;
    }

    private static InputStream decode(InputStream wire, String encoding, URI uri)
            throws IOException {
        return switch (encoding) {
            case "", "identity" -> wire;
            case "gzip" -> new GZIPInputStream(wire, MINIMUM_GZIP_BUFFER);
            default -> throw downloadFailure(
                    uri, "HTTPS FILE Content-Encoding is not allowed: " + encoding, null);
        };
    }

    private static java.util.Optional<String> header(
            Map<String, List<String>> headers, String name) {
        return headers.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .flatMap(entry -> entry.getValue().stream())
                .findFirst();
    }

    private static Set<String> normalizeHosts(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        return values.stream().map(HttpsBlobImporter::canonicalHost)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static Set<String> normalizeMimeTypes(Set<String> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("HTTPS FILE MIME allowlist must not be empty");
        }
        return values.stream()
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static String canonicalHost(String host) {
        if (host == null || host.isBlank()) {
            return null;
        }
        return IDN.toASCII(host, IDN.USE_STD3_ASCII_RULES).toLowerCase(Locale.ROOT);
    }

    static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static PangrovaException sizeFailure(
            String representation, long actual, long maximum) {
        return new PangrovaException(
                PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED,
                "HTTPS FILE " + representation + " size " + actual
                        + " exceeds " + maximum + " bytes");
    }

    private static PangrovaException downloadFailure(
            URI uri, String message, Exception failure) {
        String safeEndpoint = uri.getScheme() + "://" + uri.getHost();
        return new PangrovaException(
                PangrovaErrorCode.CONNECTOR_READ_ERROR,
                message + ": endpoint=" + safeEndpoint, failure);
    }

    @FunctionalInterface
    interface HostResolver {
        InetAddress[] resolve(String host) throws IOException;
    }

    @FunctionalInterface
    interface HttpsTransport {
        DownloadResponse fetch(
                URI uri, List<InetAddress> addresses, Duration readTimeout)
                throws IOException, InterruptedException;
    }

    /** Immutable limits and connection-pool policy for governed HTTPS imports. */
    public record Settings(
            Set<String> allowedHosts,
            Set<String> allowedMimeTypes,
            Duration connectTimeout,
            Duration readTimeout,
            long maxWireBytes,
            long maxDecodedBytes,
            PoolSettings pool) {

        public Settings {
            allowedHosts = Set.copyOf(Objects.requireNonNull(
                    allowedHosts, "allowedHosts must not be null"));
            allowedMimeTypes = Set.copyOf(Objects.requireNonNull(
                    allowedMimeTypes, "allowedMimeTypes must not be null"));
            connectTimeout = requirePositive(connectTimeout, "connectTimeout");
            readTimeout = requirePositive(readTimeout, "readTimeout");
            if (maxWireBytes <= 0 || maxDecodedBytes <= 0) {
                throw new IllegalArgumentException("HTTPS FILE byte limits must be positive");
            }
            Objects.requireNonNull(pool, "pool must not be null");
        }
    }

    /** Bounded connection-pool settings shared by all URL imports on one Worker. */
    public record PoolSettings(
            int maxConnections,
            int maxConnectionsPerRoute,
            Duration connectionTimeToLive,
            Duration idleEviction) {

        public PoolSettings {
            if (maxConnections <= 0 || maxConnectionsPerRoute <= 0
                    || maxConnectionsPerRoute > maxConnections) {
                throw new IllegalArgumentException("HTTPS FILE pool limits are invalid");
            }
            connectionTimeToLive = requirePositive(
                    connectionTimeToLive, "connectionTimeToLive");
            idleEviction = requirePositive(idleEviction, "idleEviction");
        }
    }

    record DownloadResponse(
            int statusCode, Map<String, List<String>> headers, InputStream body) {
        DownloadResponse {
            headers = Map.copyOf(headers);
            Objects.requireNonNull(body, "body must not be null");
        }
    }

    @Override
    public void close() throws Exception {
        if (ownedTransport != null) {
            ownedTransport.close();
        }
    }

    private static final class LimitedInputStream extends FilterInputStream {
        private final long maximum;
        private long count;

        private LimitedInputStream(InputStream input, long maximum) {
            super(input);
            this.maximum = maximum;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                add(1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int value = super.read(buffer, offset, length);
            if (value > 0) {
                add(value);
            }
            return value;
        }

        private void add(int value) {
            count = Math.addExact(count, value);
            if (count > maximum) {
                throw sizeFailure("compressed", count, maximum);
            }
        }
    }
}
