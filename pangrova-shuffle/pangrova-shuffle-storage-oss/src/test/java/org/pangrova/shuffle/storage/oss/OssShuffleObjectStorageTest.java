package org.pangrova.shuffle.storage.oss;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OssShuffleObjectStorageTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "pfx";
    private static final Pattern KEY_PATTERN = Pattern.compile("<Key>([^<]+)</Key>");

    @Test
    void rejectsPerObjectDeleteFailureByAbsence() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            fake.deleteFailures = ignored -> Set.of("pfx/shuffle/batch");
            try (OssShuffleObjectStorage objects = storage(fake)) {
                assertThatThrownBy(() -> objects.delete(List.of("shuffle/batch")))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("1 object(s)")
                        .hasMessageContaining("key=pfx/shuffle/batch");
                assertThat(fake.deleteRequests).hasValue(1);
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void rejectsLaterBatchErrorAfterEarlierBatchSucceeded() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            fake.deleteFailures = index -> index == 1 ? Set.of("pfx/shuffle/batch-1000") : Set.of();
            try (OssShuffleObjectStorage objects = storage(fake)) {
                List<String> keys = IntStream.range(0, 1_001)
                        .mapToObj(index -> "shuffle/batch-" + index).toList();
                assertThatThrownBy(() -> objects.delete(keys))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("key=pfx/shuffle/batch-1000");
                assertThat(fake.deleteRequests).hasValue(2);
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void acceptsSuccessfulDeleteAndSkipsEmptyRequest() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                objects.delete(List.of());
                assertThat(fake.deleteRequests).hasValue(0);
                objects.delete(List.of("shuffle/batch"));
                assertThat(fake.deleteRequests).hasValue(1);
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void putImmutableRoundTripAndVerifyStored() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                byte[] data = "hello oss".getBytes(StandardCharsets.UTF_8);
                byte[] sha = sha256(data);
                objects.putImmutable("shuffle/batch", () -> new ByteArrayInputStream(data),
                        data.length, sha);
                assertThat(objects.get("shuffle/batch")).isEqualTo(data);
                objects.verifyStored("shuffle/batch", data.length, sha);
                assertThatThrownBy(() -> objects.verifyStored("shuffle/batch", data.length,
                        sha256("other".getBytes(StandardCharsets.UTF_8))))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("does not match");
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void putImmutableConflictWithSameContentReconciles() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                byte[] data = "same content".getBytes(StandardCharsets.UTF_8);
                byte[] sha = sha256(data);
                objects.putImmutable("shuffle/batch", () -> new ByteArrayInputStream(data),
                        data.length, sha);
                objects.putImmutable("shuffle/batch", () -> new ByteArrayInputStream(data),
                        data.length, sha);
                assertThat(objects.get("shuffle/batch")).isEqualTo(data);
                assertThat(fake.putRequests).hasValue(2);
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void putImmutableConflictWithDifferentContentFails() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                byte[] first = "first".getBytes(StandardCharsets.UTF_8);
                byte[] second = "second".getBytes(StandardCharsets.UTF_8);
                objects.putImmutable("shuffle/batch", () -> new ByteArrayInputStream(first),
                        first.length, sha256(first));
                assertThatThrownBy(() -> objects.putImmutable("shuffle/batch",
                        () -> new ByteArrayInputStream(second), second.length, sha256(second)))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("conflicts with existing content");
                assertThat(objects.get("shuffle/batch")).isEqualTo(first);
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void listPrefixReturnsKeysInLexicographicOrder() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                put(objects, "a/2", "two");
                put(objects, "a/1", "one");
                put(objects, "b/1", "other");
                assertThat(objects.listPrefix("a")).containsExactly("a/1", "a/2");
                assertThat(objects.listPrefix("b")).containsExactly("b/1");
            }
        } finally {
            fake.close();
        }
    }

    @Test
    void deletePrefixPagePagesThroughAllKeys() throws Exception {
        FakeOss fake = FakeOss.start();
        try {
            try (OssShuffleObjectStorage objects = storage(fake)) {
                for (int index = 0; index < 5; index++) {
                    put(objects, "page/" + index, "value-" + index);
                }
                String after = null;
                int pages = 0;
                do {
                    after = objects.deletePrefixPage("page", after, 2);
                    pages++;
                } while (after != null);
                assertThat(pages).isEqualTo(3);
                assertThat(objects.listPrefix("page")).isEmpty();
            }
        } finally {
            fake.close();
        }
    }

    private static void put(OssShuffleObjectStorage objects, String key, String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        objects.putImmutable(key, () -> new ByteArrayInputStream(data), data.length, sha256(data));
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static OssShuffleObjectStorage storage(FakeOss fake) {
        OSS client = new OSSClientBuilder().build(
                "http://127.0.0.1:" + fake.port(),
                "access-key-id", "access-key-secret");
        return OssShuffleObjectStorage.testing(client, BUCKET, PREFIX);
    }

    /**
     * Minimal raw-socket OSS stand-in. Uses exact wire header casing because the OSS SDK
     * matches {@code x-oss-meta-} and {@code Content-Length} case-sensitively, which
     * com.sun.net.httpserver's header canonicalization cannot produce.
     */
    private static final class FakeOss implements AutoCloseable {

        private ServerSocket serverSocket;
        private final TreeMap<String, byte[]> objects = new TreeMap<>();
        private final AtomicInteger putRequests = new AtomicInteger();
        private final AtomicInteger deleteRequests = new AtomicInteger();
        private volatile IntFunction<Set<String>> deleteFailures = ignored -> Set.of();

        static FakeOss start() throws IOException {
            FakeOss fake = new FakeOss();
            fake.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread thread = new Thread(fake::acceptLoop, "fake-oss");
            thread.setDaemon(true);
            thread.start();
            return fake;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try {
                    handle(serverSocket.accept());
                } catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private void handle(Socket socket) {
            try (socket;
                    InputStream in = socket.getInputStream();
                    OutputStream out = socket.getOutputStream()) {
                String requestLine = readLine(in);
                if (requestLine == null || requestLine.isEmpty()) {
                    return;
                }
                String[] parts = requestLine.split(" ");
                String method = parts[0];
                String target = parts[1];
                Map<String, String> headers = new HashMap<>();
                String line;
                while ((line = readLine(in)) != null && !line.isEmpty()) {
                    int colon = line.indexOf(':');
                    if (colon > 0) {
                        headers.put(line.substring(0, colon).trim().toLowerCase(),
                                line.substring(colon + 1).trim());
                    }
                }
                int contentLength = Integer.parseInt(
                        headers.getOrDefault("content-length", "0"));
                byte[] body = in.readNBytes(contentLength);

                String path = target;
                String query = "";
                int question = target.indexOf('?');
                if (question >= 0) {
                    path = target.substring(0, question);
                    query = target.substring(question + 1);
                }
                String prefix = "/" + BUCKET + "/";
                String key = path.startsWith(prefix) ? path.substring(prefix.length()) : "";
                if (query.contains("list-type=2")) {
                    handleList(out, query);
                } else if ("PUT".equals(method)) {
                    handlePut(out, key, body);
                } else if ("POST".equals(method) && "delete".equals(query)) {
                    handleDelete(out, body);
                } else if ("HEAD".equals(method) || "GET".equals(method)) {
                    handleGet(out, key, "HEAD".equals(method));
                } else {
                    writeResponse(out, "HTTP/1.1 405 Method Not Allowed", Map.of(), new byte[0], null);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void handlePut(OutputStream out, String key, byte[] body) throws IOException {
            putRequests.incrementAndGet();
            if (objects.putIfAbsent(key, body) != null) {
                byte[] error = ("<Error><Code>FileAlreadyExists</Code>"
                        + "<Message>The object you specified already exists and can not be "
                        + "overwritten.</Message></Error>").getBytes(StandardCharsets.UTF_8);
                writeResponse(out, "HTTP/1.1 409 Conflict",
                        Map.of("Content-Type", "application/xml"), error, null);
            } else {
                writeResponse(out, "HTTP/1.1 200 OK", Map.of(), new byte[0], null);
            }
        }

        private void handleGet(OutputStream out, String key, boolean head) throws IOException {
            byte[] data = objects.get(key);
            if (data == null) {
                writeResponse(out, "HTTP/1.1 404 Not Found", Map.of(), new byte[0], null);
                return;
            }
            Map<String, String> headers = new HashMap<>();
            headers.put("x-oss-meta-sha256",
                    java.util.Base64.getEncoder().encodeToString(sha256(data)));
            headers.put("Last-Modified", "Wed, 01 Jan 2020 00:00:00 GMT");
            if (head) {
                writeResponse(out, "HTTP/1.1 200 OK", headers, new byte[0], data.length);
            } else {
                writeResponse(out, "HTTP/1.1 200 OK", headers, data, null);
            }
        }

        private void handleDelete(OutputStream out, byte[] requestBody) throws IOException {
            int requestIndex = deleteRequests.getAndIncrement();
            String body = new String(requestBody, StandardCharsets.UTF_8);
            Matcher matcher = KEY_PATTERN.matcher(body);
            List<String> keys = new ArrayList<>();
            while (matcher.find()) {
                keys.add(matcher.group(1));
            }
            Set<String> failures = deleteFailures.apply(requestIndex);
            StringBuilder xml = new StringBuilder("<DeleteResult>");
            for (String key : keys) {
                if (failures.contains(key)) {
                    xml.append("<Error><Key>").append(key)
                            .append("</Key><Code>AccessDenied</Code></Error>");
                } else {
                    objects.remove(key);
                    xml.append("<Deleted><Key>").append(key).append("</Key></Deleted>");
                }
            }
            xml.append("</DeleteResult>");
            writeResponse(out, "HTTP/1.1 200 OK",
                    Map.of("Content-Type", "application/xml"),
                    xml.toString().getBytes(StandardCharsets.UTF_8), null);
        }

        private void handleList(OutputStream out, String query) throws IOException {
            Map<String, String> params = new HashMap<>();
            for (String part : query.split("&")) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    params.put(part.substring(0, eq),
                            URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8));
                }
            }
            String prefix = params.getOrDefault("prefix", "");
            String after = params.get("continuation-token");
            if (after == null) {
                after = params.get("start-after");
            }
            int maxKeys = params.containsKey("max-keys")
                    ? Integer.parseInt(params.get("max-keys")) : 1000;
            List<String> candidates = new ArrayList<>();
            for (String key : objects.keySet()) {
                if (key.startsWith(prefix) && (after == null || key.compareTo(after) > 0)) {
                    candidates.add(key);
                }
            }
            List<String> page = candidates.subList(0, Math.min(maxKeys, candidates.size()));
            boolean truncated = page.size() < candidates.size();
            StringBuilder xml = new StringBuilder("<ListBucketResult>")
                    .append("<Name>").append(BUCKET).append("</Name>")
                    .append("<Prefix>").append(prefix).append("</Prefix>")
                    .append("<KeyCount>").append(page.size()).append("</KeyCount>")
                    .append("<MaxKeys>").append(maxKeys).append("</MaxKeys>")
                    .append("<IsTruncated>").append(truncated).append("</IsTruncated>");
            if (truncated) {
                xml.append("<NextContinuationToken>").append(page.get(page.size() - 1))
                        .append("</NextContinuationToken>");
            }
            for (String key : page) {
                xml.append("<Contents><Key>").append(key)
                        .append("</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified>")
                        .append("<ETag>\"etag\"</ETag><Type>Normal</Type>")
                        .append("<Size>").append(objects.get(key).length).append("</Size>")
                        .append("<StorageClass>Standard</StorageClass></Contents>");
            }
            xml.append("</ListBucketResult>");
            writeResponse(out, "HTTP/1.1 200 OK",
                    Map.of("Content-Type", "application/xml"),
                    xml.toString().getBytes(StandardCharsets.UTF_8), null);
        }

        private static void writeResponse(OutputStream out, String statusLine,
                Map<String, String> headers, byte[] body, Integer contentLength)
                throws IOException {
            StringBuilder response = new StringBuilder(statusLine).append("\r\n");
            response.append("Content-Length: ")
                    .append(contentLength != null ? contentLength : body.length)
                    .append("\r\n");
            for (Map.Entry<String, String> header : headers.entrySet()) {
                response.append(header.getKey()).append(": ")
                        .append(header.getValue()).append("\r\n");
            }
            response.append("Connection: close\r\n\r\n");
            out.write(response.toString().getBytes(StandardCharsets.UTF_8));
            out.write(body);
            out.flush();
        }

        private static String readLine(InputStream in) throws IOException {
            StringBuilder line = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                if (ch == '\n') {
                    return line.toString();
                }
                if (ch != '\r') {
                    line.append((char) ch);
                }
            }
            return line.isEmpty() ? null : line.toString();
        }
    }
}
