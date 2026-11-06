package org.pangrova.ai.worker.blob;

import org.pangrova.filesystem.api.blob.BlobObjectId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.AiTestTaskAuthority;
import org.pangrova.ai.worker.invocation.AiInvocationService;
import org.pangrova.protocol.v2.StageResourceGrant;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.local.blob.FileSystemBlobStorage;

class GovernedBlobStoreTest {

    private static final Instant NOW = Instant.parse("2026-09-13T00:00:00Z");
    private static final Set<String> MIME_TYPES = Set.of("text/plain", "image/png");

    @TempDir
    Path temporaryDirectory;

    @Test
    void persistsContentAddressedObjectAndVerifiesRead() {
        GovernedBlobStore store = store(1024);
        byte[] content = "governed content".getBytes(StandardCharsets.UTF_8);

        BlobReference first = as("tenant-a", "alice", 1024,
                () -> store.persist(content, "TEXT/PLAIN"));
        BlobReference second = as("tenant-a", "alice", 1024,
                () -> store.persist(content, "text/plain"));
        byte[] actual = as("tenant-a", "alice", 1024, () -> store.read(first));

        assertThat(first).isEqualTo(second);
        assertThat(first.uri()).startsWith("pangrova-blob://v2/");
        assertThat(first.uri()).doesNotContain("tenant-a").doesNotContain("alice");
        assertThat(first.size()).isEqualTo(content.length);
        assertThat(first.mimeType()).isEqualTo("text/plain");
        assertThat(actual).isEqualTo(content);
        assertThat(publishedFiles()).hasSize(1);
    }

    @Test
    void cumulativeFileWritesCannotExceedTheAdmittedTaskMediaGrant() {
        GovernedBlobStore store = store(1024);
        String taskId = "task-" + UUID.randomUUID();
        var request = AiTestTaskAuthority.request(
                new AiTestTaskAuthority.Identity("tenant-a", "alice", "query-1", taskId,
                        "trace-1", Instant.now().plus(Duration.ofHours(1)).toEpochMilli()),
                StageResourceGrant.newBuilder().setGrantId(taskId + ":grant")
                        .setCpuSlots(1).setNativeMemoryBytes(1048576)
                        .setMediaBytes(5).build());
        AiExecutionContext context = AiExecutionContext.fromTaskRequest(request);
        AiInvocationService.registerTask(request);
        try {
            AiExecutionContext.callWith(context, () -> {
                store.persist(new byte[] {1, 2, 3}, "image/png");
                assertThatThrownBy(() -> store.persist(new byte[] {4, 5, 6}, "image/png"))
                        .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                                assertThat(failure.category())
                                        .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED));
                return null;
            });
            assertThat(AiInvocationService.resourceUsage(context.tenantId(), context.taskAttempt())
                    .getMediaBytes()).isEqualTo(3);
            assertThat(publishedFiles()).hasSize(1);
            assertThat(stagedFiles()).isEmpty();
        } finally {
            AiInvocationService.releaseTask(context.tenantId(), context.taskAttempt());
        }
    }

    @Test
    void allowsAuthorizedCrossPrincipalReferenceResolutionWithinTenant() {
        GovernedBlobStore store = store(1024);
        BlobReference reference = as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3}, "image/png"));

        assertThat(as("tenant-a", "bob", 1024,
                () -> store.read(reference))).containsExactly(1, 2, 3);
    }

    @Test
    void twoWorkersShareDurableObjectsAndExternallySuppliedCapabilityKey() {
        byte[] key = BlobTestStores.capabilityKey();
        Path objects = temporaryDirectory.resolve("objects");
        GovernedBlobStore workerA = distributedStore(objects, "worker-a", key);
        GovernedBlobStore workerB = distributedStore(objects, "worker-b", key);

        BlobReference reference = as("tenant-a", "alice", 1024,
                () -> workerA.persist(new byte[] {4, 5, 6}, "image/png"));

        assertThat(as("tenant-a", "bob", 1024, () -> workerB.read(reference)))
                .containsExactly(4, 5, 6);
    }

    @Test
    void concurrentWorkersPublishOneContentIdentity() throws Exception {
        byte[] key = BlobTestStores.capabilityKey();
        Path objects = temporaryDirectory.resolve("objects");
        GovernedBlobStore workerA = distributedStore(objects, "worker-a", key);
        GovernedBlobStore workerB = distributedStore(objects, "worker-b", key);
        var executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<BlobReference>> writes = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                GovernedBlobStore worker = (index & 1) == 0 ? workerA : workerB;
                writes.add(() -> as("tenant-a", "alice", 1024,
                        () -> worker.persist(new byte[] {7, 8, 9}, "image/png")));
            }
            List<BlobReference> references = new ArrayList<>();
            for (var future : executor.invokeAll(writes)) {
                references.add(future.get());
            }

            assertThat(references).containsOnly(references.get(0));
            assertThat(publishedFiles()).hasSize(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsCrossTenantReferenceResolution() {
        GovernedBlobStore store = store(1024);
        BlobReference reference = as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3}, "image/png"));

        assertThatThrownBy(() -> as("tenant-b", "bob", 1024,
                () -> store.read(reference)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED));
    }

    @Test
    void rejectsForgedObjectCapabilityWithinTenant() {
        GovernedBlobStore store = store(1024);
        BlobReference reference = as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3}, "image/png"));
        String[] segments = java.net.URI.create(reference.uri()).getPath().split("/", -1);
        BlobReference forged = new BlobReference(
                "pangrova-blob://v2/" + segments[1] + "/" + "f".repeat(64)
                        + "/" + reference.checksum(),
                reference.mimeType(), reference.size(), reference.checksum());

        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.read(forged)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED));
    }

    @Test
    void oversizedAndDisallowedContentNeverPublishes() {
        GovernedBlobStore store = store(3);

        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3, 4}, "image/png")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED));
        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1}, "application/x-executable")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not allowed");
        assertThat(publishedFiles()).isEmpty();
        assertThat(stagedFiles()).isEmpty();
    }

    @Test
    void interruptedInputLeavesNoPublishedObject() {
        GovernedBlobStore store = store(1024);
        InputStream failing = new InputStream() {
            private boolean first = true;

            @Override
            public int read(byte[] target, int offset, int length) throws IOException {
                if (first) {
                    first = false;
                    target[offset] = 1;
                    return 1;
                }
                throw new IOException("injected read failure");
            }

            @Override
            public int read() throws IOException {
                byte[] value = new byte[1];
                int count = read(value, 0, 1);
                return count < 0 ? -1 : Byte.toUnsignedInt(value[0]);
            }
        };

        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.persist(failing, "image/png", 1024)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.CONNECTOR_WRITE_ERROR));
        assertThat(publishedFiles()).isEmpty();
        assertThat(stagedFiles()).isEmpty();
    }

    @Test
    void deadlineExpiringDuringStagingDeletesPartialFile() {
        AtomicLong elapsedMillis = new AtomicLong();
        Clock advancing = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return NOW.plusMillis(elapsedMillis.getAndIncrement());
            }
        };
        GovernedBlobStore store = new GovernedBlobStore(
                new FileSystemBlobStorage(temporaryDirectory.resolve("objects")),
                temporaryDirectory.resolve("staging"), BlobTestStores.capabilityKey(),
                1024, MIME_TYPES, advancing);
        AiExecutionContext context = org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                "tenant-a", "alice", "query-1", "task-1", "trace-1",
                NOW.plusMillis(1).toEpochMilli(), 1024);

        assertThatThrownBy(() -> AiExecutionContext.callWith(context,
                () -> store.persist(new byte[] {1}, "image/png")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("staging deadline");
        assertThat(stagedFiles()).isEmpty();
        assertThat(publishedFiles()).isEmpty();
    }

    @Test
    void failedBackendPublicationDoesNotReturnReferenceOrLeaveStaging() {
        BlobStorage failing = new BlobStorage() {
            @Override
            public void publish(
                    BlobObjectId object, Path content)
                    throws IOException {
                throw new IOException("injected publication failure");
            }

            @Override
            public InputStream open(BlobObjectId object)
                    throws IOException {
                throw new IOException("not published");
            }
        };
        GovernedBlobStore store = new GovernedBlobStore(
                failing, temporaryDirectory.resolve("staging"),
                BlobTestStores.capabilityKey(), 1024, MIME_TYPES,
                Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3}, "image/png")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.CONNECTOR_WRITE_ERROR));
        assertThat(stagedFiles()).isEmpty();
    }

    @Test
    void readDetectsPostPublicationTampering() throws IOException {
        GovernedBlobStore store = store(1024);
        BlobReference reference = as("tenant-a", "alice", 1024,
                () -> store.persist(new byte[] {1, 2, 3}, "image/png"));
        Path object = publishedFiles().iterator().next();
        Files.write(object, new byte[] {9, 9, 9},
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);

        assertThatThrownBy(() -> as("tenant-a", "alice", 1024,
                () -> store.read(reference)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.CONNECTOR_READ_ERROR));
    }

    @Test
    void stagingCleanupIsAgeAndCountBounded() throws IOException {
        GovernedBlobStore store = store(1024);
        Path staging = temporaryDirectory.resolve("staging/scope");
        Files.createDirectories(staging);
        Path oldA = Files.write(staging.resolve("a.part"), new byte[] {1});
        Path oldB = Files.write(staging.resolve("b.part"), new byte[] {2});
        Files.setLastModifiedTime(oldA, FileTime.from(NOW.minus(Duration.ofDays(2))));
        Files.setLastModifiedTime(oldB, FileTime.from(NOW.minus(Duration.ofDays(2))));

        int deleted = store.cleanupStaging(Duration.ofDays(1), 1);

        assertThat(deleted).isEqualTo(1);
        assertThat(Files.exists(oldA) || Files.exists(oldB)).isTrue();
    }

    private GovernedBlobStore store(long maxBytes) {
        return new GovernedBlobStore(
                new FileSystemBlobStorage(temporaryDirectory.resolve("objects")),
                temporaryDirectory.resolve("staging"),
                BlobTestStores.capabilityKey(), maxBytes, MIME_TYPES,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private GovernedBlobStore distributedStore(
            Path objects, String worker, byte[] capabilityKey) {
        return new GovernedBlobStore(
                new FileSystemBlobStorage(objects),
                temporaryDirectory.resolve("staging").resolve(worker),
                capabilityKey, 1024, MIME_TYPES,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private Set<Path> publishedFiles() {
        Path objects = temporaryDirectory.resolve("objects");
        try (var paths = Files.walk(objects)) {
            return paths.filter(Files::isRegularFile)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
        } catch (IOException failure) {
            throw new AssertionError(failure);
        }
    }

    private Set<Path> stagedFiles() {
        Path staging = temporaryDirectory.resolve("staging");
        try (var paths = Files.walk(staging)) {
            return paths.filter(Files::isRegularFile)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
        } catch (IOException failure) {
            throw new AssertionError(failure);
        }
    }

    private static <T> T as(
            String tenant, String principal, long maxMediaBytes, Supplier<T> action) {
        return AiTestTaskAuthority.callWithRegisteredTask(tenant, principal,
                Instant.now().plus(Duration.ofHours(1)).toEpochMilli(), maxMediaBytes, action);
    }
}
