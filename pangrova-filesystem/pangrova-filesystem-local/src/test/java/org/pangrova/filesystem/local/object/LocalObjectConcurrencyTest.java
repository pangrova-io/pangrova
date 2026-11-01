package org.pangrova.filesystem.local.object;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.filesystem.api.object.*;
import org.pangrova.filesystem.api.profile.StorageProfile;

import static org.assertj.core.api.Assertions.*;

/** Real file operations with deterministic barriers at NIO publication boundaries, including another JVM. */
class LocalObjectConcurrencyTest {
    @TempDir Path directory;

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void committedReceiptAlwaysDescribesItsOwnBytes(boolean multipart, boolean isolatedProviderLoader) throws Exception {
        var fs = new LocalObjectTestFileSystem();
        Path root = fs.wrap(directory);
        var location = location(root);
        var first = new LocalObjectStoreAccessor(root);
        try (var isolated = isolatedLoader()) {
        ObjectStoreAccessor next = isolatedProviderLoader
                ? (ObjectStoreAccessor) isolated.loadClass(LocalObjectStoreAccessor.class.getName())
                        .getConstructor(Path.class).newInstance(root)
                : new LocalObjectStoreAccessor(root);
        var committed = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var nextTransition = new CompletableFuture<String>();
        fs.afterMove = path -> {
            if (!path.getFileName().toString().equals("item.bin")) return;
            if (Thread.currentThread().getName().equals("first-publisher")) {
                committed.countDown(); await(release);
            } else if (Thread.currentThread().getName().equals("next-publisher")) nextTransition.complete("published");
        };
        fs.beforeRealPath = path -> {
            if (Thread.currentThread().getName().equals("next-publisher")) nextTransition.complete("coordinating");
        };
        var firstThread = Executors.newSingleThreadExecutor(run -> new Thread(run, "first-publisher"));
        var nextThread = Executors.newSingleThreadExecutor(run -> new Thread(run, "next-publisher"));
        try {
            var accepted = firstThread.submit(() -> write(first, location, "first", multipart));
            assertThat(committed.await(5, TimeUnit.SECONDS)).isTrue();
            var following = nextThread.submit(() -> write(next, location, "replacement", false));
            nextTransition.get(5, TimeUnit.SECONDS);
            release.countDown();
            FileStat receipt = accepted.get(5, TimeUnit.SECONDS);
            FileStat replacement = following.get(5, TimeUnit.SECONDS);
            assertThat(receipt.size()).isEqualTo(5);
            assertThat(receipt.providerVersion()).isEqualTo(hash("first"));
            assertThat(replacement.providerVersion()).isEqualTo(hash("replacement"));
            assertThat(Files.readString(directory.resolve("space/item.bin"))).isEqualTo("replacement");
        } finally {
            release.countDown(); firstThread.shutdownNow(); nextThread.shutdownNow();
            assertThat(firstThread.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nextThread.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void conditionalDeleteCannotDeleteAConcurrentReplacement(boolean nestedProfileRoot) throws Exception {
        var fs = new LocalObjectTestFileSystem();
        Path root = fs.wrap(directory);
        var location = location(root);
        var deleter = new LocalObjectStoreAccessor(root);
        Path writerRoot = nestedProfileRoot ? root.resolve("space") : root;
        var writer = new LocalObjectStoreAccessor(writerRoot);
        var writerLocation = nestedProfileRoot
                ? new ObjectLocation(new StorageProfile("nested", StorageType.LOCAL, writerRoot.toString(),
                        "", "", "", Map.of(), Map.of()), "") : location;
        FileStat before = write(deleter, location, "before", false);
        var deleting = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var writerTransition = new CompletableFuture<String>();
        fs.beforeDelete = path -> {
            if (path.getFileName().toString().equals("item.bin")
                    && Thread.currentThread().getName().equals("conditional-deleter")) {
                deleting.countDown(); await(release);
            }
        };
        fs.beforeRealPath = path -> {
            if (Thread.currentThread().getName().equals("concurrent-writer")) writerTransition.complete("coordinating");
        };
        fs.afterMove = path -> {
            if (path.getFileName().toString().equals("item.bin")
                    && Thread.currentThread().getName().equals("concurrent-writer")) writerTransition.complete("published");
        };
        var deleteThread = Executors.newSingleThreadExecutor(run -> new Thread(run, "conditional-deleter"));
        var writeThread = Executors.newSingleThreadExecutor(run -> new Thread(run, "concurrent-writer"));
        try {
            var removed = deleteThread.submit(() -> deleter.delete(location, "item.bin", before.providerVersion()));
            assertThat(deleting.await(5, TimeUnit.SECONDS)).isTrue();
            var replaced = writeThread.submit(() -> write(writer, writerLocation, "replacement", false));
            writerTransition.get(5, TimeUnit.SECONDS);
            release.countDown();
            removed.get(5, TimeUnit.SECONDS);
            replaced.get(5, TimeUnit.SECONDS);
            assertThat(Files.readString(directory.resolve("space/item.bin"))).isEqualTo("replacement");
        } finally {
            release.countDown(); deleteThread.shutdownNow(); writeThread.shutdownNow();
            assertThat(deleteThread.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(writeThread.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void anotherJvmMustHonorTheSameFilesystemMutationLock() throws Exception {
        Path space = Files.createDirectories(directory.resolve("space"));
        Files.writeString(space.resolve("item.bin"), "before");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = null;
        try (var channel = FileChannel.open(space.resolve(".pangrova-mutation.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE); var lock = channel.lock()) {
            child = new ProcessBuilder(java, "-cp", classpath, LocalObjectMutationProcess.class.getName(),
                    directory.toString()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            var process = child;
            var line = CompletableFuture.supplyAsync(() -> {
                try { return new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))
                        .readLine(); } catch (IOException failure) { throw new UncheckedIOException(failure); }
            }).get(5, TimeUnit.SECONDS);
            assertThat(line).isEqualTo("locking");
            assertThat(Files.readString(space.resolve("item.bin"))).isEqualTo("before");
            assertThat(process.isAlive()).isTrue();
        } finally {
            if (child != null) {
                boolean exited = child.waitFor(5, TimeUnit.SECONDS);
                if (!exited) child.destroyForcibly().waitFor();
                assertThat(exited).isTrue();
                assertThat(child.exitValue()).isZero();
            }
        }
        assertThat(Files.readString(space.resolve("item.bin"))).isEqualTo("replacement");
    }

    @Test
    void privateStagingCannotBeListedOrAddressedAsUserData() throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var root = new ObjectLocation(location(directory).profile(), "");
        var upload = accessor.beginMultipart(root, "public.bin", null);
        accessor.uploadPart(upload, 1, bytes("secret"), 6);
        assertThat(accessor.list(root, "", null, 100).files()).isEmpty();
        String privatePart = ".pangrova-multipart/" + upload.uploadId() + "/00001.part";
        assertThatThrownBy(() -> accessor.head(root, privatePart)).isInstanceOfSatisfying(PangrovaException.class,
                failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
        accessor.abortMultipart(upload);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fatalInputFailureStillDeletesItsPrivateDraft(boolean multipart) throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var location = location(directory);
        var fatal = new AssertionError("input failed");
        var input = new InputStream() { @Override public int read() { throw fatal; } };
        Path parent;
        if (multipart) {
            var upload = accessor.beginMultipart(location, "item.bin", null);
            parent = directory.resolve(".pangrova-multipart").resolve(upload.uploadId());
            assertThatThrownBy(() -> accessor.uploadPart(upload, 1, input, 1)).isSameAs(fatal);
        } else {
            parent = directory.resolve("space");
            assertThatThrownBy(() -> accessor.put(location, "item.bin", input, 1, null)).isSameAs(fatal);
        }
        try (var files = Files.list(parent)) {
            assertThat(files.map(path -> path.getFileName().toString()))
                    .noneMatch(name -> name.startsWith(".pangrova-upload-") || name.startsWith(".upload-"));
        }
    }

    @Test
    void exhaustedRangeStillValidatesArgumentsAndHonorsZeroLengthReads() throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var location = location(directory);
        write(accessor, location, "x", false);
        try (var input = accessor.openRange(location, "item.bin", 1, 0, null)) {
            assertThat(input.read(new byte[1], 0, 0)).isZero();
            assertThatThrownBy(() -> input.read(new byte[1], -1, 0)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> input.read(null, 0, 0)).isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void multipartHandleCannotBeTransplantedToAnotherObject() throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var location = location(directory);
        var upload = accessor.beginMultipart(location, "item.bin", null);
        var part = accessor.uploadPart(upload, 1, bytes("first"), 5);
        var moved = new MultipartUpload(upload.uploadId(), location, "other.bin");
        for (var action : List.<org.assertj.core.api.ThrowableAssert.ThrowingCallable>of(
                () -> accessor.uploadPart(moved, 1, bytes("other"), 5),
                () -> accessor.completeMultipart(moved, List.of(part)), () -> accessor.abortMultipart(moved))) {
            assertThatThrownBy(action).isInstanceOfSatisfying(PangrovaException.class,
                    failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        }
        accessor.completeMultipart(upload, List.of(part));
        assertThat(Files.readString(directory.resolve("space/item.bin"))).isEqualTo("first");
        assertThat(directory.resolve("space/other.bin")).doesNotExist();
    }

    @Test
    void abortWhileAnInputIsBlockedCannotResurrectTheSession() throws Exception {
        var first = new LocalObjectStoreAccessor(directory);
        var second = new LocalObjectStoreAccessor(directory);
        var upload = first.beginMultipart(location(directory), "item.bin", null);
        var reading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var input = new ByteArrayInputStream(new byte[]{1}) {
            @Override public synchronized int read(byte[] bytes, int offset, int length) {
                reading.countDown(); await(release); return super.read(bytes, offset, length);
            }
        };
        var thread = Executors.newSingleThreadExecutor();
        try {
            var pending = thread.submit(() -> first.uploadPart(upload, 1, input, 1));
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            second.abortMultipart(upload);
            release.countDown();
            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOfSatisfying(PangrovaException.class,
                            failure -> assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
            assertThat(directory.resolve(".pangrova-multipart").resolve(upload.uploadId())).doesNotExist();
            try (var files = Files.list(directory.resolve(".pangrova-multipart"))) {
                assertThat(files.map(path -> path.getFileName().toString()))
                        .containsExactly(".pangrova-mutation.lock");
            }
            assertThat(directory.resolve("space/item.bin")).doesNotExist();
        } finally { release.countDown(); thread.shutdownNow(); assertThat(thread.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test
    void draftDeletionFailureIsSuppressedWithoutReplacingTheFatalInputFailure() throws Exception {
        var fs = new LocalObjectTestFileSystem();
        var cleanup = new IOException("draft deletion failed");
        fs.beforeDelete = path -> { if (path.getFileName().toString().startsWith(".pangrova-upload-")) throw cleanup; };
        Path root = fs.wrap(directory);
        var fatal = new AssertionError("input failed");
        var input = new InputStream() { @Override public int read() { throw fatal; } };
        assertThatThrownBy(() -> new LocalObjectStoreAccessor(root).put(location(root), "item.bin", input, 1, null))
                .isSameAs(fatal);
        assertThat(fatal.getSuppressed()).containsExactly(cleanup);
        fs.beforeDelete = path -> { };
    }

    @Test
    void anUploadIdCollisionNeverDeletesAnotherSessionsMetadata() throws Exception {
        var fs = new LocalObjectTestFileSystem();
        var collided = new java.util.concurrent.atomic.AtomicReference<Path>();
        fs.beforeDirectory = path -> {
            if (path.getFileName().toString().matches("[0-9a-f-]{36}")) {
                Files.createDirectory(path);
                Files.writeString(path.resolve("other-owner"), "preserve");
                collided.set(path);
                throw new FileAlreadyExistsException(path.toString());
            }
        };
        Path root = fs.wrap(directory);
        assertThatThrownBy(() -> new LocalObjectStoreAccessor(root).beginMultipart(location(root), "item.bin", null))
                .isInstanceOf(PangrovaException.class);
        assertThat(collided.get()).isNotNull();
        assertThat(Files.readString(collided.get().resolve("other-owner"))).isEqualTo("preserve");
    }

    @Test
    void truncatedStorageCannotSilentlyReturnAShortDeclaredRange() throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var location = location(directory);
        write(accessor, location, "data", false);
        try (var input = accessor.openRange(location, "item.bin", 0, 4, null)) {
            Files.newByteChannel(directory.resolve("space/item.bin"), StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING).close();
            assertThatThrownBy(input::readAllBytes).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void interruptedMutationAdmissionReleasesItsDraftAndJvmGate() throws Exception {
        Path space = Files.createDirectories(directory.resolve("space"));
        Files.writeString(space.resolve("item.bin"), "before");
        var fs = new LocalObjectTestFileSystem();
        var locking = new CountDownLatch(1);
        var opened = new java.util.concurrent.atomic.AtomicInteger();
        fs.beforeRealPath = path -> locking.countDown();
        fs.beforeChannel = path -> {
            if (path.getFileName().toString().equals(".pangrova-mutation.lock")) opened.incrementAndGet();
        };
        Path root = fs.wrap(directory);
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var interruptRetained = new java.util.concurrent.atomic.AtomicBoolean();
        var thread = new Thread(() -> {
            try { write(new LocalObjectStoreAccessor(root), location(root), "replacement", false); }
            catch (Throwable caught) { failure.set(caught); interruptRetained.set(Thread.currentThread().isInterrupted()); }
        }, "interrupted-publisher");
        String identity = space.toRealPath().resolve(".pangrova-mutation.lock").toString();
        try (var admission = org.pangrova.common.concurrent.ProcessResourceLocks.acquire(List.of(identity));
                var channel = FileChannel.open(space.resolve(".pangrova-mutation.lock"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE); var lock = channel.lock()) {
            thread.start();
            assertThat(locking.await(5, TimeUnit.SECONDS)).isTrue();
            thread.interrupt();
            thread.join(5_000);
            assertThat(thread.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(IOException.class);
            assertThat(interruptRetained).isTrue();
            var probe = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    LocalObjectMutationProcess.class.getName(), directory.toString(), "probe")
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try {
                assertThat(probe.waitFor(5, TimeUnit.SECONDS)).isTrue();
                assertThat(probe.exitValue()).isZero();
                String result = new String(probe.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
                assertThat(result).as("owner kernel lock after waiter interruption; waiter channels=%s", opened.get())
                        .isEqualTo("held");
                assertThat(opened).hasValue(0);
            } finally { if (probe.isAlive()) probe.destroyForcibly().waitFor(); }
            assertThat(Files.readString(space.resolve("item.bin"))).isEqualTo("before");
            try (var files = Files.list(space)) {
                assertThat(files.map(path -> path.getFileName().toString()))
                        .noneMatch(name -> name.startsWith(".pangrova-upload-"));
            }
        } finally { if (thread.isAlive()) { thread.interrupt(); thread.join(5_000); } }
        write(new LocalObjectStoreAccessor(directory), location(directory), "after", false);
        assertThat(Files.readString(space.resolve("item.bin"))).isEqualTo("after");
    }

    @Test
    void multipartPartsResumeAcrossJvmLocalesUsingTheSamePhysicalNames() throws Exception {
        var accessor = new LocalObjectStoreAccessor(directory);
        var upload = accessor.beginMultipart(location(directory), "item.bin", null);
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var child = new ProcessBuilder(java, "-cp", classpath, LocalObjectMutationProcess.class.getName(),
                directory.toString(), upload.uploadId()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try {
            assertThat(child.waitFor(5, TimeUnit.SECONDS)).isTrue();
            assertThat(child.exitValue()).isZero();
            String checksum = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            accessor.completeMultipart(upload, List.of(new UploadedPart(1, checksum, 1)));
            assertThat(Files.readAllBytes(directory.resolve("space/item.bin"))).containsExactly((byte) 1);
        } finally { if (child.isAlive()) child.destroyForcibly().waitFor(); }
    }

    @Test
    void legacyUnboundMultipartStateCannotPublishOrBeTransplanted() throws Exception {
        String id = UUID.randomUUID().toString();
        Path session = Files.createDirectories(directory.resolve(".pangrova-multipart").resolve(id));
        Path part = Files.writeString(session.resolve("00001.part"), "legacy");
        var accessor = new LocalObjectStoreAccessor(directory);
        var upload = new MultipartUpload(id, location(directory), "item.bin");
        var receipt = new UploadedPart(1, hash("legacy"), 6);
        assertThatThrownBy(() -> accessor.completeMultipart(upload, List.of(receipt)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        assertThat(Files.readString(part)).isEqualTo("legacy");
        assertThat(directory.resolve("space/item.bin")).doesNotExist();
    }

    @Test
    void concurrentAbortIsIdempotentAndCannotRecreateAnUploadLock() throws Exception {
        var first = new LocalObjectStoreAccessor(directory);
        var second = new LocalObjectStoreAccessor(directory);
        var upload = first.beginMultipart(location(directory), "item.bin", null);
        first.uploadPart(upload, 1, bytes("part"), 4);
        var start = new CountDownLatch(1);
        var threads = Executors.newFixedThreadPool(2);
        try {
            var a = threads.submit(() -> { await(start); first.abortMultipart(upload); });
            var b = threads.submit(() -> { await(start); second.abortMultipart(upload); });
            start.countDown();
            a.get(5, TimeUnit.SECONDS); b.get(5, TimeUnit.SECONDS);
            assertThat(directory.resolve(".pangrova-multipart").resolve(upload.uploadId())).doesNotExist();
            second.abortMultipart(upload);
        } finally { start.countDown(); threads.shutdownNow(); assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uncertainMultipartCompletionCannotReplayOverANewerWriter(boolean publicationFailure) throws Exception {
        var fs = new LocalObjectTestFileSystem();
        Path root = fs.wrap(directory);
        var accessor = new LocalObjectStoreAccessor(root);
        var upload = accessor.beginMultipart(location(root), "item.bin", null);
        var part = accessor.uploadPart(upload, 1, bytes("first"), 5);
        if (publicationFailure) {
            fs.afterMove = path -> {
                if (path.getFileName().toString().equals("item.bin")) throw new IOException("lost publication reply");
            };
        } else {
            fs.beforeDelete = path -> {
                if (path.getFileName().toString().equals("00001.part")) throw new IOException("cleanup failed");
            };
        }
        assertThatThrownBy(() -> accessor.completeMultipart(upload, List.of(part)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining(publicationFailure ? "publication was attempted" : "committed");
        assertThat(Files.readString(directory.resolve("space/item.bin"))).isEqualTo("first");
        write(new LocalObjectStoreAccessor(directory), location(directory), "replacement", false);
        assertThatThrownBy(() -> accessor.completeMultipart(upload, List.of(part)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        assertThat(Files.readString(directory.resolve("space/item.bin"))).isEqualTo("replacement");
        fs.afterMove = path -> { }; fs.beforeDelete = path -> { };
        accessor.abortMultipart(upload);
    }

    private static java.net.URLClassLoader isolatedLoader() {
        return new java.net.URLClassLoader(new java.net.URL[]{
                LocalObjectStoreAccessor.class.getProtectionDomain().getCodeSource().getLocation()},
                LocalObjectConcurrencyTest.class.getClassLoader()) {
            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!name.startsWith("org.pangrova.filesystem.local.")) return super.loadClass(name, resolve);
                synchronized (getClassLoadingLock(name)) {
                    Class<?> type = findLoadedClass(name);
                    if (type == null) type = findClass(name);
                    if (resolve) resolveClass(type);
                    return type;
                }
            }
        };
    }

    static ObjectLocation location(Path root) {
        return new ObjectLocation(new StorageProfile("local", StorageType.LOCAL, root.toString(), "", "", "",
                Map.of(), Map.of()), "space");
    }
    static FileStat write(ObjectStoreAccessor accessor, ObjectLocation location, String text,
            boolean multipart) throws IOException {
        if (!multipart) return accessor.put(location, "item.bin", bytes(text), text.length(), null);
        var upload = accessor.beginMultipart(location, "item.bin", null);
        var part = accessor.uploadPart(upload, 1, bytes(text), text.length());
        return accessor.completeMultipart(upload, List.of(part));
    }
    private static InputStream bytes(String text) { return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)); }
    private static String hash(String text) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("Barrier was not released"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
}
