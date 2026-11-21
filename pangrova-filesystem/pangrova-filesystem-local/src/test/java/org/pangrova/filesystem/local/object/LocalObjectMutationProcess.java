package org.pangrova.filesystem.local.object;

import java.nio.file.Path;

/** Independent JVM owns real NIO files and OS locks; the test filesystem only reports transition boundaries. */
public final class LocalObjectMutationProcess {
    public static void main(String[] args) throws Exception {
        if (args.length > 1 && args[1].equals("probe")) {
            try (var channel = java.nio.channels.FileChannel.open(Path.of(args[0]).resolve("space/.pangrova-mutation.lock"),
                    java.nio.file.StandardOpenOption.WRITE); var lock = channel.tryLock()) {
                System.out.println(lock == null ? "held" : "free");
            }
            return;
        }
        if (args.length > 1) {
            java.util.Locale.setDefault(java.util.Locale.forLanguageTag("ar-EG"));
            Path root = Path.of(args[0]);
            var accessor = new LocalObjectStoreAccessor(root);
            var upload = new org.pangrova.filesystem.api.object.MultipartUpload(args[1],
                    LocalObjectConcurrencyTest.location(root), "item.bin");
            var receipt = accessor.uploadPart(upload, 1, new java.io.ByteArrayInputStream(new byte[]{1}), 1);
            System.out.println(receipt.etag());
            return;
        }
        var fs = new LocalObjectTestFileSystem();
        fs.beforeChannel = path -> {
            if (path.getFileName().toString().equals(".pangrova-mutation.lock")) {
                System.out.println("locking"); System.out.flush();
            }
        };
        fs.afterMove = path -> {
            if (path.getFileName().toString().equals("item.bin")) { System.out.println("published"); System.out.flush(); }
        };
        Path root = fs.wrap(Path.of(args[0]));
        LocalObjectConcurrencyTest.write(new LocalObjectStoreAccessor(root),
                LocalObjectConcurrencyTest.location(root), "replacement", false);
    }
}
