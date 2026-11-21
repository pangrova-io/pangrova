package org.pangrova.shuffle.local.spool;

import java.nio.file.Path;

/** Separate JVM fixture: tests the OS lock rather than only Java's overlapping-lock guard. */
public final class LocalSpoolDirectoryOwnerProbe {
    private LocalSpoolDirectoryOwnerProbe() {}
    public static void main(String[] arguments) {
        try (var store = new LocalFsSpoolStore(Path.of(arguments[0]))) {
            System.out.println("OWNED");
        } catch (IllegalStateException conflict) {
            System.out.println("BUSY");
            System.exit(23);
        }
    }
}
