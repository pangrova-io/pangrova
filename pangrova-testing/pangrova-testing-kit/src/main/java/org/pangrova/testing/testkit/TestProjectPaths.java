package org.pangrova.testing.testkit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility for resolving the Pangrova project root directory.
 *
 * <p>Walks up from the current working directory looking for the root
 * marker files ({@code pom.xml} + {@code .pangrova/modules.yaml}).
 * Falls back to the current working directory when no marker is found.
 */
public final class TestProjectPaths {

    private static final int MAX_DEPTH = 10;
    private static final String ROOT_MARKER = ".pangrova/modules.yaml";

    private TestProjectPaths() {
        // utility class
    }

    /**
     * Finds the Pangrova multi-module project root.
     *
     * @return absolute path to the project root directory
     */
    public static Path findProjectRoot() {
        Path current = Paths.get("").toAbsolutePath();
        for (int i = 0; i < MAX_DEPTH; i++) {
            if (Files.isRegularFile(current.resolve(ROOT_MARKER))
                    && Files.isRegularFile(current.resolve("pom.xml"))) {
                return current;
            }
            current = current.getParent();
            if (current == null) {
                break;
            }
        }
        return Paths.get("").toAbsolutePath();
    }
}
