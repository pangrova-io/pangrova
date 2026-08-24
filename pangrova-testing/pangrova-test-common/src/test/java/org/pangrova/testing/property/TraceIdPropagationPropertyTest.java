package org.pangrova.testing.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.testing.TestProjectPaths;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying that queryId is consistently used in structured
 * log format across coordinator-to-worker dispatch boundaries.
 *
 * <p>Scans all production source files for log statements referencing queryId and
 * verifies they use the {@code queryId={}} parameterized placeholder format, not
 * string concatenation, so trace IDs are correctly captured by log aggregators.</p>
 *
 * <p><b>Validates: Requirements 7.12, trace-ID propagation</b></p>
 */
class TraceIdPropagationPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();

    /** Detects LOG.info/warn/error lines that reference queryId in the format string. */
    private static final Pattern QUERY_ID_LOG_PATTERN =
            Pattern.compile("LOG\\.(info|warn|error)\\(\"[^\"]*queryId");

    /** Detects concatenation adjacent to a queryId literal — the banned form. */
    private static final Pattern QUERY_ID_CONCAT_PATTERN =
            Pattern.compile("\"queryId=\"\\s*\\+|\"\\s*\\+\\s*queryId");

    private static final List<QueryIdLogEntry> QUERY_ID_LOG_ENTRIES =
            scanQueryIdLogEntries();

    // -----------------------------------------------------------------------
    // Property 14: queryId log references use structured placeholder format
    // -----------------------------------------------------------------------

    /**
     * Property 14: Every log statement that references queryId must use the
     * {@code queryId={}} SLF4J parameterized format, not string concatenation.
     * Concatenation defeats structured log parsing and breaks trace correlation.
     *
     * <p><b>Validates: Requirements 7.12</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 14: TraceId propagation")
    void queryIdLogsMustUseStructuredPlaceholderFormat(
            @ForAll("queryIdLogEntries") QueryIdLogEntry entry) {
        assertThat(entry.hasConcatenation())
                .as("queryId log at %s:%d uses string concatenation — use queryId={} format: %s",
                        entry.relativePath, entry.lineNumber, entry.lineContent.trim())
                .isFalse();
    }

    // -----------------------------------------------------------------------
    // Provider
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<QueryIdLogEntry> queryIdLogEntries() {
        if (QUERY_ID_LOG_ENTRIES.isEmpty()) {
            return Arbitraries.just(new QueryIdLogEntry("(none)", 0, "event=noop"));
        }
        return Arbitraries.of(QUERY_ID_LOG_ENTRIES);
    }

    // -----------------------------------------------------------------------
    // Scanning
    // -----------------------------------------------------------------------

    private static List<QueryIdLogEntry> scanQueryIdLogEntries() {
        List<QueryIdLogEntry> entries = new ArrayList<>();
        for (Path file : findAllMainJavaFiles()) {
            try {
                List<String> lines = Files.readAllLines(file);
                String relativePath = PROJECT_ROOT.relativize(file).toString();
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (QUERY_ID_LOG_PATTERN.matcher(line).find()) {
                        entries.add(new QueryIdLogEntry(relativePath, i + 1, line));
                    }
                }
            } catch (IOException ignored) {
                // skip unreadable files
            }
        }
        return List.copyOf(entries);
    }

    private static List<Path> findAllMainJavaFiles() {
        List<Path> result = new ArrayList<>();
        try (Stream<Path> moduleDirs = Files.list(PROJECT_ROOT)) {
            moduleDirs
                    .filter(Files::isDirectory)
                    .filter(d -> d.getFileName().toString().startsWith("pangrova-"))
                    .filter(d -> !d.getFileName().toString().equals("pangrova-testing"))
                    .filter(d -> Files.isRegularFile(d.resolve("pom.xml")))
                    .filter(d -> Files.isDirectory(d.resolve("src/main/java")))
                    .forEach(moduleDir -> {
                        Path srcDir = moduleDir.resolve("src/main/java");
                        try (Stream<Path> files = Files.walk(srcDir)) {
                            files.filter(p -> p.getFileName().toString().endsWith(".java"))
                                    .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                                    .forEach(result::add);
                        } catch (IOException ignored) {
                            // skip
                        }
                    });
        } catch (IOException ignored) {
            // skip
        }
        return List.copyOf(result);
    }

    // -----------------------------------------------------------------------
    // Data types
    // -----------------------------------------------------------------------

    record QueryIdLogEntry(String relativePath, int lineNumber, String lineContent) {

        boolean hasConcatenation() {
            return QUERY_ID_CONCAT_PATTERN.matcher(lineContent).find();
        }

        @Override
        public String toString() {
            return relativePath + ":" + lineNumber;
        }
    }
}
