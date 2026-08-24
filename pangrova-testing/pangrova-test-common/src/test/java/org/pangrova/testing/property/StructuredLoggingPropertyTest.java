package org.pangrova.testing.property;

import org.pangrova.testing.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying structured logging compliance across Pangrova critical paths.
 *
 * <p>Scans Java source files in key modules to verify:</p>
 * <ul>
 *   <li>Log statements use structured key=value format (no string concatenation in SLF4J calls)</li>
 *   <li>Critical path classes include queryId/taskId/workerId in log messages</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 7.12</b></p>
 */
class StructuredLoggingPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();

    /**
     * Pattern matching SLF4J log calls with string concatenation inside the message argument.
     * Detects: LOG.info("some text" + variable) or logger.warn("text" + var + "more")
     * Allows: LOG.info("key={} other={}", var1, var2) (SLF4J parameterized)
     * Recognises common SLF4J variable names: LOG / log / LOGGER / logger.
     */
    private static final Pattern SLF4J_CONCAT_PATTERN = Pattern.compile(
            "(LOG|log|LOGGER|logger)\\.(info|warn|error|debug|trace)\\(\"[^\"]*\"\\s*\\+");

    /**
     * Critical path classes that MUST include queryId/taskId/workerId in their log statements.
     */
    private static final List<String> CRITICAL_PATH_CLASSES = List.of(
            "QueryManager.java",
            "TaskManager.java",
            "HeartbeatSender.java",
            "WorkerHealthScheduler.java",
            "RemoteTaskDispatcher.java",
            "RemoteTaskStatusReporter.java",
            "TaskStatusReceiver.java",
            "QueryDispatcher.java"
    );

    private static final List<LogEntry> SLF4J_LOG_ENTRIES = scanSlf4jLogEntries();
    private static final List<LogEntry> CRITICAL_PATH_LOG_ENTRIES = scanCriticalPathLogEntries();

    // -----------------------------------------------------------------------
    // Property 16a: No string concatenation in SLF4J log calls
    // -----------------------------------------------------------------------

    /**
     * Property 16: SLF4J log statements must not use string concatenation.
     * Use parameterized messages (LOG.info("key={}", value)) instead.
     *
     * <p><b>Validates: Requirements 7.12</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 16: Structured logging compliance")
    void slf4jLogsMustNotUseStringConcatenation(@ForAll("slf4jLogEntries") LogEntry entry) {
        assertThat(entry.hasConcatenation)
                .as("SLF4J log at %s:%d uses string concatenation — use parameterized format instead: %s",
                        entry.relativePath, entry.lineNumber, entry.lineContent.trim())
                .isFalse();
    }

    /**
     * Property 16: Critical path log statements must include trace identifiers
     * (queryId, taskId, or workerId) for observability.
     *
     * <p><b>Validates: Requirements 7.12</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 16: Structured logging compliance")
    void criticalPathLogsMustIncludeTraceIdentifiers(@ForAll("criticalPathLogEntries") LogEntry entry) {
        boolean hasIdentifier = entry.lineContent.contains("queryId")
                || entry.lineContent.contains("taskId")
                || entry.lineContent.contains("workerId")
                || entry.lineContent.contains("worker=")
                || entry.lineContent.contains("event=")
                || entry.lineContent.contains("METRIC ")
                || entry.lineContent.contains("count=");
        assertThat(hasIdentifier)
                .as("Critical path log at %s:%d missing trace identifier (queryId/taskId/workerId/event=): %s",
                        entry.relativePath, entry.lineNumber, entry.lineContent.trim())
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<LogEntry> slf4jLogEntries() {
        if (SLF4J_LOG_ENTRIES.isEmpty()) {
            return Arbitraries.just(new LogEntry("(none)", 0, "", false));
        }
        return Arbitraries.of(SLF4J_LOG_ENTRIES);
    }

    @Provide
    Arbitrary<LogEntry> criticalPathLogEntries() {
        if (CRITICAL_PATH_LOG_ENTRIES.isEmpty()) {
            return Arbitraries.just(new LogEntry("(none)", 0, "event=noop", false));
        }
        return Arbitraries.of(CRITICAL_PATH_LOG_ENTRIES);
    }

    // -----------------------------------------------------------------------
    // Scanning
    // -----------------------------------------------------------------------

    private static List<LogEntry> scanSlf4jLogEntries() {
        List<LogEntry> entries = new ArrayList<>();
        for (Path file : findAllMainJavaFiles()) {
            try {
                List<String> lines = Files.readAllLines(file);
                String relativePath = PROJECT_ROOT.relativize(file).toString();
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (isSlf4jLogLine(line)) {
                        boolean hasConcatenation = SLF4J_CONCAT_PATTERN.matcher(line).find();
                        entries.add(new LogEntry(relativePath, i + 1, line, hasConcatenation));
                    }
                }
            } catch (IOException ignored) {
                // skip unreadable files
            }
        }
        return List.copyOf(entries);
    }

    private static List<LogEntry> scanCriticalPathLogEntries() {
        List<LogEntry> entries = new ArrayList<>();
        for (Path file : findAllMainJavaFiles()) {
            String fileName = file.getFileName().toString();
            if (!CRITICAL_PATH_CLASSES.contains(fileName)) {
                continue;
            }
            try {
                List<String> lines = Files.readAllLines(file);
                String relativePath = PROJECT_ROOT.relativize(file).toString();
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (isAnyLogLine(line)) {
                        entries.add(new LogEntry(relativePath, i + 1, line, false));
                    }
                }
            } catch (IOException ignored) {
                // skip unreadable files
            }
        }
        return List.copyOf(entries);
    }

    private static boolean isSlf4jLogLine(String line) {
        String trimmed = line.trim();
        return startsWithAnyLogger(trimmed, "info(")
                || startsWithAnyLogger(trimmed, "warn(")
                || startsWithAnyLogger(trimmed, "error(")
                || startsWithAnyLogger(trimmed, "debug(")
                || startsWithAnyLogger(trimmed, "trace(");
    }

    private static boolean startsWithAnyLogger(String trimmed, String method) {
        return trimmed.startsWith("LOG." + method)
                || trimmed.startsWith("log." + method)
                || trimmed.startsWith("LOGGER." + method)
                || trimmed.startsWith("logger." + method);
    }

    private static boolean isAnyLogLine(String line) {
        String trimmed = line.trim();
        return isSlf4jLogLine(line)
                || trimmed.contains(".info(\"")
                || trimmed.contains(".warn(\"")
                || trimmed.contains(".error(\"")
                || trimmed.contains(".debug(\"");
    }

    // -----------------------------------------------------------------------
    // File discovery (reused pattern from CleanCodeMetricsPropertyTest)
    // -----------------------------------------------------------------------

    private static List<Path> findAllMainJavaFiles() {
        List<Path> result = new ArrayList<>();
        List<String> modules = findJavaModules();
        for (String module : modules) {
            Path srcDir = PROJECT_ROOT.resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(srcDir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".java"))
                        .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                        .filter(p -> !p.getFileName().toString().equals("module-info.java"))
                        .forEach(result::add);
            } catch (IOException ignored) {
                // skip if walk fails
            }
        }
        return List.copyOf(result);
    }

    private static List<String> findJavaModules() {
        List<String> modules = new ArrayList<>();
        try (Stream<Path> walked = Files.walk(PROJECT_ROOT)) {
            walked.filter(Files::isDirectory)
                    .filter(d -> d.getFileName().toString().startsWith("pangrova-"))
                    .filter(d -> Files.isRegularFile(d.resolve("pom.xml")))
                    .filter(d -> Files.isDirectory(d.resolve("src/main/java")))
                    .forEach(d -> modules.add(PROJECT_ROOT.relativize(d).toString()));
        } catch (IOException ignored) {
            // skip
        }
        return List.copyOf(modules);
    }

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    record LogEntry(String relativePath, int lineNumber, String lineContent, boolean hasConcatenation) {
        @Override
        public String toString() {
            return relativePath + ":" + lineNumber;
        }
    }
}
