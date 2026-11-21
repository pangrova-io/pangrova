package org.pangrova.coordinator.ci;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CI guard: fails the build if any critical test class is listed in
 * {@code <testExclude>} within {@code pangrova-coordination/pangrova-coordination-control/pom.xml}.
 *
 * <p>Critical tests validate the main query execution pipeline and must
 * never be silently excluded. Adding a critical test to the exclude list
 * requires removing it from {@link #CRITICAL_TEST_CLASSES} first, which
 * forces an explicit review and approval.</p>
 *
 * <p>Validates: Requirements 1.6, 2.6 — CI SHALL fail if critical test
 * classes are excluded.</p>
 */
class CriticalTestExclusionGuardTest {

    /**
     * Test classes that must never appear in testExclude.
     * Extend this list as new critical tests are added.
     */
    private static final List<String> CRITICAL_TEST_CLASSES = List.of(
            "PipelineIntegrationTest",
            "QueryDispatcherTest",
            "CoordinatorQueryLifecycleTest",
            "QueryStateMachinePropertyTest",
            "StageFragmenterTest",
            "StageFragmenterPropertyTest",
            "StageDAGTest",
            "DAGDispatchTest",
            "DistributedScenarioTest"
    );

    private static final Pattern EXCLUDE_PATTERN =
            Pattern.compile("<testExclude>\\*\\*/([^<]+)\\.java</testExclude>");

    @Test
    void criticalTestClassesMustNotBeExcluded() throws IOException {
        Path pomPath = resolvePomPath();
        String pomContent = Files.readString(pomPath);

        List<String> excludedClasses = EXCLUDE_PATTERN.matcher(pomContent).results()
                .map(m -> m.group(1))
                .collect(Collectors.toList());

        List<String> violatingExcludes = CRITICAL_TEST_CLASSES.stream()
                .filter(excludedClasses::contains)
                .collect(Collectors.toList());

        assertThat(violatingExcludes)
                .as("Critical test classes must not be in testExclude. "
                        + "Remove them from pom.xml or justify removal from CRITICAL_TEST_CLASSES list.")
                .isEmpty();
    }

    @Test
    void pomFileIsReadable() throws IOException {
        Path pomPath = resolvePomPath();
        assertThat(pomPath).exists().isReadable();
    }

    private static Path resolvePomPath() {
        // Try multiple resolution strategies for different execution contexts
        Path projectRoot = Path.of(System.getProperty("user.dir"));
        Path pomInModule = projectRoot.resolve("pom.xml");
        if (Files.exists(pomInModule)) {
            return pomInModule;
        }
        // Fallback: running from repo root
        Path pomFromRoot = projectRoot.resolve("pangrova-coordination/pangrova-coordination-control/pom.xml");
        if (Files.exists(pomFromRoot)) {
            return pomFromRoot;
        }
        return pomInModule; // will fail with clear message
    }
}
