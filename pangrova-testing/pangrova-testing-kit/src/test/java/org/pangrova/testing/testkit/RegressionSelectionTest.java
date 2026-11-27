package org.pangrova.testing.testkit;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** Release regression must not silently inherit daily test exclusions. */
class RegressionSelectionTest {
    private static final Path ROOT = TestProjectPaths.findProjectRoot();
    private static final String PROFILE = "/project/profiles/profile[id='full-regression']";
    private static final String CONFIGURATION = PROFILE
            + "/build/plugins/plugin[artifactId='maven-surefire-plugin']/configuration";

    @Test
    void fullRegressionIncludesUntaggedIntegrationAndPropertyTests() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        assertThat(value(project, CONFIGURATION + "/includes/@combine.self")).isEqualTo("override");
        assertThat(value(project, CONFIGURATION + "/includes/include")).isEqualTo("**/*Test.java");
        assertThat(value(project, CONFIGURATION + "/excludes/@combine.self")).isEqualTo("override");
        assertThat(value(project, CONFIGURATION + "/excludes/exclude")).isEqualTo("none");
        assertThat(value(project, CONFIGURATION + "/groups/@combine.self")).isEqualTo("override");
        assertThat(value(project, CONFIGURATION + "/groups")).isEmpty();
    }

    @Test
    void e2eModuleConsumesTheReleaseExclusionPolicy() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        assertThat(value(project, PROFILE + "/properties/pangrova.e2e.excludedGroups"))
                .isEqualTo("native-runtime-owner");
        var e2e = read(ROOT.resolve("pangrova-testing/pangrova-testing-integration-tests/pom.xml"));
        assertThat(value(e2e, "/project/build/plugins/plugin[artifactId='maven-surefire-plugin']"
                + "/configuration/excludedGroups")).isEqualTo("${pangrova.e2e.excludedGroups}");
        var workflow = Files.readString(ROOT.resolve(".github/workflows/ci.yml"));
        assertThat(workflow).contains("-Dpangrova.e2e.excludedGroups=none -Dtest=\"$suite\" test");
        for (var owner : List.of("WorkerBootstrapE2ETest",
                "NativeExchangeTransportExternalIntegrationTest",
                "WorkerShuffleChannelAdmissionIntegrationTest",
                "NativeShuffleEgressContractIntegrationTest",
                "NativeAiFailureProjectionRegistrationTest",
                "WorkerMembershipAdmissionE2ETest",
                "LocalGovernedFileSqlE2ETest",
                "CopySourceReadHttpE2ETest#privateTicketStagesExactVersionAndRestartsFromManagedBytes")) {
            assertThat(workflow).contains(owner);
        }
    }

    @Test
    void everyRuntimeOwnerSourceHasAnIsolatedCiSelection() throws Exception {
        java.util.Map<?, ?> workflow;
        try (var input = Files.newBufferedReader(ROOT.resolve(".github/workflows/ci.yml"))) {
            workflow = new org.yaml.snakeyaml.Yaml().load(input);
        }
        var jobs = (java.util.Map<?, ?>) workflow.get("jobs");
        var production = (java.util.Map<?, ?>) jobs.get("production-regression");
        var steps = (List<?>) production.get("steps");
        String isolatedRun = steps.stream().map(java.util.Map.class::cast)
                .filter(step -> "Run isolated Native runtime owner E2E".equals(step.get("name")))
                .map(step -> (String) step.get("run")).findFirst().orElseThrow();
        var ownerTag = java.util.regex.Pattern.compile(
                "@(?:org\\.junit\\.jupiter\\.api\\.)?Tag\\s*\\(\\s*\"native-runtime-owner\"\\s*\\)");
        try (var sources = Files.walk(ROOT.resolve(
                "pangrova-testing/pangrova-testing-integration-tests/src/test/java"))) {
            for (var source : sources.filter(path -> path.toString().endsWith("Test.java")).toList()) {
                if (!ownerTag.matcher(Files.readString(source)).find()) continue;
                String owner = source.getFileName().toString().replace(".java", "");
                assertThat(isolatedRun).as("isolated Native CI selection for %s", owner).contains(owner);
            }
        }
    }

    @Test
    void dailyRunsRetainTheirExplicitResourceCategories() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        assertThat(value(project, "/project/properties/pangrova.e2e.excludedGroups"))
                .isEqualTo("routing-integration,native-required,performance,e2e");
        // The jqwik engine ignores tag filters, so E2E property tests are excluded
        // by path in the daily build and re-enabled by full regression.
        assertThat(value(project, "/project/build/pluginManagement/plugins/"
                + "plugin[artifactId='maven-surefire-plugin']/configuration/excludes/exclude[2]"))
                .isEqualTo("${pangrova.e2e.excludedClasses}");
        assertThat(value(project, "/project/properties/pangrova.e2e.excludedClasses"))
                .isEqualTo("**/e2e/**/*PropertyTest.java");
        assertThat(value(project, PROFILE + "/properties/pangrova.e2e.excludedClasses")).isEqualTo("none");
    }

    @Test
    void moduleGroupPoliciesCannotOverrideFullRegression() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        for (var module : List.of("worker", "coordinator", "metadata")) {
            var property = "pangrova." + module + ".excludedTestGroups";
            assertThat(value(project, PROFILE + "/properties/" + property)).isEqualTo("none");
            var ownerPath = switch (module) {
                case "worker" -> "pangrova-worker";
                case "coordinator" -> "pangrova-coordination/pangrova-coordination-control";
                default -> "pangrova-metadata/pangrova-metadata-catalog";
            };
            var owner = read(ROOT.resolve(ownerPath).resolve("pom.xml"));
            assertThat(value(owner, "/project/properties/" + property)).isEmpty();
            assertThat(value(owner, "/project/build/plugins/plugin[artifactId='maven-surefire-plugin']"
                    + "/configuration/excludedGroups")).isEqualTo("${" + property + "}");
        }
        var worker = read(ROOT.resolve("pangrova-worker/pom.xml"));
        assertThat(value(worker, CONFIGURATION + "/excludes/exclude")).isEqualTo("none");
        var profileRetries = value(worker, CONFIGURATION + "/rerunFailingTestsCount");
        var inheritedRetries = value(worker, "/project/build/plugins/plugin[artifactId='maven-surefire-plugin']"
                + "/configuration/rerunFailingTestsCount");
        assertThat(profileRetries.isEmpty() ? inheritedRetries : profileRetries).isEqualTo("0");
    }

    @Test
    void coverageAgentSurvivesEveryModuleJvmOverride() throws Exception {
        // Modules without their own surefire argLine inherit the root configuration;
        // only an explicit override can drop the late-bound JaCoCo agent.
        var inherited = value(read(ROOT.resolve("pom.xml")),
                "//plugin[artifactId='maven-surefire-plugin']/configuration/argLine");
        for (var module : List.of("", "pangrova-worker/", "pangrova-coordination/pangrova-coordination-control/", "pangrova-scheduler/pangrova-scheduler-api/", "pangrova-scheduler/pangrova-scheduler-policy/", "pangrova-scheduler/pangrova-scheduler-strategies-builtin/",
                "pangrova-client/pangrova-client-cli/", "pangrova-testing/pangrova-testing-integration-tests/")) {
            var project = read(ROOT.resolve(module + "pom.xml"));
            var override = value(project, "//plugin[artifactId='maven-surefire-plugin']/configuration/argLine");
            assertThat(override.isEmpty() ? inherited : override)
                    .as("JaCoCo late-bound agent in %s", module).contains("@{jacoco.agent.argLine}");
        }
    }

    @Test
    void sanitizerCiUsesTheSamePresetEnvironmentAsLocalValidation() throws Exception {
        var caller = Files.readString(ROOT.resolve(".github/workflows/ci.yml"));
        assertThat(caller).contains("uses: ./.github/workflows/native-sanitizer.yml",
                "preset: dev-asan", "preset: dev-ubsan", "preset: ${{ matrix.sanitizer.preset }}");
        var workflow = Files.readString(ROOT.resolve(".github/workflows/native-sanitizer.yml"));
        assertThat(normalizeCommands(workflow)).contains(
                "python3 bin/run-native-tests.py --build-dir build-${{ inputs.name }} "
                        + "--prepared-java \"$RUNNER_TEMP/native-java-fixture\" "
                        + "-- --preset ${{ inputs.preset }} --timeout 300")
                .doesNotContain("detect_leaks=1:halt_on_error=1:suppressions=sanitizer-suppressions/asan.supp");
    }

    @Test
    void productionCiBuildsAnActualImageBeforeUnfilteredRegression() throws Exception {
        var workflow = Files.readString(ROOT.resolve(".github/workflows/ci.yml"));
        var start = workflow.indexOf("  production-regression:");
        var end = workflow.indexOf("  tpch-performance:", start);
        assertThat(start).isGreaterThanOrEqualTo(0);
        assertThat(end).isGreaterThan(start);
        var production = normalizeCommands(workflow.substring(start, end));
        assertThat(production).contains("needs: [change-scope, iceberg-sdk, native-build, native-sanitizers]")
                .contains("needs.iceberg-sdk.result == 'success'")
                .contains("IMAGE=\"$PANGROVA_E2E_IMAGE\" SOURCE_COMMIT=\"$GITHUB_SHA\" docker buildx bake")
                .contains("image_id=$(docker image inspect --format '{{.Id}}' \"$PANGROVA_E2E_IMAGE\")")
                .contains("\"PANGROVA_E2E_IMAGE_ID=$image_id\" >> \"$GITHUB_ENV\"")
                .contains("stage-production-regression-native.sh \"$PANGROVA_E2E_IMAGE_ID\"")
                .contains("-Pnative-runtime,full-regression verify")
                .contains("-Dtest=\"$suite\" test")
                .doesNotContain("-Dtest=ActivationSmokeE2ETest test")
                .doesNotContain("diff-coverage-check.sh")
                .doesNotContain("pangrova/pangrova:latest");
        int bakeStart = production.indexOf("docker buildx bake --load");
        int inspectStart = production.indexOf("image_id=$(docker image inspect", bakeStart);
        assertThat(bakeStart).isGreaterThanOrEqualTo(0);
        assertThat(inspectStart).isGreaterThan(bakeStart);
        assertThat(production.substring(bakeStart, inspectStart).trim())
                .startsWith("docker buildx bake --load -f deploy/docker/docker-bake.hcl ")
                .contains("--allow \"fs.read=$RUNNER_TEMP/pangrova-iceberg-sdk\"",
                        "--set \"pangrova.contexts.iceberg-sdk=$RUNNER_TEMP/pangrova-iceberg-sdk\"")
                .endsWith(" pangrova");
        assertThat(bakeStart).isLessThan(production.indexOf("full-regression verify"));
        assertThat(production.indexOf("docker image inspect")).isLessThan(production.indexOf("stage-production-regression-native"));
        assertThat(production.indexOf("stage-production-regression-native")).isLessThan(production.indexOf("full-regression verify"));
        for (var property : List.of("-Dpangrova.e2e.image=\"$PANGROVA_E2E_IMAGE\"",
                "-Dpangrova.e2e.image-id=\"$PANGROVA_E2E_IMAGE_ID\"")) {
            assertThat(production.split(java.util.regex.Pattern.quote(property), -1).length - 1).isEqualTo(2);
        }
    }

    private static Element read(Path file) throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(file.toFile()).getDocumentElement();
    }

    private static String normalizeCommands(String workflow) {
        return workflow.replaceAll("\\\\\\r?\\n\\s*", " ").replaceAll("\\s+", " ");
    }

    private static String value(Element project, String expression) throws Exception {
        return XPathFactory.newInstance().newXPath().evaluate(expression, project).trim();
    }
}
