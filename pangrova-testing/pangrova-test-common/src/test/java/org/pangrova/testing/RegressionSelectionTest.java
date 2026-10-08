package org.pangrova.testing;

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
        assertThat(value(project, PROFILE + "/properties/pangrova.e2e.excludedGroups")).isEqualTo("none");
        var e2e = read(ROOT.resolve("pangrova-testing/pangrova-test-e2e/pom.xml"));
        assertThat(value(e2e, "/project/build/plugins/plugin[artifactId='maven-surefire-plugin']"
                + "/configuration/excludedGroups")).isEqualTo("${pangrova.e2e.excludedGroups}");
    }

    @Test
    void dailyRunsRetainTheirExplicitResourceCategories() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        assertThat(value(project, "/project/properties/pangrova.e2e.excludedGroups"))
                .isEqualTo("routing-integration,native-required,performance");
    }

    @Test
    void moduleGroupPoliciesCannotOverrideFullRegression() throws Exception {
        var project = read(ROOT.resolve("pom.xml"));
        for (var module : List.of("worker", "coordinator", "metadata")) {
            var property = "pangrova." + module + ".excludedTestGroups";
            assertThat(value(project, PROFILE + "/properties/" + property)).isEqualTo("none");
            var owner = read(ROOT.resolve("pangrova-" + module + "/pom.xml"));
            assertThat(value(owner, "/project/properties/" + property)).isEmpty();
            assertThat(value(owner, "/project/build/plugins/plugin[artifactId='maven-surefire-plugin']"
                    + "/configuration/excludedGroups")).isEqualTo("${" + property + "}");
        }
        var worker = read(ROOT.resolve("pangrova-worker/pom.xml"));
        assertThat(value(worker, CONFIGURATION + "/excludes/exclude")).isEqualTo("none");
        assertThat(value(worker, CONFIGURATION + "/rerunFailingTestsCount")).isEqualTo("0");
    }

    @Test
    void coverageAgentSurvivesEveryModuleJvmOverride() throws Exception {
        for (var module : List.of("", "pangrova-worker/", "pangrova-coordinator/", "pangrova-resource/",
                "pangrova-client/pangrova-client-cli/", "pangrova-testing/pangrova-test-e2e/")) {
            var project = read(ROOT.resolve(module + "pom.xml"));
            assertThat(value(project, "//plugin[artifactId='maven-surefire-plugin']/configuration/argLine"))
                    .as("JaCoCo late-bound agent in %s", module).contains("@{jacoco.agent.argLine}");
        }
    }

    @Test
    void sanitizerCiUsesTheSamePresetEnvironmentAsLocalValidation() throws Exception {
        var workflow = Files.readString(ROOT.resolve(".github/workflows/ci.yml"));
        assertThat(workflow).contains("run: ctest --preset ${{ matrix.sanitizer.preset }} --timeout 300")
                .doesNotContain("detect_leaks=1:halt_on_error=1:suppressions=sanitizer-suppressions/asan.supp");
    }

    @Test
    void productionCiBuildsAnActualImageBeforeUnfilteredRegressionAndCoverage() throws Exception {
        var workflow = Files.readString(ROOT.resolve(".github/workflows/ci.yml"));
        var start = workflow.indexOf("  production-regression:");
        var end = workflow.indexOf("  tpch-performance:", start);
        assertThat(start).isGreaterThanOrEqualTo(0);
        assertThat(end).isGreaterThan(start);
        var production = workflow.substring(start, end).replaceAll("[ \\t]*\\\\[ \\t]*\\R[ \\t]*", " ");
        assertThat(production).contains("needs: [native-build, native-sanitizers]")
                .contains("docker buildx bake --load -f deploy/docker/docker-bake.hcl pangrova")
                .contains("IMAGE=\"$PANGROVA_E2E_IMAGE\" SOURCE_COMMIT=\"$GITHUB_SHA\" docker buildx bake")
                .contains("org.opencontainers.image.revision\" }}' \"$image_id\")\" == \"$GITHUB_SHA\"")
                .contains("image_id=$(docker image inspect --format '{{.Id}}' \"$PANGROVA_E2E_IMAGE\")")
                .contains("\"PANGROVA_E2E_IMAGE_ID=$image_id\" >> \"$GITHUB_ENV\"")
                .contains("stage-production-regression-native.sh \"$PANGROVA_E2E_IMAGE_ID\"")
                .contains("\"$PANGROVA_E2E_IMAGE_ID\" \"$stage\" \"$GITHUB_SHA\"")
                .contains("-Pnative-runtime,full-regression,coverage verify")
                .contains("-Dtest=ActivationSmokeE2ETest test")
                .contains("diff-coverage-check.sh --threshold 60")
                .doesNotContain("pangrova/pangrova:latest");
        assertThat(production.indexOf("docker buildx bake")).isLessThan(production.indexOf("full-regression,coverage verify"));
        assertThat(production.indexOf("docker image inspect")).isLessThan(production.indexOf("stage-production-regression-native"));
        assertThat(production.indexOf("stage-production-regression-native")).isLessThan(production.indexOf("full-regression,coverage verify"));
        for (var property : List.of("-Dpangrova.e2e.image=\"$PANGROVA_E2E_IMAGE\"",
                "-Dpangrova.e2e.image-id=\"$PANGROVA_E2E_IMAGE_ID\"")) {
            assertThat(production.split(java.util.regex.Pattern.quote(property), -1).length - 1).isEqualTo(3);
        }
    }

    private static Element read(Path file) throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(file.toFile()).getDocumentElement();
    }

    private static String value(Element project, String expression) throws Exception {
        return XPathFactory.newInstance().newXPath().evaluate(expression, project).trim();
    }
}
