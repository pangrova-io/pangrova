package org.pangrova.testing.property;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.architecture.JavaSourceReferences;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the artifact/package/dependency model declared in
 * {@code .pangrova/modules.yaml} against the actual reactor.
 *
 * <p>Facts are recomputed from POMs and sources on every run; the committed
 * {@code .pangrova/module-baseline.yaml} must stay identical to them, so an
 * unreviewed structural change fails instead of silently expanding the
 * architecture. Dependency checking is default-deny: an artifact edge or an
 * imported package that is neither allowed nor recorded as owned debt is a
 * violation.</p>
 */
class ModuleDependencyPropertyTest {

    private static final Path ROOT = TestProjectPaths.findProjectRoot();
    private static final Pattern MODULE_NAME_PATTERN =
            Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*");

    private static final Model MODEL = Model.load(ROOT);

    @Test
    void reactorIsFullyDeclared() {
        assertThat(MODEL.discoveredPoms())
                .as("every pom.xml must be declared in modules.yaml or module-baseline.yaml; "
                        + "an undeclared module can escape every architecture rule")
                .isEqualTo(MODEL.declaredPoms());

        MODEL.declaredPoms().forEach((artifactId, path) -> {
            Path pom = ROOT.resolve(path).resolve("pom.xml");
            assertThat(pom).as("declared pom for %s", artifactId).exists();
            assertThat(pomArtifactId(pom)).as("artifactId in %s", path).isEqualTo(artifactId);
        });

        Set<String> moduleNames = new HashSet<>();
        for (Policy policy : MODEL.policies().values()) {
            assertThat(policy.moduleName())
                    .as("Automatic-Module-Name for %s", policy.artifactId())
                    .matches(MODULE_NAME_PATTERN);
            assertThat(moduleNames.add(policy.moduleName()))
                    .as("duplicate Automatic-Module-Name %s", policy.moduleName())
                    .isTrue();
            assertThat(pomModuleName(ROOT.resolve(policy.path()).resolve("pom.xml")))
                    .as("%s must declare <module.name>%s</module.name> so the jar manifest "
                            + "carries the Automatic-Module-Name declared in modules.yaml",
                            policy.artifactId(), policy.moduleName())
                    .isEqualTo(policy.moduleName());
        }
    }

    @Test
    void packageOwnershipIsUnambiguous() {
        Map<String, List<String>> rootOwners = new TreeMap<>();
        for (Policy policy : MODEL.policies().values()) {
            if (policy.resourceOnly()) {
                assertThat(policy.packageRoots()).as("resource artifacts cannot own Java packages").isEmpty();
                assertThat(policy.testPackageRoots()).isEmpty();
                assertThat(policy.exports()).isEmpty();
                assertThat(policy.providers()).isEmpty();
                assertThat(ResourceOnlyModuleLayout.violations(ROOT.resolve(policy.path()), policy.resourceRoots()))
                        .as("%s resource-only layout", policy.artifactId()).isEmpty();
            } else {
                assertThat(policy.resourceRoots()).as("resource roots require resourceOnly declaration").isEmpty();
                assertThat(policy.packageRoots())
                        .as("%s must declare at least one package root", policy.artifactId()).isNotEmpty();
            }
            assertThat(policy.exports())
                    .as("%s exports must be declared package roots", policy.artifactId())
                    .allMatch(policy.packageRoots()::contains);
            if (!policy.forbiddenImports().isEmpty()) {
                assertThat(policy.allowedImports())
                        .as("%s allowedImports and forbiddenImports overlap", policy.artifactId())
                        .doesNotContainAnyElementsOf(policy.forbiddenImports());
            }
            assertThat(MODEL.facts().keySet())
                    .as("%s imports an undeclared artifact", policy.artifactId())
                    .containsAll(policy.allowedImports())
                    .containsAll(policy.forbiddenImports());

            for (Map.Entry<String, List<String>> friend : policy.friendExports().entrySet()) {
                assertThat(policy.allowedImports())
                        .as("%s friend export %s must also be an allowed import",
                                policy.artifactId(), friend.getKey())
                        .contains(friend.getKey());
            }
            for (Violation violation : policy.baselineViolations()) {
                assertThat(policy.allowedImports())
                        .as("%s records %s as debt but also allows it", policy.artifactId(), violation.target())
                        .doesNotContain(violation.target());
                assertThat(policy.forbiddenImports())
                        .as("%s records %s as debt but also forbids it", policy.artifactId(), violation.target())
                        .doesNotContain(violation.target());
                assertThat(violation.milestone())
                        .as("%s -> %s must name the milestone that removes the debt",
                                policy.artifactId(), violation.target())
                        .matches("M\\d+");
            }

            for (String root : concat(policy.packageRoots(), policy.testPackageRoots())) {
                rootOwners.computeIfAbsent(root, ignored -> new ArrayList<>()).add(policy.artifactId());
            }
        }
        rootOwners.forEach((root, owners) -> assertThat(owners)
                .as("package root %s is declared by more than one artifact", root)
                .hasSize(1));

        Map<String, List<String>> splitPackages = new TreeMap<>();
        for (Policy policy : MODEL.policies().values()) {
            Set<String> declaredPackages = new HashSet<>(MODEL.mainPackages(policy.artifactId()));
            declaredPackages.addAll(MODEL.testPackages(policy.artifactId()));
            if (policy.resourceOnly()) {
                assertThat(declaredPackages).as("resource-only artifacts must not contain Java source").isEmpty();
            } else {
                assertThat(declaredPackages)
                        .as("%s declares no packages at all", policy.artifactId()).isNotEmpty();
            }
            assertThat(policy.packageRoots())
                    .as("%s packageRoots are not backed by sources", policy.artifactId())
                    .allMatch(root -> declaredPackages.stream().anyMatch(packageName -> isUnder(packageName, root)));

            for (String packageName : MODEL.mainPackages(policy.artifactId())) {
                assertThat(MODEL.ownerOf(packageName))
                        .as("%s declares main package %s outside its packageRoots",
                                policy.artifactId(), packageName)
                        .isEqualTo(policy.artifactId());
                splitPackages.computeIfAbsent(packageName, ignored -> new ArrayList<>())
                        .add(policy.artifactId());
            }
            for (String packageName : MODEL.testPackages(policy.artifactId())) {
                assertThat(MODEL.ownerOf(packageName))
                        .as("%s declares test package %s outside its packageRoots/testPackageRoots",
                                policy.artifactId(), packageName)
                        .isEqualTo(policy.artifactId());
            }
        }
        splitPackages.values().removeIf(owners -> owners.size() == 1);
        assertThat(splitPackages)
                .as("split packages are illegal on a single classpath")
                .isEmpty();
    }

    @Test
    void exportedRootsHavePackageDocumentation() {
        for (Policy policy : MODEL.policies().values()) {
            if (policy.testSurface()) {
                continue;
            }
            for (String root : policy.exports()) {
                Path packageInfo = ROOT.resolve(policy.path())
                        .resolve("src/main/java")
                        .resolve(root.replace('.', '/'))
                        .resolve("package-info.java");
                assertThat(packageInfo)
                        .as("%s exports %s but the package has no package-info.java",
                                policy.artifactId(), root)
                        .isRegularFile();
            }
        }
    }

    @Test
    void mainImportsFollowDeclaredPolicy() {
        assertThat(MODEL.unresolvedImports())
                .as("imports resolve to no declared package root")
                .isEmpty();

        Map<String, Set<String>> violations = new TreeMap<>();
        for (Policy policy : MODEL.policies().values()) {
            Set<String> edges = MODEL.imports(policy.artifactId()).getOrDefault("main", Set.of());
            if (!policy.testSurface()) {
                Set<String> allowed = new HashSet<>(policy.allowedImports());
                policy.baselineViolations().forEach(violation -> allowed.add(violation.target()));

                for (String target : edges) {
                    assertThat(policy.forbiddenImports())
                            .as("%s imports forbidden artifact %s", policy.artifactId(), target)
                            .doesNotContain(target);
                    assertThat(allowed)
                            .as("%s -> %s is not allowed and is not recorded in baselineViolations",
                                    policy.artifactId(), target)
                            .contains(target);
                    if (!policy.allowedImports().contains(target)) {
                        violations.computeIfAbsent(policy.artifactId(), ignored -> new TreeSet<>()).add(target);
                    }
                    assertFriendExport(MODEL, policy, target);
                    assertExportedPackages(MODEL, policy, target);
                }
            }

            assertThat(pomScope(MODEL, policy.artifactId(), edges))
                    .as("%s must declare a compile/runtime/provided POM dependency for every main import",
                            policy.artifactId())
                    .isEmpty();
        }

        Map<String, Set<String>> declared = new TreeMap<>();
        MODEL.policies().values().forEach(policy -> {
            if (!policy.baselineViolations().isEmpty()) {
                declared.put(policy.artifactId(), new TreeSet<>(
                        policy.baselineViolations().stream().map(Violation::target).toList()));
            }
        });
        assertThat(violations)
                .as("baselineViolations must describe exactly the current debt")
                .isEqualTo(declared);
    }

    @Test
    void testImportsUseDeclaredDependencies() {
        for (Policy policy : MODEL.policies().values()) {
            assertTestDependencies(MODEL, policy.artifactId());
        }
    }

    @Test
    void serviceProvidersMatchDeclaredFamilies() {
        for (Policy policy : MODEL.policies().values()) {
            Map<String, List<String>> descriptors = MODEL.providers(policy.artifactId());
            assertThat(providerSets(descriptors))
                    .as("%s ServiceLoader descriptors differ from modules.yaml providers",
                            policy.artifactId())
                    .isEqualTo(providerSets(policy.providers()));

            for (String providerClass : descriptors.values().stream().flatMap(Collection::stream).toList()) {
                String packageName = MODEL.packageOf(providerClass);
                assertThat(packageName)
                        .as("%s provider %s has no declared package", policy.artifactId(), providerClass)
                        .isNotNull();
                assertThat(MODEL.ownerOf(packageName))
                        .as("%s provider %s is owned by another artifact",
                                policy.artifactId(), providerClass)
                        .isEqualTo(policy.artifactId());
                assertThat(ROOT.resolve(policy.path()).resolve("src/main/java")
                        .resolve(providerClass.replace('.', '/') + ".java"))
                        .as("provider class %s must be main source of %s", providerClass, policy.artifactId())
                        .isRegularFile();
            }

            for (String service : descriptors.keySet()) {
                if (!service.startsWith("org.pangrova.")) {
                    continue;
                }
                String servicePackage = MODEL.packageOf(service);
                assertThat(servicePackage)
                        .as("%s registers service %s that no artifact owns", policy.artifactId(), service)
                        .isNotNull();
                String owner = MODEL.ownerOf(servicePackage);
                if (owner != null && !owner.equals(policy.artifactId())) {
                    assertThat(policy.allowedImports())
                            .as("%s registers service %s from non-allowed artifact %s",
                                    policy.artifactId(), service, owner)
                            .contains(owner);
                }
            }
        }
    }

    @Test
    void baselineIsCurrent() {
        assertThat(MODEL.policies().keySet())
                .as("modules.yaml and module-baseline.yaml declare different artifacts")
                .isEqualTo(MODEL.facts().keySet());

        for (Policy policy : MODEL.policies().values()) {
            Facts facts = MODEL.facts().get(policy.artifactId());
            assertThat(facts.path())
                    .as("%s path differs from modules.yaml", policy.artifactId())
                    .isEqualTo(policy.path());
            assertThat(MODEL.mainPackages(policy.artifactId()))
                    .as("%s mainPackages are stale; regenerate the baseline", policy.artifactId())
                    .isEqualTo(new TreeSet<>(facts.mainPackages()));
            assertThat(MODEL.imports(policy.artifactId()))
                    .as("%s imports are stale; regenerate the baseline", policy.artifactId())
                    .isEqualTo(facts.imports());
            assertThat(MODEL.exportedPackages(policy.artifactId()))
                    .as("%s exportedPackages are stale; regenerate the baseline", policy.artifactId())
                    .isEqualTo(new TreeSet<>(facts.exportedPackages()));
            assertThat(providerSets(MODEL.providers(policy.artifactId())))
                    .as("%s providers are stale; regenerate the baseline", policy.artifactId())
                    .isEqualTo(providerSets(facts.providers()));
            assertThat(MODEL.pomDependencies(policy.artifactId()))
                    .as("%s pomDependencies are stale; regenerate the baseline", policy.artifactId())
                    .isEqualTo(facts.pomDependencies());
        }
    }

    static void assertFriendExport(Model model, Policy policy, String target) {
        List<String> friends = policy.friendExports().get(target);
        if (friends == null) {
            return;
        }
        for (String importedPackage : importedPackages(model, policy.artifactId(), target)) {
            assertThat(friends)
                    .as("%s may only import %s from %s; %s", policy.artifactId(), friends, target,
                            model.witness(policy.artifactId(), "main", importedPackage))
                    .anyMatch(friend -> isUnder(importedPackage, friend));
        }
    }

    static void assertExportedPackages(Model model, Policy policy, String target) {
        Policy targetPolicy = model.policies().get(target);
        for (String importedPackage : importedPackages(model, policy.artifactId(), target)) {
            assertThat(targetPolicy.exports())
                    .as("%s imports %s from %s, which is not exported there; %s",
                            policy.artifactId(), importedPackage, target,
                            model.witness(policy.artifactId(), "main", importedPackage))
                    .anyMatch(export -> isUnder(importedPackage, export));
        }
    }

    private static Set<String> importedPackages(Model model, String artifact, String target) {
        Set<String> packages = new TreeSet<>();
        for (String fqcn : model.importedTypes(artifact, "main")) {
            String packageName = model.packageOf(fqcn);
            if (packageName != null && target.equals(model.ownerOf(packageName))) {
                packages.add(packageName);
            }
        }
        return packages;
    }

    static Set<String> pomScope(Model model, String artifact, Set<String> imports) {
        Map<String, String> deps = model.pomDependencies(artifact);
        Set<String> missing = new TreeSet<>();
        for (String target : imports) {
            String scope = deps.get(target);
            if (scope == null || scope.equals("test")) {
                missing.add(target + "(" + (scope == null ? "absent" : scope) + ")");
            }
        }
        return missing;
    }

    static void assertTestDependencies(Model model, String artifact) {
        Set<String> declared = model.pomDependencies(artifact).keySet();
        for (String target : model.imports(artifact).getOrDefault("test", Set.of())) {
            assertThat(declared).as("%s test code references %s without a POM dependency", artifact, target)
                    .contains(target);
        }
    }

    private static Map<String, Set<String>> providerSets(Map<String, List<String>> providers) {
        Map<String, Set<String>> sets = new TreeMap<>();
        providers.forEach((service, classes) -> sets.put(service, new TreeSet<>(classes)));
        return sets;
    }

    private static boolean isUnder(String packageName, String root) {
        return packageName.equals(root) || packageName.startsWith(root + ".");
    }

    private static List<String> concat(Collection<String> first, Collection<String> second) {
        List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    // ------------------------------------------------------------------
    // Model
    // ------------------------------------------------------------------

    record Violation(String target, String milestone, String reason) {}

    record Policy(
            String artifactId,
            String path,
            String moduleName,
            boolean testSurface,
            boolean resourceOnly,
            List<String> resourceRoots,
            List<String> packageRoots,
            List<String> testPackageRoots,
            List<String> exports,
            Set<String> allowedImports,
            Set<String> forbiddenImports,
            Map<String, List<String>> friendExports,
            Map<String, List<String>> providers,
            List<Violation> baselineViolations) {}

    record Facts(
            String path,
            List<String> mainPackages,
            Map<String, String> pomDependencies,
            Map<String, Set<String>> imports,
            List<String> exportedPackages,
            Map<String, List<String>> providers) {}

    static final class Model {

        private final Map<String, Policy> policies;
        private final Map<String, Facts> facts;
        private final Map<String, String> aggregators;
        private final Map<String, Set<String>> mainPackages;
        private final Map<String, Set<String>> testPackages;
        private final Map<String, Map<String, Set<String>>> imports;
        private final Map<String, Map<String, Set<String>>> rawImports;
        private final Map<String, Map<String, Map<String, String>>> witnesses;
        private final Map<String, Map<String, List<String>>> providers;
        private final Map<String, Map<String, String>> pomDependencies;
        private final Map<String, Set<String>> exportedPackages;
        private final Set<String> unresolvedImports;
        private final Map<String, String> discoveredPoms;
        private final Set<String> allPackages;

        private Model(Path root) {
            Map<String, Object> policyYaml = readYaml(root.resolve(".pangrova/modules.yaml"));
            Map<String, Object> baselineYaml = readYaml(root.resolve(".pangrova/module-baseline.yaml"));

            policies = parsePolicies(map(policyYaml.get("artifacts")));
            facts = parseFacts(map(baselineYaml.get("artifacts")));
            aggregators = new TreeMap<>();
            for (Object entry : list(baselineYaml.get("reactor"))) {
                Map<String, Object> aggregator = map(entry);
                aggregators.put(string(aggregator.get("artifactId")), string(aggregator.get("path")));
            }

            mainPackages = new TreeMap<>();
            testPackages = new TreeMap<>();
            imports = new TreeMap<>();
            rawImports = new TreeMap<>();
            witnesses = new TreeMap<>();
            providers = new TreeMap<>();
            pomDependencies = new TreeMap<>();
            exportedPackages = new TreeMap<>();
            unresolvedImports = new TreeSet<>();
            discoveredPoms = discoverPoms(root);

            List<Path> sourceRoots = policies.values().stream().flatMap(policy ->
                    List.of("main", "test").stream().map(kind ->
                            root.resolve(policy.path()).resolve("src/" + kind + "/java"))).toList();
            Map<Path, JavaSourceReferences.SourceFacts> scanned;
            try {
                scanned = JavaSourceReferences.scan(sourceRoots);
            } catch (IOException failure) {
                throw new UncheckedIOException("Cannot inspect architecture sources", failure);
            }
            for (Policy policy : policies.values()) {
                Path module = root.resolve(policy.path());
                mainPackages.put(policy.artifactId(), scanned.get(module.resolve("src/main/java")).packages());
                testPackages.put(policy.artifactId(), scanned.get(module.resolve("src/test/java")).packages());
                Map<String, Set<String>> importedTypes = new TreeMap<>();
                Map<String, Map<String, String>> sourceWitnesses = new TreeMap<>();
                for (String kind : List.of("main", "test")) {
                    var references = scanned.get(module.resolve("src/" + kind + "/java")).references();
                    importedTypes.put(kind, references.keySet());
                    sourceWitnesses.put(kind, references);
                }
                rawImports.put(policy.artifactId(), importedTypes);
                witnesses.put(policy.artifactId(), sourceWitnesses);
                providers.put(policy.artifactId(),
                        scanProviders(module.resolve("src/main/resources/META-INF/services")));
                pomDependencies.put(policy.artifactId(),
                        scanPomDependencies(module.resolve("pom.xml")));
            }

            Set<String> packages = new HashSet<>();
            mainPackages.values().forEach(packages::addAll);
            testPackages.values().forEach(packages::addAll);
            allPackages = Set.copyOf(packages);

            policies.keySet().forEach(id -> exportedPackages.put(id, new TreeSet<>()));
            for (Policy policy : policies.values()) {
                Map<String, Set<String>> edges = new TreeMap<>();
                for (Map.Entry<String, Set<String>> kind : rawImports.get(policy.artifactId()).entrySet()) {
                    Set<String> targets = new TreeSet<>();
                    for (String fqcn : kind.getValue()) {
                        String packageName = packageOf(fqcn);
                        String owner = packageName == null ? null : ownerOf(packageName);
                        if (owner == null) {
                            unresolvedImports.add(fqcn);
                            continue;
                        }
                        if (owner.equals(policy.artifactId())) {
                            continue;
                        }
                        targets.add(owner);
                        exportedPackages.get(owner).add(packageName);
                    }
                    edges.put(kind.getKey(), targets);
                }
                imports.put(policy.artifactId(), edges);
            }
        }

        static Model load(Path root) {
            return new Model(root);
        }

        Map<String, Policy> policies() {
            return policies;
        }

        Map<String, Facts> facts() {
            return facts;
        }

        Map<String, String> declaredPoms() {
            Map<String, String> declared = new TreeMap<>(aggregators);
            policies.values().forEach(policy -> declared.put(policy.artifactId(), policy.path()));
            return declared;
        }

        Map<String, String> discoveredPoms() {
            return discoveredPoms;
        }

        Set<String> mainPackages(String artifact) {
            return mainPackages.getOrDefault(artifact, Set.of());
        }

        Set<String> testPackages(String artifact) {
            return testPackages.getOrDefault(artifact, Set.of());
        }

        Map<String, Set<String>> imports(String artifact) {
            return imports.getOrDefault(artifact, Map.of());
        }

        Set<String> importedTypes(String artifact, String kind) {
            return rawImports.getOrDefault(artifact, Map.of()).getOrDefault(kind, Set.of());
        }

        String witness(String artifact, String kind, String packageName) {
            return new TreeMap<>(witnesses.get(artifact).get(kind)).entrySet().stream()
                    .filter(entry -> packageName.equals(packageOf(entry.getKey())))
                    .map(entry -> policies.get(artifact).path() + "/src/" + kind + "/java/"
                            + entry.getValue() + " -> " + entry.getKey())
                    .findFirst().orElseThrow();
        }

        Set<String> exportedPackages(String artifact) {
            return exportedPackages.getOrDefault(artifact, Set.of());
        }

        Map<String, List<String>> providers(String artifact) {
            return providers.getOrDefault(artifact, Map.of());
        }

        Map<String, String> pomDependencies(String artifact) {
            return pomDependencies.getOrDefault(artifact, Map.of());
        }

        Set<String> unresolvedImports() {
            return unresolvedImports;
        }

        String packageOf(String fqcn) {
            String candidate = fqcn;
            while (candidate.contains(".")) {
                if (allPackages.contains(candidate)) {
                    return candidate;
                }
                candidate = candidate.substring(0, candidate.lastIndexOf('.'));
            }
            return null;
        }

        String ownerOf(String packageName) {
            String owner = null;
            int best = -1;
            for (Policy policy : policies.values()) {
                for (String root : concat(policy.packageRoots(), policy.testPackageRoots())) {
                    if (isUnder(packageName, root) && root.length() > best) {
                        owner = policy.artifactId();
                        best = root.length();
                    }
                }
            }
            return owner;
        }
    }

    // ------------------------------------------------------------------
    // Parsing / scanning
    // ------------------------------------------------------------------

    private static Map<String, Policy> parsePolicies(Map<String, Object> entries) {
        Map<String, Policy> policies = new TreeMap<>();
        entries.forEach((artifactId, raw) -> {
            Map<String, Object> node = map(raw);
            Map<String, List<String>> friends = new TreeMap<>();
            map(node.getOrDefault("friendExports", Map.of())).forEach(
                    (target, packages) -> friends.put(target, strings(packages).stream().toList()));
            List<Violation> violations = new ArrayList<>();
            for (Object entry : list(node.getOrDefault("baselineViolations", List.of()))) {
                Map<String, Object> violation = map(entry);
                violations.add(new Violation(
                        string(violation.get("target")),
                        string(violation.get("milestone")),
                        string(violation.get("reason"))));
            }
            policies.put(artifactId, new Policy(
                    artifactId,
                    string(node.get("path")),
                    string(node.get("moduleName")),
                    Boolean.TRUE.equals(node.get("testSurface")),
                    Boolean.TRUE.equals(node.get("resourceOnly")),
                    List.copyOf(strings(node.getOrDefault("resourceRoots", List.of()))),
                    List.copyOf(strings(node.get("packageRoots"))),
                    List.copyOf(strings(node.getOrDefault("testPackageRoots", List.of()))),
                    List.copyOf(strings(node.get("exports"))),
                    strings(node.get("allowedImports")),
                    strings(node.get("forbiddenImports")),
                    friends,
                    stringLists(node.getOrDefault("providers", Map.of())),
                    violations));
        });
        return policies;
    }

    private static Map<String, Facts> parseFacts(Map<String, Object> entries) {
        Map<String, Facts> facts = new TreeMap<>();
        entries.forEach((artifactId, raw) -> {
            Map<String, Object> node = map(raw);
            Map<String, String> dependencies = new TreeMap<>();
            map(node.get("pomDependencies")).forEach((key, value) ->
                    dependencies.put(key, value == null ? "" : value.toString()));
            Map<String, Set<String>> imports = new TreeMap<>();
            map(node.get("imports")).forEach((kind, targets) ->
                    imports.put(kind, strings(targets)));
            facts.put(artifactId, new Facts(
                    string(node.get("path")),
                    List.copyOf(strings(node.get("mainPackages"))),
                    dependencies,
                    imports,
                    List.copyOf(strings(node.get("exportedPackages"))),
                    stringLists(node.getOrDefault("providers", Map.of()))));
        });
        return facts;
    }

    private static Map<String, String> discoverPoms(Path root) {
        Map<String, String> poms = new TreeMap<>();
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(path -> path.getFileName().toString().equals("pom.xml"))
                    .filter(path -> !path.toString().contains("/target/"))
                    .filter(path -> !path.toString().contains("/node_modules/"))
                    .filter(path -> !path.toString().contains("/.git/"))
                    .forEach(pom -> {
                        String relative = root.relativize(pom.getParent()).toString().replace('\\', '/');
                        poms.put(pomArtifactId(pom), relative.isEmpty() ? "." : relative);
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return poms;
    }

    private static Map<String, String> scanPomDependencies(Path pom) {
        Map<String, String> dependencies = new TreeMap<>();
        Element project = parseXml(pom).getDocumentElement();
        for (Element dependenciesElement : childElements(project, "dependencies")) {
            for (Element dependency : childElements(dependenciesElement, "dependency")) {
                String group = childText(dependency, "groupId");
                String artifact = childText(dependency, "artifactId");
                if (artifact == null) {
                    continue;
                }
                if ((group == null || !group.contains("pangrova")) && !artifact.startsWith("pangrova-")) {
                    continue;
                }
                String scope = childText(dependency, "scope");
                String effective = scope == null ? "compile" : scope;
                dependencies.merge(artifact, effective,
                        (existing, candidate) -> precedence(candidate) >= precedence(existing)
                                ? candidate : existing);
            }
        }
        return dependencies;
    }

    private static int precedence(String scope) {
        return switch (scope) {
            case "compile" -> 4;
            case "runtime" -> 3;
            case "provided" -> 2;
            default -> 1;
        };
    }

    private static String pomArtifactId(Path pom) {
        return childText(parseXml(pom).getDocumentElement(), "artifactId");
    }

    private static String pomModuleName(Path pom) {
        Element project = parseXml(pom).getDocumentElement();
        for (Element properties : childElements(project, "properties")) {
            String moduleName = childText(properties, "module.name");
            if (moduleName != null) {
                return moduleName;
            }
        }
        return null;
    }

    private static Map<String, List<String>> scanProviders(Path serviceDir) {
        Map<String, List<String>> providers = new TreeMap<>();
        if (!Files.isDirectory(serviceDir)) {
            return providers;
        }
        try (Stream<Path> stream = Files.list(serviceDir)) {
            stream.filter(Files::isRegularFile).forEach(descriptor -> {
                try {
                    List<String> classes = Files.readAllLines(descriptor).stream()
                            .map(String::trim)
                            .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                            .toList();
                    providers.put(descriptor.getFileName().toString(), classes);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return providers;
    }

    private static Document parseXml(Path pom) {
        try (InputStream stream = Files.newInputStream(pom)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setExpandEntityReferences(false);
            return factory.newDocumentBuilder().parse(stream);
        } catch (Exception e) {
            throw new IllegalStateException("cannot parse " + pom, e);
        }
    }

    private static List<Element> childElements(Element parent, String name) {
        List<Element> elements = new ArrayList<>();
        var children = parent.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            if (children.item(index) instanceof Element element && name.equals(element.getTagName())) {
                elements.add(element);
            }
        }
        return elements;
    }

    private static String childText(Element parent, String name) {
        List<Element> elements = childElements(parent, name);
        return elements.isEmpty() ? null : elements.get(0).getTextContent().trim();
    }

    private static Map<String, Object> readYaml(Path path) {
        try (InputStream stream = Files.newInputStream(path)) {
            return map(new Yaml().load(stream));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }



    private static Map<String, Object> map(Object value) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (value == null || (value instanceof List<?> entries && entries.isEmpty())) {
            return result;
        }
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("expected mapping but got " + value.getClass());
        }
        raw.forEach((key, entry) -> result.put(key.toString(), entry));
        return result;
    }

    private static List<Object> list(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> entries) {
            return new ArrayList<>(entries);
        }
        return List.of(value);
    }

    private static Set<String> strings(Object value) {
        return new LinkedHashSet<>(list(value).stream().map(Object::toString).toList());
    }

    private static Map<String, List<String>> stringLists(Object value) {
        Map<String, List<String>> result = new TreeMap<>();
        map(value).forEach((key, entry) -> result.put(key, List.copyOf(strings(entry))));
        return result;
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }
}
