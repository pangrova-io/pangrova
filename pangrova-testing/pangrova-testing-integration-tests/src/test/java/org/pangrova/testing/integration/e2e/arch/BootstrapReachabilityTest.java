package org.pangrova.testing.integration.e2e.arch;

import org.pangrova.scheduler.admission.StageScheduler;
import org.pangrova.scheduler.admission.FairScheduler;


import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that every concrete {@code *Service}, {@code *Coordinator}, and
 * {@code *Scheduler} class in production code is reachable — via field
 * references, constructor parameters, method signatures, or direct
 * dependencies or an actual {@code ServiceLoader} declaration — from at least
 * one {@code *BootstrapMain} / {@code *Main} entry point.
 *
 * <p>This is the structural twin of {@link SpiWiringVerificationTest}: it
 * targets the long stateful collaborator classes (services, coordinators,
 * schedulers) that should be wired into the running process. An orphan in
 * this set indicates dead wiring: a service that compiles but is never
 * started, or whose start path was deleted leaving the class behind.</p>
 *
 * <p>Classes annotated with {@code @DeadCode} or {@code @ExperimentalImplementation} are
 * excluded. Known pre-existing orphans are baselined for incremental
 * cleanup — the test fails only on NEW orphans.</p>
 *
 * <p>To refresh the baseline after intentionally adding/removing a class,
 * run the test once, copy the printed list into {@link #BASELINED_ORPHANS},
 * and link a tracking issue in the comment for each entry.</p>
 */
@DisplayName("ArchUnit — Bootstrap Reachability of Services / Coordinators / Schedulers")
class BootstrapReachabilityTest {

    /** Top-level process entry points whose dependency graph defines "reachable". */
    private static final List<String> BOOTSTRAP_MAINS = List.of(
            "org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain",
            "org.pangrova.server.worker.bootstrap.PangrovaWorkerMain",
            "org.pangrova.shuffle.deployment.PangrovaShuffleServer"
    );

    private static final String DEAD_CODE_ANNOTATION =
            "org.pangrova.common.annotation.DeadCode";
    private static final String EXPERIMENTAL_ANNOTATION =
            "org.pangrova.common.annotation.ExperimentalImplementation";

    private static final String PANGROVA_PKG_PREFIX = "org.pangrova.";
    private static final String TESTING_PKG_PREFIX = "org.pangrova.testing.";

    /**
     * Suffixes that mark stateful collaborator classes which must be
     * wired into a running process. New suffixes can be added here as
     * the architecture evolves.
     */
    private static final Set<String> TARGET_SUFFIXES = Set.of(
            "Service", "Coordinator", "Scheduler"
    );

    /**
     * Baselined orphans pending wiring or removal. Each entry should be
     * traceable to a follow-up issue. New entries require explicit
     * justification in the PR description.
     *
     * <p>Empty initially: the gate starts in observe-only mode for the
     * first CI run, at which point the list of current orphans is printed
     * and the maintainer either (a) wires the class, (b) annotates it with
     * {@code @DeadCode}, or (c) adds it here with a tracking issue.</p>
     */
    private static final Set<String> BASELINED_ORPHANS = Set.of(
            // Embedding façade entry point for embedded deployments; it is a public
            // API surface, not a process started by a BootstrapMain.
            "org.pangrova.server.coordinator.embedding.EmbeddedCoordinator",
            "org.pangrova.coordinator.clustering.ClusteringScheduler",
            "org.pangrova.scheduler.admission.StageScheduler",
            "org.pangrova.scheduler.admission.FairScheduler",
            "org.pangrova.governance.runtime.classification.CatalogTagService",
            "org.pangrova.governance.runtime.lineage.LineageQueryService",
            "org.pangrova.metadata.relation.ObjectDependencyService",
            "org.pangrova.metadata.service.ModelCatalogService",
            "org.pangrova.observability.logging.MdcPropagatingExecutorService",
            "org.pangrova.planner.adaptive.StageReplanCoordinator",
            "org.pangrova.worker.resource.memory.HeartbeatCrossDomainMemoryCoordinator",
            "org.pangrova.security.tenant.QuotaAlertService",
            "org.pangrova.streaming.commit.TwoPhaseCommitCoordinator"
    );

    private static JavaClasses importedClasses;

    @BeforeAll
    static void importClasses() {
        importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.pangrova");
    }

    @Test
    @DisplayName("Entry-point bootstrap mains exist on the classpath")
    void entryPointsExist() {
        Set<String> classNames = importedClasses.stream()
                .map(JavaClass::getName)
                .collect(Collectors.toSet());

        for (String entry : BOOTSTRAP_MAINS) {
            assertThat(classNames)
                    .as("bootstrap main %s must be on classpath", entry)
                    .contains(entry);
        }
    }

    @Test
    @DisplayName("No new orphan Service / Coordinator / Scheduler classes")
    void noNewOrphans() {
        Set<String> reachable = computeReachableClasses();

        List<String> orphans = importedClasses.stream()
                .filter(BootstrapReachabilityTest::isCandidate)
                .map(JavaClass::getName)
                .filter(name -> !reachable.contains(name))
                .filter(name -> !BASELINED_ORPHANS.contains(name))
                .sorted()
                .collect(Collectors.toList());

        if (!orphans.isEmpty()) {
            System.out.println(
                    "Unreachable Service/Coordinator/Scheduler classes "
                    + "(wire them, annotate with @DeadCode, or add to "
                    + "BASELINED_ORPHANS with a tracking issue):");
            orphans.forEach(c -> System.out.println("  " + c));
        }

        assertThat(orphans)
                .as("New *Service / *Coordinator / *Scheduler classes must be "
                        + "reachable from a BootstrapMain. Either wire into the "
                        + "production startup path, annotate with "
                        + "@DeadCode(issueReference=...), or add to "
                        + "BASELINED_ORPHANS in this test with a tracking issue.")
                .isEmpty();
    }

    @Test
    @DisplayName("Baselined orphans have not been silently resolved")
    void baselinedOrphansStillOrphans() {
        Set<String> reachable = computeReachableClasses();

        List<String> resolved = BASELINED_ORPHANS.stream()
                .filter(reachable::contains)
                .sorted()
                .collect(Collectors.toList());

        if (!resolved.isEmpty()) {
            System.out.println(
                    "INFO: the following baselined orphans are now reachable "
                    + "and should be removed from BASELINED_ORPHANS:");
            resolved.forEach(c -> System.out.println("  " + c));
        }
    }

    private static boolean isCandidate(JavaClass cls) {
        if (cls.isInterface() || cls.isEnum() || cls.isAnnotation()) {
            return false;
        }
        if (cls.isMemberClass() || cls.isAnonymousClass() || cls.isLocalClass()) {
            return false;
        }
        if (cls.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.ABSTRACT)) {
            return false;
        }
        if (cls.isAnnotatedWith(DEAD_CODE_ANNOTATION)
                || cls.isAnnotatedWith(EXPERIMENTAL_ANNOTATION)) {
            return false;
        }
        String name = cls.getName();
        if (!name.startsWith(PANGROVA_PKG_PREFIX) || name.startsWith(TESTING_PKG_PREFIX)) {
            return false;
        }
        String simple = cls.getSimpleName();
        return TARGET_SUFFIXES.stream().anyMatch(simple::endsWith);
    }

    private Set<String> computeReachableClasses() {
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new ArrayDeque<>(BOOTSTRAP_MAINS);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (!visited.add(current)) {
                continue;
            }

            ServiceLoaderReachability.enqueueProviders(
                    current, importedClasses, queue, visited);

            importedClasses.stream()
                    .filter(c -> c.getName().equals(current))
                    .findFirst()
                    .ifPresent(javaClass -> {
                        javaClass.getFields().forEach(field ->
                                enqueueIfPangrova(queue, visited,
                                        field.getRawType().getName()));

                        javaClass.getConstructors().forEach(ctor ->
                                ctor.getRawParameterTypes().forEach(param ->
                                        enqueueIfPangrova(queue, visited,
                                                param.getName())));

                        javaClass.getMethods().forEach(method -> {
                            enqueueIfPangrova(queue, visited,
                                    method.getRawReturnType().getName());
                            method.getRawParameterTypes().forEach(param ->
                                    enqueueIfPangrova(queue, visited,
                                            param.getName()));
                        });

                        javaClass.getDirectDependenciesFromSelf().forEach(dep ->
                                enqueueIfPangrova(queue, visited,
                                        dep.getTargetClass().getName()));
                    });
        }

        return visited;
    }

    private static void enqueueIfPangrova(
            Queue<String> queue, Set<String> visited, String className) {
        if (className.startsWith(PANGROVA_PKG_PREFIX) && !visited.contains(className)) {
            queue.add(className);
        }
    }
}
