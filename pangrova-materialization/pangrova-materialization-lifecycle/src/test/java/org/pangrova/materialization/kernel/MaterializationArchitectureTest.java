package org.pangrova.materialization.kernel;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

class MaterializationArchitectureTest {

    private static final JavaClasses KERNEL = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("org.pangrova.materialization");

    private static final String[] EXTENSIONS = {
            "org.pangrova.materialization.api.definition..", "org.pangrova.materialization.api.policy..",
            "org.pangrova.materialization.api.refresh..", "org.pangrova.materialization.api.rewrite.."};

    /** Connector implementation packages are owned outside the connector API. */
    private static final DescribedPredicate<JavaClass> CONNECTOR_IMPLEMENTATION =
            DescribedPredicate.describe("connector implementation packages",
                    type -> type.getPackageName().startsWith("org.pangrova.connector.")
                            && !type.getPackageName().startsWith("org.pangrova.connector.api"));

    @Test
    void kernelDependsOnlyOnJdkCommonAndContractPackages() {
        classes()
                .that().resideInAPackage("org.pangrova.materialization..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "org.pangrova.materialization..",
                        "org.pangrova.persistence.materialization.api..",
                        "org.pangrova.connector.api..",
                        "org.pangrova.common..")
                .check(KERNEL);
    }

    @Test
    void kernelNeverDependsOnPlannerMetadataCoordinatorsConnectorsOrDataPlaneFormats() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.apache.calcite..",
                        "java.sql..",
                        "javax.sql..",
                        "com.google.protobuf..",
                        "org.pangrova.coordinator..",
                        "org.pangrova.planner..",
                        "org.pangrova.metadata..",
                        "org.pangrova.worker..",
                        "org.pangrova.runtime..",
                        "org.pangrova.protocol..")
                .check(KERNEL);

        noClasses()
                .that().resideInAPackage("org.pangrova.materialization..")
                .should().dependOnClassesThat(CONNECTOR_IMPLEMENTATION)
                .check(KERNEL);
    }

    @Test
    void domainDoesNotDependOnPortsOrExtensions() {
        classes()
                .that().resideInAPackage("org.pangrova.materialization.api.domain..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..", "org.pangrova.common..", "org.pangrova.connector.api..",
                        "org.pangrova.materialization.api.domain..", "org.pangrova.materialization.api.enums..",
                        "org.pangrova.materialization.api.exception..")
                .check(KERNEL);
    }

    @Test
    void publicContractsDoNotDependOnPersistenceOrRegistry() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization.api..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.pangrova.persistence..", "org.pangrova.materialization..",
                        "org.pangrova.materialization.usecase..")
                .check(KERNEL);
    }

    @Test
    void portsDoNotDependOnExtensions() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization.api.port..")
                .should().dependOnClassesThat().resideInAnyPackage(EXTENSIONS)
                .check(KERNEL);
    }

    @Test
    void extensionsDoNotDependOnPorts() {
        noClasses()
                .that().resideInAnyPackage(EXTENSIONS)
                .should().dependOnClassesThat().resideInAPackage("org.pangrova.materialization.api.port..")
                .check(KERNEL);
    }
}
