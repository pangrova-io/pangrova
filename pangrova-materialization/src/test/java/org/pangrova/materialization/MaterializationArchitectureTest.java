package org.pangrova.materialization;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

class MaterializationArchitectureTest {

    private static final JavaClasses KERNEL = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("org.pangrova.materialization");

    @Test
    void kernelDependsOnlyOnJdkCommonAndSpi() {
        classes()
                .that().resideInAPackage("org.pangrova.materialization..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "org.pangrova.materialization..",
                        "org.pangrova.spi..",
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
                        "org.pangrova.connector..",
                        "org.pangrova.worker..",
                        "org.pangrova.runtime..",
                        "org.pangrova.protocol..")
                .check(KERNEL);
    }

    @Test
    void domainDoesNotDependOnPortsOrExtensions() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.pangrova.materialization.port..",
                        "org.pangrova.materialization.extension..")
                .check(KERNEL);
    }

    @Test
    void portsDoNotDependOnExtensions() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization.port..")
                .should().dependOnClassesThat().resideInAPackage("org.pangrova.materialization.extension..")
                .check(KERNEL);
    }

    @Test
    void extensionsDoNotDependOnPorts() {
        noClasses()
                .that().resideInAPackage("org.pangrova.materialization.extension..")
                .should().dependOnClassesThat().resideInAPackage("org.pangrova.materialization.port..")
                .check(KERNEL);
    }
}
