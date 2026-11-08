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

    /** Connector implementations; the connector-spi contract package stays allowed. */
    private static final DescribedPredicate<JavaClass> CONNECTOR_IMPLEMENTATION =
            DescribedPredicate.describe("connector implementation packages",
                    type -> type.getPackageName().startsWith("org.pangrova.connector.")
                            && !type.getPackageName().startsWith("org.pangrova.connector.api")
                            && !type.getPackageName().startsWith("org.pangrova.connector.spi"));

    @Test
    void kernelDependsOnlyOnJdkCommonAndConnectorSpi() {
        classes()
                .that().resideInAPackage("org.pangrova.materialization..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "org.pangrova.materialization..",
                        "org.pangrova.connector.api..",
                        "org.pangrova.connector.spi..",
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
