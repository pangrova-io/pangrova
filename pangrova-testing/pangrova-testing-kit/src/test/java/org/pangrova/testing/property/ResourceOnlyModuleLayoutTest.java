package org.pangrova.testing.property;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ResourceOnlyModuleLayoutTest {
    @TempDir Path module;

    @Test
    void acceptsRealResourceOwnerWithoutInventingJavaPackages() throws Exception {
        write("src/main/resources/db/migration/V1__initial.sql", "SELECT 1;");
        assertThat(ResourceOnlyModuleLayout.violations(module, List.of("db/migration"))).isEmpty();
    }

    @Test
    void rejectsMissingAndEscapingRootsAndUndeclaredResources() throws Exception {
        write("src/main/resources/elsewhere/V1__initial.sql", "SELECT 1;");
        assertThat(ResourceOnlyModuleLayout.violations(module, List.of("db/migration", "../outside")))
                .anyMatch(error -> error.contains("missing"))
                .anyMatch(error -> error.contains("escapes"))
                .anyMatch(error -> error.contains("outside declared"));
        assertThat(ResourceOnlyModuleLayout.violations(module, List.of())).isNotEmpty();
    }

    @Test
    void resourceOnlyCannotHideMainTestOrPackagedClassesAndProviders() throws Exception {
        write("src/main/resources/db/migration/V1__initial.sql", "SELECT 1;");
        for (String code : List.of("src/main/java/Hidden.java", "src/test/java/HiddenTest.java",
                "src/main/resources/db/migration/Hidden.class", "src/main/resources/META-INF/services/Hidden")) {
            Path hidden = write(code, "unexpected executable content");
            assertThat(ResourceOnlyModuleLayout.violations(module, List.of("db/migration", "META-INF/services")))
                    .anyMatch(error -> error.contains(hidden.toString()));
            Files.delete(hidden);
        }
    }

    private Path write(String name, String value) throws Exception {
        Path path = module.resolve(name);
        Files.createDirectories(path.getParent());
        return Files.writeString(path, value);
    }
}
