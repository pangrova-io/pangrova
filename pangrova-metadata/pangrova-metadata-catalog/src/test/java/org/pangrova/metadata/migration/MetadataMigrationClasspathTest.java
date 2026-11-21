package org.pangrova.metadata.migration;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** The real Metadata dependency graph must not retain a second copy of the extracted SQL bundle. */
class MetadataMigrationClasspathTest {
    @Test
    void everyMigrationComesFromExactlyOneResourceBundle() throws Exception {
        ClassLoader loader = getClass().getClassLoader();
        var manifests = Collections.list(loader.getResources("META-INF/pangrova/schema-migrations.tsv"));
        assertThat(manifests).hasSize(1);
        assertThat(Collections.list(loader.getResources("db/migration"))).hasSize(1);
        try (var input = manifests.get(0).openStream()) {
            var paths = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .filter(line -> !line.isBlank() && !line.startsWith("#"))
                    .map(line -> line.split("\t")[2]).toList();
            assertThat(paths).isNotEmpty();
            for (String path : paths) {
                assertThat(Collections.list(loader.getResources(path))).as(path).hasSize(1);
            }
        }
    }
}
