package org.pangrova.testing.testkit.postgres;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.net.JarURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaMigrationBundleTest extends ContainerTestBase {
    private static final String MANIFEST = "META-INF/pangrova/schema-migrations.tsv";

    @Test
    void standaloneBundleHasExactlyTheOriginalBytesAndNoRuntimeImplementation() throws Exception {
        ClassLoader loader = getClass().getClassLoader();
        assertThatThrownBy(() -> loader.loadClass("org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner"))
                .isInstanceOf(ClassNotFoundException.class);
        assertThat(Collections.list(loader.getResources(MANIFEST))).hasSize(1);
        Set<String> resources = new HashSet<>();
        Set<Integer> versions = new HashSet<>();
        for (String[] entry : manifest()) {
            assertThat(versions.add(Integer.parseInt(entry[0]))).isTrue();
            assertThat(resources.add(entry[2])).isTrue();
            assertThat(entry[2]).startsWith("db/migration/V" + entry[0] + "__");
            var copies = Collections.list(loader.getResources(entry[2]));
            assertThat(copies).as("unique migration %s", entry[2]).hasSize(1);
            try (var input = copies.get(0).openStream()) {
                assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.readAllBytes())))
                        .as(entry[2]).isEqualTo(entry[1]);
            }
        }
        var roots = Collections.list(loader.getResources("db/migration"));
        assertThat(roots).hasSize(1);
        var root = roots.get(0);
        Set<String> packaged = new HashSet<>();
        var resourceConnection = root.openConnection();
        resourceConnection.setUseCaches(false);
        if (resourceConnection instanceof JarURLConnection connection) {
            try (var jar = connection.getJarFile()) {
                var entries = jar.stream().filter(entry -> !entry.isDirectory()).map(entry -> entry.getName()).toList();
                assertThat(entries).noneMatch(name -> name.endsWith(".class"));
                entries.stream().filter(name -> name.startsWith("db/migration/")).forEach(packaged::add);
            }
        } else {
            Path directory = Path.of(root.toURI());
            try (var classes = Files.walk(directory.getParent().getParent())) {
                assertThat(classes.filter(Files::isRegularFile).map(path -> path.getFileName().toString()))
                        .noneMatch(name -> name.endsWith(".class"));
            }
            try (var files = Files.walk(directory)) {
                files.filter(Files::isRegularFile).forEach(path ->
                        packaged.add("db/migration/" + directory.relativize(path).toString().replace('\\', '/')));
            }
        }
        assertThat(packaged).containsExactlyInAnyOrderElementsOf(resources);
    }

    @Test
    void freshDatabaseAppliesEveryVersionAndSecondRunIsANoop() throws Exception {
        var source = MigratedPostgres.create();
        var flyway = Flyway.configure().dataSource(source).locations("classpath:db/migration").load();
        flyway.validate();
        assertThat(flyway.info().applied()).hasSize(manifest().size());
        assertThat(flyway.info().pending()).isEmpty();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
    }

    @Test
    void existingHistoryAndCatalogDataSurviveUpgradeFromVersion60() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        var old = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("60").load();
        old.migrate();
        var checksums = java.util.Arrays.stream(old.info().applied())
                .collect(java.util.stream.Collectors.toMap(info -> info.getVersion().toString(), info -> info.getChecksum()));
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO catalog_definitions (name,type,provider,properties) "
                    + "VALUES ('preserved-catalog','PHYSICAL','iceberg','{}'::jsonb)");
        }
        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").load();
        assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(manifest().size() - checksums.size());
        upgraded.validate();
        for (var migration : upgraded.info().applied()) {
            if (checksums.containsKey(migration.getVersion().toString())) {
                assertThat(migration.getChecksum()).isEqualTo(checksums.get(migration.getVersion().toString()));
            }
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT provider FROM catalog_definitions "
                        + "WHERE name='preserved-catalog'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("iceberg");
            assertThat(rows.next()).isFalse();
        }
    }

    private List<String[]> manifest() throws Exception {
        try (var input = getClass().getClassLoader().getResourceAsStream(MANIFEST)) {
            assertThat(input).isNotNull();
            var entries = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .filter(line -> !line.isBlank() && !line.startsWith("#"))
                    .map(line -> line.split("\t", -1)).toList();
            assertThat(entries).isNotEmpty().allSatisfy(entry -> assertThat(entry).hasSize(3));
            return entries;
        }
    }
}
