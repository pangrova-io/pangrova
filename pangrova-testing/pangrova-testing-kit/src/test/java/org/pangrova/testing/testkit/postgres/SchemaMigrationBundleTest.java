package org.pangrova.testing.testkit.postgres;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
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
        assertThatThrownBy(() -> loader.loadClass("org.pangrova.persistence.jdbc.support.SchemaMigrationRunner"))
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

    @Test
    void version121AdoptsPublishedNativeJobsWithPersistedAuthority() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("120").load().migrate();
        insertPublishedNativeJob(source, true);

        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").load();
        assertThat(upgraded.migrate().migrationsExecuted).isOne();
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT state,principal,job_id,attempt_id "
                        + "FROM artifact_snapshot_pins WHERE tenant_id='tenant-a' AND index_id='artifact-a'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("state")).isEqualTo("INTENDED");
            assertThat(rows.getString("principal")).isEqualTo("alice");
            assertThat(rows.getString("job_id")).isEqualTo("job-a");
            assertThat(rows.getString("attempt_id")).startsWith("legacy-");
            assertThat(rows.next()).isFalse();
        }
    }

    @Test
    void version121RejectsPublishedNativeJobsWithoutReplayAuthority() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("120").load().migrate();
        insertPublishedNativeJob(source, false);

        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").load();
        assertThatThrownBy(upgraded::migrate)
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("must be resubmitted before V121");
    }

    private static void insertPublishedNativeJob(javax.sql.DataSource source, boolean scoped) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                        + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,"
                        + "format_minor,schema_sha256,writer_build_id,idempotency_key,expected_pages,state,"
                        + "created_at,updated_at,publication_expected_updated_at,dependency_set_sha256) "
                        + "VALUES ('tenant-a','job-a','artifact-a','TEXT_INDEX','0','lake.raw.items',"
                        + "'snapshot-a','s3://warehouse/staging','pangrova.bm25.segment',1,0,"
                        + "?,'writer-a','job-a',0,'PUBLISHED',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,"
                        + "CURRENT_TIMESTAMP,?)")) {
            statement.setString(1, "a".repeat(64));
            statement.setString(2, "b".repeat(64));
            statement.executeUpdate();
        }
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO artifact_build_dispatch_specs (tenant_id,job_id,stage_graph_bytes,"
                        + "stage_graph_sha256,principal,max_memory_bytes,max_threads,max_spill_bytes,"
                        + "task_timeout_millis,authorization_scope_bytes) "
                        + "VALUES ('tenant-a','job-a',decode('00','hex'),?,'alice',1024,1,1024,60000,?)")) {
            statement.setString(1, "c".repeat(64));
            if (scoped) {
                statement.setBytes(2, new byte[] {1});
            } else {
                statement.setNull(2, java.sql.Types.BINARY);
            }
            statement.executeUpdate();
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
