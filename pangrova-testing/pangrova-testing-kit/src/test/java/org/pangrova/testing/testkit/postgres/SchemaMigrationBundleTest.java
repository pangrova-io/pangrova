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
        assertThatThrownBy(() -> loader.loadClass("org.pangrova.persistence.jdbc.migration.SchemaMigrationRunner"))
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

        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("121").load();
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
    void version121DetachesDeletedBuildWithoutDiscardingTenantPin() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("120").load().migrate();
        insertPublishedNativeJob(source, true);
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("121").load().migrate();

        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("DELETE FROM artifact_build_jobs "
                    + "WHERE tenant_id='tenant-a' AND job_id='job-a'")).isOne();
            try (var rows = statement.executeQuery("SELECT tenant_id, job_id, state "
                    + "FROM artifact_snapshot_pins WHERE index_id='artifact-a'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("tenant_id")).isEqualTo("tenant-a");
                assertThat(rows.getString("job_id")).isNull();
                assertThat(rows.getString("state")).isEqualTo("INTENDED");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    @Test
    void version121RejectsPublishedNativeJobsWithoutReplayAuthority() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("120").load().migrate();
        insertPublishedNativeJob(source, false);

        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("121").load();
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

    @Test
    void version122FencesUnboundIndexesOnUpgrade() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("121").load().migrate();
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO index_descriptors (index_id,index_name,catalog_name,schema_name,table_name,"
                        + "column_names,index_type,properties,state,generation,created_at,updated_at,tenant_id) "
                        + "VALUES (?,?,'lake','raw','items','[\"value\"]','BLOOM_FILTER',"
                        + "'{}',?,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'tenant')")) {
            for (String state : List.of("READY", "STALE", "BUILDING")) {
                statement.setString(1, "unbound-" + state.toLowerCase(java.util.Locale.ROOT));
                statement.setString(2, "index-" + state.toLowerCase(java.util.Locale.ROOT));
                statement.setString(3, state);
                statement.executeUpdate();
            }
        }

        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("122").load().migrate();

        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT index_id,state,stale_reason,source_schema_fingerprint "
                        + "FROM index_descriptors WHERE index_id LIKE 'unbound-%' ORDER BY index_id")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("index_id")).isEqualTo("unbound-building");
            assertThat(rows.getString("state")).isEqualTo("BUILDING");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("index_id")).isEqualTo("unbound-ready");
            assertThat(rows.getString("state")).isEqualTo("STALE");
            assertThat(rows.getString("stale_reason")).isEqualTo("source_schema_fingerprint_required");
            assertThat(rows.getString("source_schema_fingerprint")).isNull();
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("index_id")).isEqualTo("unbound-stale");
            assertThat(rows.getString("state")).isEqualTo("STALE");
            assertThat(rows.getString("stale_reason")).isEqualTo("source_schema_fingerprint_required");
            assertThat(rows.next()).isFalse();
        }
    }

    @Test
    void version123RetainsAllUncertainLegacyOperationClaimsOnUpgrade() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("122").load().migrate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO index_schema_evolution_outbox(operation_id,tenant_id,"
                    + "catalog_name,schema_name,table_name,event_type,state,created_at,updated_at) VALUES "
                    + "('00000000-0000-0000-0000-000000000001','tenant-a','lake','raw','items',"
                    + "'DROP_TABLE','INTENDED',now(),now()),"
                    + "('00000000-0000-0000-0000-000000000002','tenant-a','lake','raw','items',"
                    + "'DROP_TABLE','COMMITTED',now(),now())");
        }
        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("123").load();
        assertThat(upgraded.migrate().migrationsExecuted).isOne();
        upgraded.validate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT count(*) FROM index_schema_evolution_outbox "
                    + "WHERE state='UNKNOWN'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM index_schema_operation_claims "
                    + "WHERE catalog_name='lake' AND schema_name='raw' AND table_name='items'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
        }
    }

    @Test
    void version124FencesLegacyBindingsWithoutRewritingPendingOwnership() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("123").load().migrate();
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO index_descriptors(index_id,index_name,catalog_name,schema_name,table_name,"
                        + "column_names,index_type,properties,state,generation,created_at,updated_at,tenant_id,"
                        + "source_schema_fingerprint,stale_reason) VALUES (?,?,'lake','raw',?,"
                        + "'[\"value\"]','BLOOM_FILTER','{}',?,0,?::timestamptz,?::timestamptz,'tenant',?,?)")) {
            for (String state : List.of("READY", "BUILDING", "STALE", "DROPPED")) {
                statement.setString(1, "legacy-" + state);
                statement.setString(2, "index-" + state);
                statement.setString(3, state.equals("STALE") ? "pending" : "items");
                statement.setString(4, state);
                statement.setString(5, "2026-11-26T00:00:00Z");
                statement.setString(6, "2026-11-26T00:00:00Z");
                statement.setString(7, state.equals("BUILDING") ? null : "a".repeat(64));
                statement.setString(8, state.equals("STALE") ? "schema_change_pending" : null);
                statement.executeUpdate();
            }
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO index_generation_builds(index_id,generation,base_generation,"
                    + "source_snapshot,state,started_at,updated_at) VALUES "
                    + "('legacy-READY',1,0,'snapshot-2','BUILDING',now(),now())");
            statement.executeUpdate("INSERT INTO index_schema_evolution_outbox(operation_id,tenant_id,"
                    + "catalog_name,schema_name,table_name,event_type,state,created_at,updated_at) VALUES "
                    + "('00000000-0000-0000-0000-000000000003','tenant','lake','raw','pending',"
                    + "'DROP_TABLE','COMMITTED',now(),now())");
            statement.executeUpdate("INSERT INTO index_schema_scope_locks VALUES ('lake','raw','pending')");
            statement.executeUpdate("INSERT INTO index_schema_operation_claims VALUES "
                    + "('lake','raw','pending','00000000-0000-0000-0000-000000000003')");
            statement.executeUpdate("INSERT INTO index_schema_operation_targets VALUES "
                    + "('00000000-0000-0000-0000-000000000003','legacy-STALE','2026-11-26T00:00:00Z')");
        }
        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("124").load();
        assertThat(upgraded.migrate().migrationsExecuted).isOne();
        upgraded.validate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT state,stale_reason,source_schema_fingerprint "
                    + "FROM index_descriptors WHERE index_id IN ('legacy-READY','legacy-BUILDING') ORDER BY index_id")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("STALE");
                assertThat(rows.getString(2)).isEqualTo("source_schema_fingerprint_v2_required");
                assertThat(rows.getString(3)).isNull();
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("STALE");
                assertThat(rows.getString(2)).isEqualTo("source_schema_fingerprint_v2_required");
                assertThat(rows.getString(3)).isEqualTo("a".repeat(64));
                assertThat(rows.next()).isFalse();
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM index_schema_operation_targets t "
                    + "JOIN index_descriptors d ON d.index_id=t.index_id AND d.updated_at=t.fenced_at "
                    + "WHERE d.state='STALE' AND d.stale_reason='schema_change_pending'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isOne();
            }
            try (var rows = statement.executeQuery("SELECT state,failure_reason FROM index_generation_builds "
                    + "WHERE index_id='legacy-READY'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("FAILED");
                assertThat(rows.getString(2)).isEqualTo("source_schema_fingerprint_v2_required");
            }
            try (var rows = statement.executeQuery("SELECT state FROM index_descriptors WHERE index_id='legacy-DROPPED'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("DROPPED");
            }
            try (var update = connection.prepareStatement(
                    "UPDATE index_descriptors SET source_schema_fingerprint=? WHERE index_id='legacy-STALE'")) {
                update.setString(1, "v2:" + "b".repeat(64));
                assertThat(update.executeUpdate()).isOne();
            }
        }
    }

    @Test
    void version125KeepsExactLegacyReservationAndAllowsOnlyTerminalNativeAttemptHistory() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        // Use the actual immediate predecessor from the canonical bundle, including independently merged V124.
        int predecessor = manifest().stream().mapToInt(entry -> Integer.parseInt(entry[0]))
                .filter(version -> version < 125).max().orElseThrow();
        Flyway.configure().dataSource(source).locations("classpath:db/migration")
                .target(Integer.toString(predecessor)).load().migrate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO index_descriptors
                      (index_id,index_name,catalog_name,schema_name,table_name,column_names,index_type,
                       properties,state,generation,build_snapshot_id,created_at,updated_at,tenant_id)
                    VALUES
                      ('resume','resume','lake','raw','items','["value"]','INVERTED','{}','BUILDING',0,
                       'snapshot','2026-01-02','2026-01-02','tenant-a'),
                      ('superseded','superseded','lake','raw','items','["value"]','INVERTED','{}','BUILDING',0,
                       'snapshot','2026-01-02','2026-01-02','tenant-a'),
                      ('staging','staging','lake','raw','items','["value"]','INVERTED','{}','READY',0,
                       'base','2026-01-01','2026-01-01','tenant-a')
                    """);
            statement.executeUpdate("""
                    INSERT INTO index_generation_builds
                      (index_id,generation,base_generation,source_snapshot,state,started_at,updated_at)
                    VALUES ('staging',1,0,'snapshot','BUILDING','2026-01-02','2026-01-02')
                    """);
            statement.executeUpdate("""
                    INSERT INTO artifact_snapshot_pins
                      (tenant_id,pin_owner,index_id,generation,attempt_id,source_id,source_snapshot,
                       principal,authorization_scope_bytes,state,created_at,updated_at)
                    VALUES
                      ('tenant-a','resume:generation:0:attempt:submission','resume',0,'submission',
                       'lake.raw.items','snapshot','alice',decode('01','hex'),'INTENDED','2026-01-03','2026-01-03'),
                      ('tenant-a','superseded:generation:0:attempt:submission','superseded',0,'submission',
                       'lake.raw.items','snapshot','alice',decode('01','hex'),'INTENDED','2026-01-01','2026-01-01'),
                      ('tenant-a','staging:generation:1:attempt:submission','staging',1,'submission',
                       'lake.raw.items','snapshot','alice',decode('01','hex'),'INTENDED','2026-01-03','2026-01-03')
                    """);
        }
        var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration").target("125").load();
        assertThat(upgraded.migrate().migrationsExecuted).isOne();
        upgraded.validate();
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT index_id,native_submission_attempt_id "
                        + "FROM index_descriptors ORDER BY index_id")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("resume");
            assertThat(rows.getString(2)).isEqualTo("submission");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("staging");
            assertThat(rows.getString(2)).isNotEqualTo("submission");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("superseded");
            assertThat(java.util.UUID.fromString(rows.getString(2))).isNotNull();
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT native_submission_attempt_id "
                    + "FROM index_generation_builds WHERE index_id='staging'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("submission");
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM artifact_snapshot_pins")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(3);
            }
            statement.executeUpdate("""
                    INSERT INTO artifact_snapshot_pins
                      (tenant_id,pin_owner,index_id,generation,attempt_id,source_id,source_snapshot,
                       principal,authorization_scope_bytes,state,created_at,updated_at)
                    SELECT 'tenant-a','superseded:generation:0:attempt:'||native_submission_attempt_id,
                           'superseded',0,native_submission_attempt_id,'lake.raw.items','snapshot',
                           'alice',decode('01','hex'),'INTENDED',now(),now()
                    FROM index_descriptors WHERE index_id='superseded'
                    """);
        }
        insertAttemptJob(source, "native-old", "native-artifact", "TEXT_INDEX", "FAILED");
        insertAttemptJob(source, "native-next", "native-artifact", "TEXT_INDEX", "QUEUED");
        assertThatThrownBy(() -> insertAttemptJob(source, "native-duplicate", "native-artifact",
                "TEXT_INDEX", "QUEUED")).isInstanceOf(java.sql.SQLException.class);
        insertAttemptJob(source, "published", "published-artifact", "VECTOR_INDEX", "PUBLISHED");
        assertThatThrownBy(() -> insertAttemptJob(source, "publication-duplicate", "published-artifact",
                "VECTOR_INDEX", "QUEUED")).isInstanceOf(java.sql.SQLException.class);
        insertAttemptJob(source, "generic-old", "generic-artifact", "GENERATED_VALUE", "FAILED");
        assertThatThrownBy(() -> insertAttemptJob(source, "generic-next", "generic-artifact",
                "GENERATED_VALUE", "QUEUED")).isInstanceOf(java.sql.SQLException.class);
    }

    private static void insertAttemptJob(javax.sql.DataSource source, String jobId, String artifactId,
                                         String artifactType, String state) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                INSERT INTO artifact_build_jobs
                  (tenant_id,job_id,artifact_id,artifact_type,staging_generation,source_id,source_snapshot,
                   staging_uri,format_id,format_major,format_minor,schema_sha256,writer_build_id,
                   idempotency_key,expected_pages,state,created_at,updated_at,
                   publication_expected_updated_at,dependency_set_sha256)
                VALUES ('tenant-a',?,?,?,'0','lake.raw.items','snapshot','s3://warehouse/staging',
                        'fixture-format',1,0,?,'writer-a',?,0,?,now(),now(),now(),?)
                """)) {
            statement.setString(1, jobId);
            statement.setString(2, artifactId);
            statement.setString(3, artifactType);
            statement.setString(4, "a".repeat(64));
            statement.setString(5, jobId);
            statement.setString(6, state);
            statement.setString(7, "b".repeat(64));
            assertThat(statement.executeUpdate()).isOne();
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
