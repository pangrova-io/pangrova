package org.pangrova.coordinator.generation;

import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;
import org.pangrova.generation.publication.enums.GenerationSourceCoverage;
import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.coordinator.generation.read.GeneratedValueGenerationBindingResolver;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.graph.GenerationStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildDescriptor;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.generation.execution.template.FrozenStageBindings;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.persistence.generation.postgresql.publication.JdbcGeneratedValuePublicationStore;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedValuePublicationIntegrationTest {

    private org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    private static final Instant START = Instant.parse("2026-09-13T00:00:00Z");
    private static final String SOURCE_ID = "lake.sales.documents";
    private static final String SNAPSHOT = "branch/main@snapshot-41";
    private static final String ARTIFACT_ID = "generated-values-test";
    private static final String GENERATION = "generation-a";
    private static final String JOB_ID = "generated-values-job-a";
    private static final String CHECKSUM = "42".repeat(32);
    private static final String TENANT = "tenant-a";

    @Test
    void atomicallyPublishesSidecarAndBindsOnlyTheTaskSourceObject() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "generated_value_publication");
        PostgresGenerationJobStore buildStore = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "alice", dispatch().authorizationScope());
        JdbcGeneratedValuePublicationStore publicationStore =
                ModelAuthorityFixtures.artifacts().generatedPublications(dataSource);
        publicationStore.register(new GeneratedValueBuildDescriptor(
                TENANT, ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));

        DataSplit split = DataSplit.newBuilder()
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "iceberg", SOURCE_ID, SNAPSHOT, "object-a",
                        "s3://warehouse/object-a.parquet", 0, 0))
                .setConnectorId("iceberg")
                .setSourceId(SOURCE_ID)
                .setSourceSnapshot(SNAPSHOT)
                .setDataObjectId("object-a")
                .setOpaqueLocation("s3://warehouse/object-a.parquet")
                .build();
        publishBuild(buildStore, publicationStore, split);
        assertPublication(buildStore, publicationStore);
        assertTaskBinding(buildStore, publicationStore, split);
        assertWindowMaterialization(dataSource, buildStore, publicationStore, split);
        assertReadLocksShareAndStillWaitForWriters(dataSource, buildStore);
        assertClaimConnectionEvidence(dataSource, buildStore, publicationStore, split);
    }

    private static void assertReadLocksShareAndStillWaitForWriters(DataSource dataSource,
            PostgresGenerationJobStore store) throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var holder = dataSource.getConnection()) {
            holder.setAutoCommit(false);
            lockPublishedJob(holder, false);
            try {
                var reader = executor.submit(() -> store.loadPublishedOutputs(TENANT, JOB_ID, Set.of("object-a")));
                assertThat(reader.get(5, TimeUnit.SECONDS)).hasSize(2);
            } finally {
                holder.rollback();
            }

            int writer = lockPublishedJob(holder, true);
            var reader = executor.submit(() -> store.loadPublishedSourceSplits(TENANT, JOB_ID, Set.of("object-a")));
            try {
                assertWaitsForWriter(dataSource, writer, reader);
            } finally {
                holder.rollback();
            }
            assertThat(reader.get(5, TimeUnit.SECONDS)).containsOnlyKeys("object-a");
        } finally {
            executor.shutdownNow();
        }
    }

    private static int lockPublishedJob(java.sql.Connection connection, boolean writer) throws Exception {
        String sql = "SELECT pg_backend_pid() FROM artifact_build_jobs WHERE tenant_id=? AND job_id=?"
                + (writer ? " FOR UPDATE" : " FOR SHARE");
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, TENANT);
            statement.setString(2, JOB_ID);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getInt(1);
            }
        }
    }

    private static void assertWaitsForWriter(DataSource dataSource, int writer,
            java.util.concurrent.Future<?> reader) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean blocked = false;
        while (!blocked && !reader.isDone() && System.nanoTime() < deadline) {
            blocked = readerWaitsFor(dataSource, writer);
            if (!blocked) Thread.sleep(10);
        }
        assertThat(blocked).isTrue();
        assertThat(reader.isDone()).isFalse();
    }

    private static boolean readerWaitsFor(DataSource dataSource, int writer) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))
                  AND wait_event_type = 'Lock' AND query LIKE 'SELECT%FROM artifact_build_jobs%')
                """)) {
            statement.setInt(1, writer);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getBoolean(1);
            }
        }
    }

    private static void assertWindowMaterialization(javax.sql.DataSource dataSource, PostgresGenerationJobStore store,
            JdbcGeneratedValuePublicationStore publicationStore, DataSplit split) throws Exception {
        var second = secondSplit(split);
        var resolution = new GeneratedValueGenerationBindingResolver(publicationStore, store)
                .resolveTemplate(TENANT, SOURCE_ID, "embedding", SNAPSHOT, List.of(split, second)).orElseThrow();
        var bindings = List.of(resolution.bind("scan-a"), resolution.bind("scan-b"));
        var resolver = new PublishedGenerationBindingResolver(store);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        }
        for (int trial = 0; trial < 3; trial++) {
            long before = materializationReads(dataSource);
            var partitions = List.of(List.of(split), List.of(second));
            var window = resolver.materializeForTasks(TENANT, bindings, partitions);
            assertThat(window).hasSize(2);
            for (int partition = 0; partition < window.size(); partition++) {
                var actual = partitions.get(partition).get(0);
                var results = window.get(partition);
                assertThat(results).extracting(OperatorBinding::getPlanNodeId).containsExactly("scan-a", "scan-b");
                for (var binding : results) {
                    var artifact = binding.getArtifacts(0);
                    PublishedGenerationBindingChecksum.verify(artifact);
                    assertThat(artifact.getSplitObjectsList()).hasSize(2)
                            .extracting(object -> object.getSourceObjectId()).containsOnly(actual.getDataObjectId());
                }
            }
            long delta = materializationReads(dataSource) - before;
            assertThat(delta).as("publication SELECTs per bounded window").isEqualTo(5);
        }
    }

    private static DataSplit secondSplit(DataSplit split) {
        return split.toBuilder()
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "iceberg", SOURCE_ID, SNAPSHOT, "object-b", "s3://warehouse/object-b.parquet", 0, 0))
                .setDataObjectId("object-b").setOpaqueLocation("s3://warehouse/object-b.parquet").build();
    }

    private static long materializationReads(javax.sql.DataSource dataSource) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT COALESCE(SUM(calls), 0)
                FROM public.pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query LIKE 'SELECT%'
                  AND (query LIKE '%FROM artifact_build_jobs%'
                    OR query LIKE '%FROM artifact_build_dependencies%'
                    OR query LIKE '%FROM artifact_build_outputs%')
                  AND query NOT LIKE '%pg_stat_statements%'
                """); var rows = statement.executeQuery()) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private void assertClaimConnectionEvidence(javax.sql.DataSource dataSource, PostgresGenerationJobStore store,
            JdbcGeneratedValuePublicationStore publicationStore, DataSplit split) throws Exception {
        var partitions = List.of(split, secondSplit(split));
        var read = new GeneratedValueGenerationBindingResolver(publicationStore, store).resolveTemplate(
                TENANT, SOURCE_ID, "embedding", SNAPSHOT, partitions).orElseThrow().bind("scan-claim");
        var original = dispatch();
        var sink = OperatorBindingSet.parseFrom(original.stageGraph().requireStage(0).bindingSetBytes())
                .getBindings(0);
        var output = sink.getOutputArtifact().toBuilder().setArtifactId("consumer-artifact")
                .setGeneration("consumer-generation").setStagingUri("s3://warehouse/consumer-generation").build();
        byte[] plan = original.stageGraph().requireStage(0).planBytes();
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(plan,
                List.of(read, sink.toBuilder().setOutputArtifact(output).build())), List.of());
        var stage = GenerationStageGraph.StagePlan.of(0, partitions.size(), GenerationPageKind.SOURCE_MAP,
                plan, template, List.of(SOURCE_ID), List.of("embedding"), true);
        var graph = new GenerationStageGraph(2, GenerationGraphPurpose.ARTIFACT_GENERATION,
                List.of(stage), List.of(), List.of());
        String consumerJob = "same-connection-consumer";
        var spec = GenerationDispatchSpecFactory.create(consumerJob, graph, TENANT, "alice", original.authorizationScope(),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        var inputSplits = partitions.stream()
                .map(value -> GenerationSplit.of(value.getDataObjectId(), value.toByteArray())).toList();
        store.createJob(consumerJob(consumerJob), spec, java.util.stream.IntStream.range(0, partitions.size())
                .mapToObj(i -> new GenerationPage(consumerJob, "consumer-page-" + i,
                        0, i, i, GenerationPageKind.SOURCE_MAP, List.of(inputSplits.get(i)), List.of(), 3)).toList());
        var owner = executions.takeOwnership(TENANT, consumerJob, "consumer-owner", 10);
        try (var pool = new com.zaxxer.hikari.HikariDataSource()) {
            pool.setDataSource(dataSource);
            pool.setMaximumPoolSize(1);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(500);
            var key = new org.pangrova.generation.api.identity.GenerationJobKey(TENANT, consumerJob);
            var claims = java.util.stream.IntStream.range(0, partitions.size())
                    .mapToObj(i -> new GenerationStageExecutionStore.PartitionClaim(key, owner,
                            0, i, i, 10, Duration.ofSeconds(30), START)).toList();
            var stages = ModelAuthorityFixtures.artifacts().stages(pool);
            replaceConsumerSplit(dataSource, consumerJob, GenerationSplit.of("object-b", new byte[]{1}));
            assertThatThrownBy(() -> stages.preparePartitions(claims, prepared -> {
                throw new AssertionError("Corrupt publication inputs must block the whole cohort");
            })).isInstanceOf(IllegalStateException.class).hasMessageContaining("source allocation differs");
            replaceConsumerSplit(dataSource, consumerJob, inputSplits.get(1));

            long guardBefore = publicationSelects(dataSource, "SELECT state FROM artifact_build_jobs%FOR SHARE");
            String manifestSql = "SELECT * FROM artifact_build_jobs%staging_generation=%FOR SHARE";
            String sourceOutputSql = "SELECT object_id%FROM artifact_build_outputs%source_object_id IN (%";
            String generationOutputSql = "SELECT object_id%FROM artifact_build_outputs%ORDER BY object_ordinal,object_id";
            String checksumSql = "SELECT source_object_id,split_sha256 FROM artifact_build_page_sources%";
            long manifestBefore = publicationSelects(dataSource, manifestSql);
            long sourceOutputBefore = publicationSelects(dataSource, sourceOutputSql);
            long generationOutputBefore = publicationSelects(dataSource, generationOutputSql);
            long checksumBefore = publicationSelects(dataSource, checksumSql);
            var executor = Executors.newSingleThreadExecutor();
            try (var holder = dataSource.getConnection()) {
                holder.setAutoCommit(false);
                int writer = lockPublishedJob(holder, true);
                var readClaim = executor.submit(() -> stages.preparePartitions(claims, prepared -> {
                    assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isEqualTo(1);
                    int partition = prepared.lease().partitionId();
                    var taskSplit = partitions.get(partition);
                    assertThat(prepared.lease().splits()).containsExactly(inputSplits.get(partition));
                    var result = PublishedGenerationBindingResolver.materializePrepared(
                            List.of(read), List.of(taskSplit), prepared.publications());
                    var artifact = result.get(0).getArtifacts(0);
                    PublishedGenerationBindingChecksum.verify(artifact);
                    assertThat(artifact.getSplitObjectsList()).hasSize(2)
                            .extracting(object -> object.getSourceObjectId()).containsOnly(taskSplit.getDataObjectId());
                    assertThat(prepared.publications()).hasSize(1);
                    assertThat(prepared.dependencyOutputs()).isEmpty();
                    return prepared.lease();
                }));
                try {
                    assertWaitsForWriter(dataSource, writer, readClaim);
                } finally {
                    holder.rollback();
                }
                assertThat(readClaim.get(5, TimeUnit.SECONDS)).hasSize(partitions.size())
                        .allSatisfy(lease -> assertThat(lease.attemptId()).isZero());
            } finally {
                executor.shutdownNow();
            }
            assertThat(publicationSelects(dataSource, "SELECT state FROM artifact_build_jobs%FOR SHARE") - guardBefore)
                    .as("separate publication guard queries in the claim transaction").isZero();
            assertThat(publicationSelects(dataSource, manifestSql) - manifestBefore)
                    .as("locked published manifest queries in the claim transaction").isOne();
            assertThat(publicationSelects(dataSource, sourceOutputSql) - sourceOutputBefore).isOne();
            assertThat(publicationSelects(dataSource, generationOutputSql) - generationOutputBefore).isOne();
            assertThat(publicationSelects(dataSource, checksumSql) - checksumBefore).isOne();
            assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isZero();
        }
    }

    private static void replaceConsumerSplit(DataSource source, String jobId, GenerationSplit split) throws Exception {
        try (var connection = source.getConnection(); var update = connection.prepareStatement("""
                UPDATE artifact_build_page_sources SET split_bytes=?, split_sha256=?
                WHERE tenant_id=? AND job_id=? AND page_id='consumer-page-1' AND source_object_id='object-b'
                """)) {
            update.setBytes(1, split.canonicalSplitBytes());
            update.setString(2, split.splitSha256());
            update.setString(3, TENANT);
            update.setString(4, jobId);
            assertThat(update.executeUpdate()).isOne();
        }
    }

    private static long publicationSelects(DataSource dataSource, String pattern) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT COALESCE(SUM(calls), 0)
                FROM public.pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query LIKE ?
                """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    private static GenerationJob consumerJob(String id) {
        var original = job();
        return new GenerationJob(TENANT, id, "consumer-artifact", original.artifactType(), "consumer-generation",
                SOURCE_ID, SNAPSHOT, original.executionKind(), original.dependencySet(), original.emptyPartitionPolicy(),
                "s3://warehouse/consumer-generation", original.formatId(), original.formatMajor(), original.formatMinor(),
                original.schemaSha256(), original.writerBuildId(), "", id, 2, 0, GenerationJobState.QUEUED,
                0, START, START, START);
    }

    private void publishBuild(PostgresGenerationJobStore buildStore,
                              JdbcGeneratedValuePublicationStore publicationStore, DataSplit split)
            throws Exception {
        GenerationSplit buildSplit = GenerationSplit.of(
                split.getDataObjectId(), split.toByteArray());
        DataSplit second = secondSplit(split);
        GenerationJob job = job();
        buildStore.createJob(job, dispatch(), List.of(new GenerationPage(
                JOB_ID, "page-a", 0, 0, 0, GenerationPageKind.SOURCE_MAP,
                List.of(buildSplit), List.of(), 5), new GenerationPage(
                JOB_ID, "page-b", 0, 1, 1, GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(second.getDataObjectId(), second.toByteArray())), List.of(), 5)));
        for (String suffix : List.of("a", "b")) completeSourcePage(buildStore, suffix);
        assertThatThrownBy(() -> buildStore.claimReadyForPublication(
                "tenant-b", JOB_ID, executions.takeOwnership("tenant-b", JOB_ID, "publisher-b", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2)))
                .isInstanceOf(org.pangrova.security.api.exception.AuthorizationDeniedException.class)
                .hasMessageContaining("no scoped dispatch specification");
        GenerationJob claim = buildStore.claimReadyForPublication(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "publisher-a", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2))
                .orElseThrow();
        publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(3));
    }

    private void completeSourcePage(PostgresGenerationJobStore buildStore, String suffix) throws Exception {
        var page = buildStore.claimNextPage(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "coordinator-a", 7), 7,
                Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(page.pageId()).isEqualTo("page-" + suffix);
        byte[] report = ("native-work-report-" + suffix).getBytes(StandardCharsets.UTF_8);
        assertThat(buildStore.completePage(new GenerationPageCompletion(
                TENANT, page.jobId(), page.pageId(),
                page.fencingToken(), page.ownerEpoch(),
                report, sha256(report), outputs(suffix), START.plusSeconds(1),
                executions.reportedOwner(TENANT, page.jobId(), page.ownerEpoch())))).isTrue();
    }

    private static void assertPublication(PostgresGenerationJobStore buildStore,
                                           JdbcGeneratedValuePublicationStore publicationStore) {
        assertThat(buildStore.findJob(TENANT, JOB_ID).orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
        assertThat(publicationStore.findGeneration(
                TENANT, SOURCE_ID, "embedding", SNAPSHOT))
                .hasValueSatisfying(reference -> {
                    assertThat(reference.artifactId()).isEqualTo(ARTIFACT_ID);
                    assertThat(reference.generation()).isEqualTo(GENERATION);
                });
        publicationStore.register(new GeneratedValueBuildDescriptor(
                "tenant-b", ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));
        assertThat(publicationStore.findGeneration(
                "tenant-b", SOURCE_ID, "embedding", SNAPSHOT)).isEmpty();
    }

    private static void assertTaskBinding(PostgresGenerationJobStore buildStore,
                                          JdbcGeneratedValuePublicationStore publicationStore, DataSplit split) {
        GeneratedValueGenerationBindingResolver resolver =
                new GeneratedValueGenerationBindingResolver(publicationStore, buildStore);
        var resolved = resolver.resolveTemplate(
                TENANT, SOURCE_ID, "embedding",
                SNAPSHOT, List.of(split))
                .orElseThrow();
        assertThat(resolved.definitionSnapshot()).isEqualTo(dispatch().authorizationScope().definitionSnapshot());
        OperatorBinding template = resolved.bind("scan-a");
        assertThat(resolved.bind("scan-b")).isEqualTo(template.toBuilder().setPlanNodeId("scan-b").build());
        assertThat(template.getCapability().getCapabilityId())
                .isEqualTo(BuiltInCapabilityIdentifiers.GENERATED_VALUES_OVERLAY_ID);
        assertThat(template.getArtifacts(0).getSplitObjectsCount()).isZero();
        var manifest = buildStore.findPublishedGeneration(TENANT, ARTIFACT_ID, GENERATION, SNAPSHOT).orElseThrow();
        assertThat(manifest.objectCount()).isEqualTo(4);
        assertThat(template.getResources().getFillKeys()).isEqualTo(manifest.rowCount());
        assertThat(template.getResources().getObjectStoreReadBytes()).isEqualTo(manifest.totalBytes());
        assertThat(template.getExecutionPolicy().getCoveragePolicy()).isEqualTo(
                org.pangrova.protocol.v2.CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE);
        assertThat(template.getExecutionPolicy().getFallbackPolicy()).isEqualTo(
                org.pangrova.protocol.v2.FallbackPolicy.FALLBACK_POLICY_FORBID);

        OperatorBinding taskBinding = new PublishedGenerationBindingResolver(buildStore).materializeForTasks(
                TENANT, List.of(template),
                List.of(List.of(split))).get(0).get(0);
        assertThat(taskBinding.getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getObjectRole())
                .containsExactly("GENERATED_VALUES", "ROW_LOCATORS");
        PublishedGenerationBindingChecksum.verify(taskBinding.getArtifacts(0));
        assertThat(taskBinding.getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getSourceObjectId()).containsOnly("object-a");
        assertThat(new PublishedGenerationBindingResolver(buildStore).resolveTemplate(
                new PublishedGenerationBindingResolver.GenerationResolutionIntent(TENANT, ARTIFACT_ID,
                        GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, GENERATION, SOURCE_ID, SNAPSHOT,
                        GenerationSourceCoverage.COMPLETE_GENERATION),
                List.of(split))).isEmpty();
        DataSplit changedSource = split.toBuilder()
                .setOpaqueLocation("s3://warehouse/replaced-object.parquet")
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "iceberg", SOURCE_ID, SNAPSHOT, "object-a",
                        "s3://warehouse/replaced-object.parquet", 0, 0))
                .build();
        assertThatThrownBy(() -> resolver.resolveTemplate(
                TENANT, SOURCE_ID, "embedding", SNAPSHOT, List.of(changedSource)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exactly cover");
    }

    private static GenerationJob job() {
        var format = BuiltInCapabilityCatalog.generatedValuesArtifactFormat();
        return new GenerationJob(
                TENANT, JOB_ID, ARTIFACT_ID,
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                GENERATION, SOURCE_ID, SNAPSHOT,
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        SOURCE_ID, SNAPSHOT),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://warehouse/system/generated-values-test", format.getFormatId(),
                format.getMajorVersion(), format.getMinorVersion(),
                HexFormat.of().formatHex(format.getSchemaSha256().toByteArray()),
                format.getWriterBuildId(), "", JOB_ID, 2, 0,
                GenerationJobState.QUEUED, 0, START, START, START);
    }

    private static GenerationDispatchSpec dispatch() {
        ArtifactWriteBinding output = ArtifactWriteBinding.newBuilder()
                .setArtifactId(ARTIFACT_ID)
                .setArtifactType(GenerationOutputTypeIds.GENERATED_COLUMN_VALUES)
                .setGeneration(GENERATION)
                .setSourceId(SOURCE_ID)
                .setSourceSnapshot(SNAPSHOT)
                .setStagingUri("s3://warehouse/system/generated-values-test")
                .setFormat(BuiltInCapabilityCatalog.generatedValuesArtifactFormat())
                .setRowLocator(BuiltInCapabilityCatalog.physicalRowLocatorContract())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT").setId(SOURCE_ID).setVersion(SNAPSHOT))
                .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                HexFormat.of().parseHex(
                                        org.pangrova.generation.api.dependency.GenerationDependencySet
                                                .sourceSnapshot(SOURCE_ID, SNAPSHOT).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId("generated-values-sink")
                .setCapability(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef())
                .setOutputArtifact(output)
                .build();
        return GenerationDispatchSpecFactory.create(
                JOB_ID, new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationGraphPurpose.ARTIFACT_GENERATION,
                        List.of(GenerationStageGraph.StagePlan.of(
                                0, 2, GenerationPageKind.SOURCE_MAP, new byte[] {1},
                                GenerationStageTemplate.of(OperatorBindingSet.newBuilder().addBindings(sink).build().toByteArray(), List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "alice",
                org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "sales", "documents"),
                        java.util.Set.of("text"),
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "sales", "documents")),
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
    }

    private static List<GenerationOutput> outputs(String suffix) {
        return List.of(
                new GenerationOutput(
                        "values-" + suffix, "s3://warehouse/values-" + suffix, "GENERATED_VALUES",
                        "object-" + suffix, 0, GenerationOutputScope.SOURCE_OBJECT,
                        1024, 10, CHECKSUM, ""),
                new GenerationOutput(
                        "locators-" + suffix, "s3://warehouse/locators-" + suffix, "ROW_LOCATORS",
                        "object-" + suffix, 1, GenerationOutputScope.SOURCE_OBJECT,
                        256, 10, CHECKSUM, ""));
    }

    private static String sha256(byte[] value) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(value));
    }
}
