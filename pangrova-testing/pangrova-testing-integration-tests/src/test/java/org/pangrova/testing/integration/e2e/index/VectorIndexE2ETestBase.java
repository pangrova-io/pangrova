/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.integration.e2e.index;


import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.indexing.store.jdbc.JdbcIndexMetadataStore;
import org.pangrova.protocol.databuild.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Real JDBC publication, issued task and Native filesystem evidence for vector SQL E2E. */
public abstract class VectorIndexE2ETestBase extends SqlE2ETestBase {
    private static EvidenceObserver observer;

    protected static IndexDescriptor findIndex(String indexName, String fqn) {
        String[] parts = fqn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("A three-part table name is required");
        }
        String tenant = authenticatedCatalogSession(parts[0]).getProperties().get("tenant_id");
        var matches = new JdbcIndexMetadataStore(stateStoreDataSource())
                .findIndexesByTable(tenant, parts[0], parts[1], parts[2]).stream()
                .filter(index -> index.indexName().equals(indexName)).toList();
        assertThat(matches).as("exact tenant/table/index metadata identity").hasSize(1);
        return matches.get(0);
    }

    protected static PublishedIndex awaitPublishedVectorIndex(String indexName, String fqn) {
        org.awaitility.Awaitility.await("vector publication: " + indexName)
                .atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    var index = findIndex(indexName, fqn);
                    if (index.state() == IndexState.STALE || index.state() == IndexState.DROPPED) {
                        throw new IllegalStateException("Vector index cannot be used: "
                                + indexName + " state=" + index.state());
                    }
                    return index.state() == IndexState.READY;
                });
        var descriptor = findIndex(indexName, fqn);
        var active = new JdbcIndexMetadataStore(stateStoreDataSource())
                .findActiveIndexGeneration(descriptor.indexId()).orElseThrow(() ->
                        new IllegalStateException("READY index has no active immutable generation"));
        assertThat(active.indexId()).isEqualTo(descriptor.indexId());
        assertThat(active.generation()).isEqualTo(descriptor.generation());
        assertThat(active.sourceId()).isEqualTo(fqn);
        assertThat(active.sourceSnapshot()).isNotBlank().isEqualTo(descriptor.buildSnapshotId());
        assertThat(active.artifactType()).isEqualTo("VECTOR_INDEX");
        var store = ModelAuthorityFixtures.artifacts().builds(stateStoreDataSource());
        var manifest = store.findPublishedGeneration(descriptor.tenantId(), descriptor.indexId(),
                Integer.toString(descriptor.generation()), active.sourceSnapshot()).orElseThrow(() ->
                        new IllegalStateException("READY index has no durable published artifact"));
        assertThat(manifest.manifestSha256()).isEqualTo(active.manifestSha256());
        assertThat(manifest.sourceId()).isEqualTo(active.sourceId());
        var job = store.findJob(manifest.tenantId(), manifest.jobId()).orElseThrow();
        assertThat(job.state()).isEqualTo(DataBuildJobState.PUBLISHED);
        assertThat(job.stagingGeneration()).isEqualTo(manifest.generation());
        assertThat(job.sourceSnapshot()).isEqualTo(manifest.sourceSnapshot());
        var outputs = store.loadCompleteOutputs(manifest.tenantId(), manifest.jobId());
        assertThat(outputs).as("actual Native output metadata").isNotEmpty();
        return new PublishedIndex(descriptor, manifest, List.copyOf(outputs));
    }

    /** The observer copies the original issued request before drain receipt compaction. */
    protected static QueryEvidence executeObserved(String sql) {
        EvidenceObserver current;
        synchronized (VectorIndexE2ETestBase.class) {
            if (observer == null || observer.tracker != taskTracker) {
                observer = new EvidenceObserver(taskTracker);
                taskTracker.addExecutionDrainListener(observer::accept);
            }
            current = observer;
        }
        Capture capture = new Capture();
        current.active.add(capture);
        try {
            var submitted = submitQueryViaHttp(sql);
            Set<String> issuedBeforeConsumption = taskTracker.getTaskIds(submitted.queryId());
            List<TypedRow> rows = consumeSubmittedQuery(submitted);
            org.awaitility.Awaitility.await("actual Native query drain")
                    .atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> {
                        var tasks = capture.forQuery(submitted.queryId());
                        assertThat(tasks).isNotEmpty();
                        assertThat(tasks.stream().map(task -> task.request().getAttempt().getTaskId()))
                                .containsAll(issuedBeforeConsumption);
                    });
            var tasks = capture.forQuery(submitted.queryId());
            for (IssuedTask task : tasks) {
                assertThat(task.report().getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
                assertThat(task.report().getReportOrigin())
                        .isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
                assertThat(task.report().getExecutionDrainState())
                        .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
                assertThat(task.report().hasNativeTaskStats()).isTrue();
                assertThat(task.report().getAttempt()).isEqualTo(task.request().getAttempt());
            }
            return new QueryEvidence(rows, tasks);
        } catch (Exception failure) {
            throw new IllegalStateException("Real SQL/Flight execution did not complete", failure);
        } finally {
            current.active.remove(capture);
        }
    }

    protected static List<TypedRow> executeAnn(String sql, PublishedIndex index) {
        QueryEvidence evidence = executeObserved(sql);
        assertAnn(index, evidence);
        return evidence.rows();
    }

    protected static double assertAnn(PublishedIndex index, QueryEvidence evidence) {
        String expectedCapability = switch (index.descriptor().properties().get("algorithm")) {
            case "DISK_ANN" -> BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID;
            case "HNSW" -> BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID;
            case "IVF_PQ" -> BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID;
            default -> throw new IllegalArgumentException("Unsupported functional ANN fixture algorithm");
        };
        return assertAnnCapability(index, evidence, expectedCapability);
    }

    protected static double assertFilteredAnn(PublishedIndex index, QueryEvidence evidence) {
        assertThat(index.descriptor().properties().get("algorithm")).isEqualTo("DISK_ANN");
        double required = assertAnnCapability(index, evidence, BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID);
        int actualOwners = 0;
        for (IssuedTask task : evidence.tasks()) {
            var bindings = task.request().getOperatorBindingsList().stream().filter(binding ->
                    binding.getCapability().getCapabilityId().equals(BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID))
                    .toList();
            for (var binding : bindings) {
                assertThat(binding.getCapability()).isEqualTo(BuiltInCapabilityCatalog.filteredDiskAnnRetrievalPlanRef());
                assertFilteredWireContract(task.request(), binding);
                assertThat(task.report().getNativeTaskStats().getOperatorStatsList()).anySatisfy(operator -> {
                    assertThat(operator.getPlanNodeId()).isEqualTo(binding.getPlanNodeId());
                    assertThat(operator.getOperatorType()).isEqualTo("FilteredArtifactRetrieval");
                });
                actualOwners++;
            }
        }
        assertThat(actualOwners).as("actual live Native filtered retrieval operators").isPositive();
        return required;
    }

    private static void assertFilteredWireContract(TaskRequest request,
            org.pangrova.protocol.v2.OperatorBinding binding) {
        try {
            var plan = io.substrait.proto.Plan.parseFrom(request.getSubstraitPlan());
            List<org.pangrova.protocol.v2.RetrievalSourceDetail> details = new java.util.ArrayList<>();
            collectRetrievalDetails(plan, details);
            var matching = details.stream().filter(detail -> detail.getPlanNodeId()
                    .equals(binding.getPlanNodeId())).toList();
            assertThat(matching).as("actual issued filtered source wire contract").hasSize(1);
            var detail = matching.get(0);
            assertThat(detail.getInvocation().getCapability()).isEqualTo(binding.getCapability());
            assertThat(detail.getOutputLimit()).isPositive()
                    .isEqualTo(binding.getExecutionPolicy().getApproximation().getRecallK());
            assertThat(detail.getCandidateLimit()).isGreaterThanOrEqualTo(detail.getOutputLimit())
                    .isEqualTo(binding.getExecutionPolicy().getApproximation().getCandidateLimit());
            assertThat(detail.getExactRescoreLimit()).isEqualTo(detail.getCandidateLimit());
            var contract = detail.getOutputContract();
            assertThat(contract.getSchemaVersion()).isEqualTo(2);
            assertThat(contract.getIdentityKind()).isEqualTo(
                    org.pangrova.protocol.v2.CandidateIdentityKind.CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW);
            assertThat(contract.getStableKeyFieldsList()).containsExactlyElementsOf(
                    org.pangrova.protocol.retrieval.RetrievalCandidateIdentity.PHYSICAL_FIELDS);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual issued Native retrieval plan is invalid", failure);
        }
    }

    private static void collectRetrievalDetails(com.google.protobuf.Message message,
            List<org.pangrova.protocol.v2.RetrievalSourceDetail> details)
            throws com.google.protobuf.InvalidProtocolBufferException {
        if (message instanceof io.substrait.proto.ExtensionSingleRel extension
                && extension.getDetail().is(org.pangrova.protocol.v2.RelationDetailEnvelope.class)) {
            var envelope = extension.getDetail().unpack(org.pangrova.protocol.v2.RelationDetailEnvelope.class);
            if (envelope.hasRetrievalSource()) details.add(envelope.getRetrievalSource());
        }
        for (var entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (entry.getKey().isRepeated()) {
                for (Object child : (List<?>) entry.getValue()) {
                    collectRetrievalDetails((com.google.protobuf.Message) child, details);
                }
            } else {
                collectRetrievalDetails((com.google.protobuf.Message) entry.getValue(), details);
            }
        }
    }

    private static double assertAnnCapability(PublishedIndex index, QueryEvidence evidence, String expectedCapability) {
        var manifest = index.manifest();
        Map<String, DataBuildOutput> outputs = index.outputs().stream().collect(Collectors.toMap(
                DataBuildOutput::objectId, Function.identity()));
        int matchingBindings = 0;
        double requiredRecall = 0;
        for (IssuedTask task : evidence.tasks()) {
            for (var binding : task.request().getOperatorBindingsList()) {
                if (!binding.getCapability().getCapabilityId()
                        .equals(expectedCapability)) continue;
                assertThat(task.request().getSecurity().getTenantId()).isEqualTo(manifest.tenantId());
                assertThat(binding.getExecutionPolicy().getFallbackPolicy())
                        .isEqualTo(FallbackPolicy.FALLBACK_POLICY_FORBID);
                assertThat(binding.getExecutionPolicy().getApproximation().getRequiredRecallAtK())
                        .isGreaterThan(0).isLessThanOrEqualTo(1);
                requiredRecall = Math.max(requiredRecall,
                        binding.getExecutionPolicy().getApproximation().getRequiredRecallAtK());
                assertThat(binding.getArtifactsList()).hasSize(1);
                var artifact = binding.getArtifacts(0);
                assertThat(artifact.getArtifactId()).isEqualTo(index.descriptor().indexId());
                assertThat(artifact.getArtifactType()).isEqualTo("VECTOR_INDEX");
                assertThat(artifact.getGeneration()).isEqualTo(manifest.generation());
                assertThat(artifact.getSourceId()).isEqualTo(manifest.sourceId());
                assertThat(artifact.getSourceSnapshot()).isEqualTo(manifest.sourceSnapshot());
                assertThat(artifact.getManifestUri()).isEqualTo(manifest.manifestUri());
                assertThat(hex(artifact.getManifestChecksum())).isEqualTo(manifest.manifestSha256());
                PublishedGenerationBindingChecksum.verify(artifact);
                assertThat(artifact.getFormat().getFormatId()).isEqualTo(manifest.formatId());
                assertThat(artifact.getFormat().getMajorVersion()).isEqualTo(manifest.formatMajor());
                assertThat(artifact.getFormat().getMinorVersion()).isEqualTo(manifest.formatMinor());
                assertThat(HexFormat.of().formatHex(artifact.getFormat().getSchemaSha256().toByteArray()))
                        .isEqualTo(manifest.schemaSha256());
                assertThat(artifact.getFormat().getWriterBuildId()).isEqualTo(manifest.writerBuildId());
                assertThat(task.request().getSplitsList()).filteredOn(split ->
                        split.getSourceId().equals(manifest.sourceId())).isNotEmpty().allSatisfy(split ->
                                assertThat(split.getSourceSnapshot()).isEqualTo(manifest.sourceSnapshot()));
                assertThat(artifact.getSplitObjectsList()).isNotEmpty();
                for (ArtifactObject object : artifact.getSplitObjectsList()) {
                    DataBuildOutput published = outputs.get(object.getObjectId());
                    assertThat(published).as("object emitted by the published Native build").isNotNull();
                    assertThat(object.getOpaqueUri()).isEqualTo(published.opaqueUri());
                    assertThat(object.getSourceObjectId()).isEqualTo(published.sourceObjectId());
                    assertThat(object.getObjectRole()).isEqualTo(published.objectRole());
                    assertThat(object.getSizeBytes()).isEqualTo(published.sizeBytes());
                    assertThat(hex(object.getChecksum())).isEqualTo(published.checksumSha256());
                    assertMaterialized(published);
                }
                matchingBindings++;
            }
        }
        assertThat(matchingBindings).as("issued ANN tasks; an exact result is insufficient")
                .isGreaterThan(0);
        assertThat(findIndex(index.descriptor().indexName(), manifest.sourceId()).state())
                .isEqualTo(IndexState.READY);
        assertThat(new JdbcIndexMetadataStore(stateStoreDataSource())
                .findActiveIndexGeneration(index.descriptor().indexId()).orElseThrow().generation())
                .isEqualTo(index.descriptor().generation());
        return requiredRecall;
    }

    protected static void assertDatasetSnapshot(QueryEvidence dataset, PublishedIndex index) {
        var snapshots = dataset.tasks().stream().flatMap(task -> task.request().getSplitsList().stream())
                .filter(split -> split.getSourceId().equals(index.manifest().sourceId()))
                .map(split -> split.getSourceSnapshot()).collect(Collectors.toSet());
        assertThat(snapshots).containsExactly(index.manifest().sourceSnapshot());
    }

    protected static void assertFunctionalRecall(List<TypedRow> exact, List<TypedRow> actual, double required) {
        Set<String> expectedIds = exact.stream().map(row -> row.getRaw("id").toString()).collect(Collectors.toSet());
        Set<String> actualIds = actual.stream().map(row -> row.getRaw("id").toString()).collect(Collectors.toSet());
        assertThat(expectedIds).isNotEmpty().hasSize(exact.size());
        assertThat(actual).hasSize(exact.size());
        assertThat(actualIds).hasSize(actual.size());
        long hits = actualIds.stream().filter(expectedIds::contains).count();
        assertThat((double) hits / expectedIds.size()).as("same-snapshot functional Recall at the issued threshold")
                .isGreaterThanOrEqualTo(required);
    }

    protected static void assertExact(QueryEvidence evidence) {
        assertThat(evidence.tasks()).allSatisfy(task ->
                assertThat(task.request().getOperatorBindingsList()).allSatisfy(binding ->
                        assertThat(binding.getArtifactsList()).noneMatch(artifact ->
                                artifact.getArtifactType().equals("VECTOR_INDEX"))));
    }

    protected static Path cachedObject(DataBuildOutput object) {
        String digest = object.checksumSha256().toLowerCase(java.util.Locale.ROOT);
        return nativeArtifactCacheDirectory().resolve(digest.substring(0, 2)).resolve(digest + ".bin");
    }

    private static void assertMaterialized(DataBuildOutput object) {
        try {
            Path actual = cachedObject(object);
            assertThat(Files.isRegularFile(actual)).as("checksum-addressed Native materialization").isTrue();
            assertThat(Files.size(actual)).isEqualTo(object.sizeBytes());
            var digest = MessageDigest.getInstance("SHA-256");
            try (var stream = Files.newInputStream(actual)) {
                byte[] buffer = new byte[8192];
                for (int read; (read = stream.read(buffer)) >= 0;) {
                    if (read != 0) digest.update(buffer, 0, read);
                }
            }
            assertThat(HexFormat.of().formatHex(digest.digest())).isEqualTo(object.checksumSha256());
        } catch (java.io.IOException | java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("Unable to verify actual Native artifact read", failure);
        }
    }

    private static String hex(Checksum checksum) {
        assertThat(checksum.getAlgorithm()).isEqualTo(Checksum.Algorithm.ALGORITHM_SHA256);
        assertThat(checksum.getDigest().size()).isEqualTo(32);
        return HexFormat.of().formatHex(checksum.getDigest().toByteArray());
    }

    protected record PublishedIndex(IndexDescriptor descriptor, DataBuildGenerationManifest manifest,
                                    List<DataBuildOutput> outputs) {}
    protected record IssuedTask(TaskRequest request, TaskReport report) {}
    protected record QueryEvidence(List<TypedRow> rows, List<IssuedTask> tasks) {}

    private static final class EvidenceObserver {
        private final CoordinatorTaskTracker tracker;
        private final CopyOnWriteArrayList<Capture> active = new CopyOnWriteArrayList<>();

        private EvidenceObserver(CoordinatorTaskTracker tracker) { this.tracker = tracker; }

        private void accept(TaskReport report) {
            if (active.isEmpty()) return;
            var issued = tracker.getTaskRequest(report.getAttempt().getTaskId());
            if (issued != null) {
                var fact = new IssuedTask(issued.taskRequest(), report);
                active.forEach(capture -> capture.accept(fact));
            }
        }
    }

    /** One bounded query scope; replay replaces the same task, and close drops retained plans. */
    private static final class Capture {
        private final Map<String, IssuedTask> tasks = new LinkedHashMap<>();
        private long bytes;
        private IllegalStateException failure;

        private synchronized void accept(IssuedTask fact) {
            if (failure != null) return;
            String id = fact.request().getAttempt().getTaskId();
            if (tasks.containsKey(id)) return;
            bytes += fact.request().getSerializedSize() + fact.report().getSerializedSize();
            if (tasks.size() >= 512 || bytes > 32L * 1024 * 1024) {
                // Observation must not reject a genuine execution-drain notification.
                // Fail the test at its own boundary while releasing captured plans.
                failure = new IllegalStateException("E2E evidence capture exceeded its explicit bound");
                tasks.clear();
                bytes = 0;
                return;
            }
            tasks.put(id, fact);
        }

        private synchronized List<IssuedTask> forQuery(String queryId) {
            if (failure != null) throw failure;
            return tasks.values().stream().filter(task -> task.request().getAttempt()
                    .getQueryOrJobId().equals(queryId)).toList();
        }
    }
}
