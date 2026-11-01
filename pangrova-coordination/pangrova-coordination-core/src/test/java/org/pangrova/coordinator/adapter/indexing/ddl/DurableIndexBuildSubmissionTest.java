package org.pangrova.coordinator.adapter.indexing.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.job.GenerationSplit;
import org.pangrova.generation.api.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.GenerationOutputTypeIds;
import org.pangrova.planner.generation.VectorGenerationPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DurableIndexBuildSubmissionTest {

    @Test
    void canonicalMetricSurvivesDurableDispatchAndEmptySnapshotPolicy() throws Exception {
        for (String algorithm : List.of("HNSW", "IVF_PQ", "DISK_ANN")) {
            for (String metric : algorithm.equals("DISK_ANN")
                    ? List.of("L2") : List.of("L2", "COSINE", "INNER_PRODUCT")) {
                for (boolean empty : List.of(true, false)) {
                    Instant now = Instant.parse("2026-01-01T00:00:00Z");
                    var properties = IndexBuildProperties.canonicalize(IndexType.VECTOR,
                            Map.of("algorithm", algorithm, "metric", metric));
                    var descriptor = new IndexDescriptor("index-1", "idx", "catalog", "schema",
                            "items", List.of("embedding"), IndexType.VECTOR, properties,
                            IndexState.BUILDING, null, 7, "snapshot-7", "snapshot-7", now, now, "tenant");
                    var capability = BuiltInCapabilityCatalog.vectorArtifactBuildPlanRef(algorithm);
                    var plan = VectorGenerationPlanBuilder.build(
                            List.of("catalog", "schema", "items"), "embedding", capability,
                            BuiltInCapabilityCatalog.vectorArtifactFormat(algorithm));
                    var splits = empty ? List.<GenerationSplit>of()
                            : List.of(GenerationSplit.of("source-1", DataSplit.newBuilder()
                                    .setSplitId("split-1").setConnectorId("file")
                                    .setDataObjectId("source-1").setSourceSnapshot("snapshot-7")
                                    .setOpaqueLocation("s3://data/items/source-1.parquet")
                                    .build().toByteArray()));
                    var pages = DurableIndexBuildService.buildPages(
                            "job-1", splits, GenerationOutputTypeIds.VECTOR_INDEX, 5);
                    var submission = DurableIndexBuildSubmission.create(
                            new DurableIndexBuildSubmission.Definition(
                                    "tenant", "user", "job-1", descriptor,
                                    new QualifiedTableName("catalog", "schema", "items"), plan,
                                    GenerationOutputTypeIds.VECTOR_INDEX,
                                    DurableIndexBuildService.vectorProperties(capability, properties, 4, metric),
                                    pages, "7", "s3://artifacts/staging/job-1", "job-1:snapshot-7",
                                    new GenerationDispatchSpec.ResourceGrant(
                                            64L * 1024 * 1024, 1, 64L * 1024 * 1024, 60_000), now));
                    assertThat(submission.job().emptyPartitionPolicy()).isEqualTo(empty
                            ? GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                            : GenerationEmptyPartitionPolicy.FORBID);
                    assertThat(submission.job().state()).isEqualTo(empty
                            ? GenerationJobState.READY_TO_PUBLISH : GenerationJobState.QUEUED);
                    var output = OperatorBindingSet.parseFrom(submission.dispatch().stageGraph().requireStage(0).bindingSetBytes())
                            .getBindings(0).getOutputArtifact();
                    assertThat(output.getFormat().getMajorVersion()).isEqualTo(2);
                    assertThat(output.getPropertiesList()).filteredOn(value -> value.getName().equals("metric"))
                            .singleElement().satisfies(value -> {
                                assertThat(value.getValue().getType().getCanonicalSql()).isEqualTo("VARCHAR");
                                assertThat(value.getValue().getStringValue()).isEqualTo(metric);
                            });
                    assertThat(output.getEmptyPartitionPolicy()).isEqualTo(empty
                            ? org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                    .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS
                            : org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                    .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID);
                }
            }
        }
    }
}
