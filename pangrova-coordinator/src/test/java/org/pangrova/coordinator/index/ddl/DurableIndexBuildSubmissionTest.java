package org.pangrova.coordinator.index.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactBuildJobState;
import org.pangrova.indexing.control.ArtifactBuildSplit;
import org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.planner.artifact.VectorArtifactBuildPlanBuilder;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;

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
                    var plan = VectorArtifactBuildPlanBuilder.build(
                            List.of("catalog", "schema", "items"), "embedding", capability,
                            BuiltInCapabilityCatalog.vectorArtifactFormat(algorithm));
                    var splits = empty ? List.<ArtifactBuildSplit>of()
                            : List.of(ArtifactBuildSplit.of("source-1", DataSplit.newBuilder()
                                    .setSplitId("split-1").setConnectorId("file")
                                    .setDataObjectId("source-1").setSourceSnapshot("snapshot-7")
                                    .setOpaqueLocation("s3://data/items/source-1.parquet")
                                    .build().toByteArray()));
                    var pages = DurableIndexBuildService.buildPages(
                            "job-1", splits, ArtifactTypeIds.VECTOR_INDEX, 5);
                    var submission = DurableIndexBuildSubmission.create(
                            new DurableIndexBuildSubmission.Definition(
                                    "tenant", "user", "job-1", descriptor,
                                    new QualifiedTableName("catalog", "schema", "items"), plan,
                                    ArtifactTypeIds.VECTOR_INDEX,
                                    DurableIndexBuildService.vectorProperties(capability, properties, 4, metric),
                                    pages, "7", "s3://artifacts/staging/job-1", "job-1:snapshot-7",
                                    new ArtifactBuildDispatchSpec.ResourceGrant(
                                            64L * 1024 * 1024, 1, 64L * 1024 * 1024, 60_000), now));
                    assertThat(submission.job().emptyPartitionPolicy()).isEqualTo(empty
                            ? ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                            : ArtifactEmptyPartitionPolicy.FORBID);
                    assertThat(submission.job().state()).isEqualTo(empty
                            ? ArtifactBuildJobState.READY_TO_PUBLISH : ArtifactBuildJobState.QUEUED);
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
