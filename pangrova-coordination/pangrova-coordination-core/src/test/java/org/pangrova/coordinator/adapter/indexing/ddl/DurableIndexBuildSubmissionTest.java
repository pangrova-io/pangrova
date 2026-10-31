package org.pangrova.coordinator.adapter.indexing.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.planner.databuild.VectorDataBuildPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;

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
                    var plan = VectorDataBuildPlanBuilder.build(
                            List.of("catalog", "schema", "items"), "embedding", capability,
                            BuiltInCapabilityCatalog.vectorArtifactFormat(algorithm));
                    var splits = empty ? List.<DataBuildSplit>of()
                            : List.of(DataBuildSplit.of("source-1", DataSplit.newBuilder()
                                    .setSplitId("split-1").setConnectorId("file")
                                    .setDataObjectId("source-1").setSourceSnapshot("snapshot-7")
                                    .setOpaqueLocation("s3://data/items/source-1.parquet")
                                    .build().toByteArray()));
                    var pages = DurableIndexBuildService.buildPages(
                            "job-1", splits, DataBuildOutputTypeIds.VECTOR_INDEX, 5);
                    var submission = DurableIndexBuildSubmission.create(
                            new DurableIndexBuildSubmission.Definition(
                                    "tenant", "user", "job-1", descriptor,
                                    new QualifiedTableName("catalog", "schema", "items"), plan,
                                    DataBuildOutputTypeIds.VECTOR_INDEX,
                                    DurableIndexBuildService.vectorProperties(capability, properties, 4, metric),
                                    pages, "7", "s3://artifacts/staging/job-1", "job-1:snapshot-7",
                                    new DataBuildDispatchSpec.ResourceGrant(
                                            64L * 1024 * 1024, 1, 64L * 1024 * 1024, 60_000), now));
                    assertThat(submission.job().emptyPartitionPolicy()).isEqualTo(empty
                            ? DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                            : DataBuildEmptyPartitionPolicy.FORBID);
                    assertThat(submission.job().state()).isEqualTo(empty
                            ? DataBuildJobState.READY_TO_PUBLISH : DataBuildJobState.QUEUED);
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
