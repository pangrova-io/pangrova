package org.pangrova.protocol.rest.task;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.ArtifactDependency;
import org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy;
import org.pangrova.protocol.v2.ArtifactExecutionKind;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;

import java.util.Map;

/** Ensures a generic feature export survives the canonical REST transport unchanged. */
class TabularExportTaskRequestRoundTripTest {


    @Test
    void preservesGenericExportBindingSplitAndZeroRowPolicy() throws Exception {
        var split = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "iceberg", "lake.raw.customers", "snapshot-41",
                        "data-0001.parquet", "s3://warehouse/data-0001.parquet",
                        0, 1024, Map.of("day", "2026-09-19"),
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                .build()));
        var output = ArtifactWriteBinding.newBuilder()
                .setArtifactId("feature-view:customer_features:1")
                .setArtifactType("TABULAR_EXPORT")
                .setGeneration("generation-12")
                .setSourceId("lake.raw.customers")
                .setSourceSnapshot("snapshot-41")
                .setStagingUri("s3://warehouse/system/staging/job-1")
                .setFormat(BuiltInCapabilityCatalog.tabularExportFormat())
                .setRowLocator(BuiltInCapabilityCatalog.physicalRowLocatorContract())
                .addExpectedSourceObjectIds("data-0001.parquet")
                .setPublicationFencingToken(7)
                .setIdempotencyKey("job-1:page-1:export")
                .setWorkUnitId("page-1")
                .setWorkKind(ArtifactWorkKind.ARTIFACT_WORK_KIND_SOURCE_MAP)
                .setExecutionKind(ArtifactExecutionKind.ARTIFACT_EXECUTION_KIND_EXPORT)
                .addDependencies(ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT")
                        .setId("lake.raw.customers")
                        .setVersion("snapshot-41"))
                .setDependencySetChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(new byte[32])))
                .setEmptyPartitionPolicy(ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS)
                .build();
        var binding = OperatorBinding.newBuilder()
                .setPlanNodeId("feature-export-sink")
                .setCapability(BuiltInCapabilityCatalog.tabularExportPlanRef())
                .setOutputArtifact(output)
                .setExecutionPolicy(ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(CoveragePolicy
                                .COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(RetryClass.RETRY_CLASS_IDEMPOTENT)
                        .setDeadlineEpochMs(4_102_444_800_000L)
                        .setIdempotencyKey("job-1:page-1:export"))
                .build();
        var canonical = CanonicalTaskRequestTestFixture.builder(
                        "page-1-attempt-0", "job-1",
                        TaskKind.TASK_KIND_BACKGROUND_EXPORT, new byte[]{1, 2, 3})
                .addSplits(split)
                .addOperatorBindings(binding)
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("page-1:grant")
                        .setCpuSlots(1)
                        .setNativeMemoryBytes(64 * 1024 * 1024)
                        .setObjectStoreReadBytes(128 * 1024 * 1024)
                        .setObjectStoreWriteBytes(128 * 1024 * 1024))
                .build();
        SubmitTaskRequest encoded = SubmitTaskRequest.from(canonical);

        SubmitTaskRequest decoded = SubmitTaskRequest.decode(encoded.encode());

        assertThat(decoded.canonicalBytes()).isEqualTo(canonical.toByteArray());
        assertThat(decoded.taskRequest().getSplits(0)).isEqualTo(split);
        assertThat(decoded.taskRequest().getOperatorBindingsList())
                .singleElement()
                .satisfies(actual -> {
                    assertThat(actual.getCapability()).isEqualTo(
                            BuiltInCapabilityCatalog.tabularExportPlanRef());
                    assertThat(actual.getOutputArtifact()).isEqualTo(output);
                    assertThat(actual.getOutputArtifact().getEmptyPartitionPolicy())
                            .isEqualTo(ArtifactEmptyPartitionPolicy
                                    .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS);
                });
    }
}
