package org.pangrova.testing.integration.e2e.jni;

import com.google.protobuf.ByteString;
import io.substrait.proto.Plan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IcebergReadMode;
import org.pangrova.protocol.v2.IcebergSplitDetail;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.RuntimeInitConfig;
import org.pangrova.protocol.v2.S3ExecutionSettings;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.testkit.MinioContainer;
import org.pangrova.worker.exception.JniCallException;
import org.pangrova.worker.jni.NativeCapabilityLoader;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.memory.NativeMemoryCoordinator;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Reads an actual MinIO Parquet object through the pinned SDK in a fresh Native process. */
@E2ETest
@org.junit.jupiter.api.Tag("native-required")
class NativeS3ReadEvidenceE2ETest extends ContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(NativeS3ReadEvidenceE2ETest.class);
    private static final String BUCKET = "native-scan-evidence";
    private static final String OBJECT = "snapshot-1/scan.parquet";
    private static final String ACCESS_KEY = "test-native-s3";
    private static final String SECRET_KEY = "test-native-s3-secret";
    private static final long MEMORY = 256L * 1024 * 1024;
    private static final int ROWS = 128;

    @TempDir Path directory;

    @Test
    void pinnedIcebergLengthEliminatesOneRealS3HeadAndRetainsTheReadEvidence() throws Exception {
        try (var minio = MinioContainer.create().withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY).withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withCommand("server", "/data")) {
            minio.start();
            var endpoint = new URI("http", null, minio.getHost(), minio.getMappedPort(9000), null, null, null);
            Path file = writeParquet();
            try (var objects = S3Client.builder().endpointOverride(endpoint).forcePathStyle(true)
                    .region(Region.US_EAST_1).credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY))).build()) {
                objects.createBucket(request -> request.bucket(BUCKET));
                objects.putObject(request -> request.bucket(BUCKET).key(OBJECT), RequestBody.fromFile(file));
            }
            runNative(endpoint, Files.size(file));

            var measured = report("unqualified");
            var pinned = report("pinned");
            assertThat(measured.getReturnedBytes()).isPositive().isEqualTo(pinned.getReturnedBytes());
            assertThat(measured.getS3ResponseBodyBytes()).isPositive().isEqualTo(pinned.getS3ResponseBodyBytes());
            assertThat(measured.getS3SdkAttempts()).isEqualTo(pinned.getS3SdkAttempts() + 1);
            assertThat(measured.getS3SdkRetries()).isZero();
            assertThat(pinned.getS3SdkRetries()).isZero();
            var rejected = TaskReport.parseFrom(Files.readAllBytes(directory.resolve("rejected.report")));
            assertThat(rejected.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(rejected.getNativeObjectReads().getS3SdkAttempts()).isPositive();
            LOG.info("event=native_s3_read_evidence measuredAttempts={} pinnedAttempts={} returnedBytes={} "
                            + "responseBodyBytes={} rejectedAttempts={}",
                    measured.getS3SdkAttempts(), pinned.getS3SdkAttempts(), pinned.getReturnedBytes(),
                    pinned.getS3ResponseBodyBytes(), rejected.getNativeObjectReads().getS3SdkAttempts());
        }
    }

    private Path writeParquet() throws Exception {
        Path file = directory.resolve("scan.parquet");
        var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        try (var writer = Parquet.writeData(org.apache.iceberg.Files.localOutput(file.toFile())).schema(schema)
                .withSpec(PartitionSpec.unpartitioned()).createWriterFunc(GenericParquetWriter::create).build()) {
            for (long id = 0; id < ROWS; id++) {
                var row = GenericRecord.create(schema);
                row.setField("id", id);
                writer.write(row);
            }
        }
        return file;
    }

    private void runNative(URI endpoint, long size) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java");
        var log = directory.resolve("native-s3.log");
        var process = new ProcessBuilder(java.toString(), "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                NativeProbe.class.getName(), directory.toString(), endpoint.toString(), Long.toString(size))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(70, TimeUnit.SECONDS)).as("Native S3 process finished").isTrue();
            assertThat(process.exitValue()).as("Native S3 log %s", Files.readString(log)).isZero();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private org.pangrova.protocol.v2.NativeObjectReadStats report(String label) throws Exception {
        var report = TaskReport.parseFrom(Files.readAllBytes(directory.resolve(label + ".report")));
        assertThat(report.getState()).as("%s", report.getError()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getNativeTaskStats().getOperatorStatsList()).anySatisfy(operator -> {
            assertThat(operator.getOperatorType()).isEqualTo("TableScan");
            assertThat(operator.getOutputRows()).isEqualTo(ROWS);
        });
        assertThat(report.hasNativeObjectReads()).isTrue();
        return report.getNativeObjectReads();
    }

    /** Uses the production JNI-only catalog binding in a fresh Native process. */
    public static final class NativeProbe {
        private NativeProbe() {}

        public static void main(String[] args) throws Exception {
            var directory = Path.of(args[0]);
            long bytes = Long.parseLong(args[2]);
            var configuration = RuntimeInitConfig.newBuilder().setMaxMemoryBytes(MEMORY).setIoThreadCount(2).build();
            try {
                startRuntime(configuration);
                assertThatThrownBy(NativeRuntime::finalizeNativeProcess)
                        .isInstanceOf(JniCallException.class).hasMessageContaining("must shut down");
                scan(directory.resolve("unqualified.report"), bytes, 0, args[1], ACCESS_KEY);
                NativeRuntime.shutdownNativeRuntime();

                startRuntime(configuration);
                scan(directory.resolve("pinned.report"), bytes, bytes, args[1], ACCESS_KEY);
                scan(directory.resolve("rejected.report"), bytes, bytes, args[1], "denied-native-fixture");
            } finally {
                NativeRuntime.shutdownNativeRuntime();
                NativeRuntime.finalizeNativeProcess();
            }
            NativeRuntime.finalizeNativeProcess();
            assertThatThrownBy(() -> startRuntime(configuration))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("have been finalized");
        }

        private static void startRuntime(RuntimeInitConfig configuration) {
            NativeRuntime.initNativeRuntime(configuration.toByteArray(),
                    new NativeMemoryCoordinator(MEMORY, new MemoryArbitrator(MEMORY, 0)));
            NativeCapabilityLoader.finalizeStartup();
        }

        private static void scan(Path receipt, long bytes, long pinnedBytes, String endpoint, String accessKey)
                throws Exception {
            var plan = Plan.newBuilder();
            org.pangrova.protocol.substrait.SubstraitProtocolVersion.stamp(plan);
            var read = plan.addRelationsBuilder().getRootBuilder().addNames("id").getInputBuilder().getReadBuilder();
            read.getBaseSchemaBuilder().addNames("id").getStructBuilder().addTypesBuilder()
                    .getI64Builder().setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
            read.getNamedTableBuilder().addNames("scan");
            byte[] substrait = plan.build().toByteArray();
            var uri = "s3://" + BUCKET + "/" + OBJECT;
            var catalog = CatalogExecutionBinding.newBuilder().setCatalogName("lake").setCatalogIncarnationId("s3-test")
                    .setCatalogRevision(1).setConnectorProvider("iceberg")
                    .setConfigurationFingerprint(ProtocolChecksums.sha256(endpoint.getBytes(StandardCharsets.UTF_8)))
                    .build();
            var request = TaskRequest.parseFrom(CanonicalNativeTask.valuesRequest()).toBuilder()
                    .setSubstraitPlan(ByteString.copyFrom(substrait))
                    .setSubstraitPlanChecksum(ProtocolChecksums.sha256(substrait))
                    .addWorkerLocalStorageBindings(ObjectStoreExecutionBinding.newBuilder().setCatalog(catalog)
                            .setTenantId("e2e").setWarehouseUri("s3://" + BUCKET + "/snapshot-1")
                            .setS3(S3ExecutionSettings.newBuilder().setEndpoint(endpoint).setRegion(Region.US_EAST_1.id())
                                    .setAccessKey(accessKey).setSecretKey(SECRET_KEY).setPathStyleAccess(true)))
                    .addSplits(DataSplit.newBuilder().setSplitId(receipt.getFileName().toString())
                            .setConnectorId("iceberg").setSourceId("scan").setSourceSnapshot("1")
                            .setDataObjectId(uri).setOpaqueLocation(uri).setLength(bytes).setCatalog(catalog)
                            .setFileDetail(FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                    .setIceberg(IcebergSplitDetail.newBuilder().setSnapshotId(1)
                                            .setReadMode(IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT)
                                            .setDataFileSizeBytes(pinnedBytes))));
            request.getResourceGrantBuilder().setObjectStoreReadBytes(16 * bytes);
            long handle = NativeRuntime.nativeCreateTaskExecutor(request.build().toByteArray());
            byte[] report;
            try {
                NativeRuntime.nativeStartTask(handle);
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    var terminal = TaskReport.parseFrom(NativeRuntime.nativeGetTaskStatus(handle));
                    assertThat(terminal.getState()).isIn(TaskState.TASK_STATE_FAILED, TaskState.TASK_STATE_FINISHED);
                });
            } finally {
                report = NativeRuntime.nativeReleaseExecutor(handle);
            }
            Files.write(receipt, report);
        }
    }
}
