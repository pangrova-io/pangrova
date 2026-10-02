package org.pangrova.protocol.validation;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Validates explicit canonical-v2 boundary constraints without name-based false positives. */
class ProtoValidatorTest {

    @Test
    void wellFormedTaskAndSplitHaveNoViolations() {
        DataSplit split = validSplit();
        TaskRequest task = validTask().toBuilder().addSplits(split).build();

        assertThatNoException().isThrownBy(() -> ProtoValidator.validate(task));
        assertThat(ProtoValidator.collectViolations(task)).isEmpty();
    }

    @Test
    void missingAttemptIdentityAndPlanAreReported() {
        TaskRequest invalid = TaskRequest.newBuilder()
                .setProtocol(ProtocolVersion.newBuilder().setMajor(2))
                .setAttempt(TaskAttemptId.newBuilder())
                .setTaskKind(TaskKind.TASK_KIND_READ)
                .build();

        List<String> violations = ProtoValidator.collectViolations(invalid);

        assertThat(violations)
                .anyMatch(value -> value.contains("query_or_job_id"))
                .anyMatch(value -> value.contains("task_id"))
                .anyMatch(value -> value.contains("substrait_plan"));
    }

    @Test
    void optionalIdsAreNotMadeRequiredBySuffix() {
        TaskRequest valid = validTask();

        assertThat(valid.getBudgetReservationId()).isEmpty();
        assertThat(ProtoValidator.collectViolations(valid)).isEmpty();
    }

    @Test
    void repeatedNestedSplitsAreValidated() {
        DataSplit invalid = validSplit().toBuilder().clearConnectorId().build();
        TaskRequest task = validTask().toBuilder().addSplits(invalid).build();

        assertThat(ProtoValidator.collectViolations(task))
                .anyMatch(value -> value.contains("splits[0]")
                        && value.contains("connector_id"));
    }

    @Test
    void identifierAndLocationLimitsAreEnforced() {
        DataSplit invalid = validSplit().toBuilder()
                .setSplitId("x".repeat(257))
                .setOpaqueLocation("x".repeat(65_537))
                .build();

        assertThat(ProtoValidator.collectViolations(invalid))
                .anyMatch(value -> value.contains("split_id")
                        && value.contains("exceeds maximum 256"))
                .anyMatch(value -> value.contains("opaque_location")
                        && value.contains("exceeds maximum 65536"));
    }

    @Test
    void validatingCodecParsesValidAndRejectsInvalidOrCorruptBytes() throws Exception {
        DataSplit valid = validSplit();
        assertThat(ValidatingProtoCodec.parseAndValidate(
                DataSplit.parser(), valid.toByteArray())).isEqualTo(valid);

        DataSplit invalid = valid.toBuilder().clearSplitId().build();
        assertThatThrownBy(() -> ValidatingProtoCodec.parseAndValidate(
                DataSplit.parser(), invalid.toByteArray()))
                .isInstanceOf(ProtoValidationException.class);
        assertThatThrownBy(() -> ValidatingProtoCodec.parseAndValidate(
                DataSplit.parser(), new byte[]{0x7F, 0x7F, 0x7F}))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void validateReturnsSameAlreadyParsedInstance() {
        DataSplit valid = validSplit();
        assertThat(ValidatingProtoCodec.validate(valid)).isSameAs(valid);
    }

    private static TaskRequest validTask() {
        return TaskRequest.newBuilder()
                .setProtocol(ProtocolVersion.newBuilder().setMajor(2))
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId("query-1")
                        .setTaskId("task-1"))
                .setTaskKind(TaskKind.TASK_KIND_READ)
                .setSubstraitPlan(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
    }

    private static DataSplit validSplit() {
        return DataSplit.newBuilder()
                .setSplitId("split-1")
                .setConnectorId("files")
                .setOpaqueLocation("/data/file.parquet")
                .setLength(100)
                .setConnectorDetail(Any.pack(FileSplitDetail.newBuilder()
                        .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                        .build()))
                .build();
    }
}
