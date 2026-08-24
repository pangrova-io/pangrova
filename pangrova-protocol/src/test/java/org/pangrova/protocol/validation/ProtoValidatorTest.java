package org.pangrova.protocol.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.v1.ResourceLimit;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.v1.TaskRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Verifies that {@link ProtoValidator} correctly validates well-formed messages
 * and rejects malformed ones with appropriate violation messages.
 *
 * <p>Addresses finding F-PROTO-02: ProtoValidator was dead code with no tests.</p>
 */
class ProtoValidatorTest {

    @Test
    void validate_wellFormedTaskRequest_noViolations() {
        TaskRequest valid = TaskRequest.newBuilder()
                .setTaskId("task-001")
                .setQueryId("query-001")
                .setPlanFragment(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setStageId(1)
                .setPartitionId(0)
                .setTenantId("tenant-001")
                .build();

        assertThatNoException().isThrownBy(() -> ProtoValidator.validate(valid));
        assertThat(ProtoValidator.collectViolations(valid)).isEmpty();
    }

    @Test
    void validate_wellFormedSplitInfo_noViolations() {
        SplitInfo valid = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath("/data/table/part-0001.parquet")
                .setStartOffset(0)
                .setLength(65536)
                .build();

        assertThatNoException().isThrownBy(() -> ProtoValidator.validate(valid));
        assertThat(ProtoValidator.collectViolations(valid)).isEmpty();
    }

    @Test
    void validate_emptyRequiredIdField_reportsViolation() {
        TaskRequest missingTaskId = TaskRequest.newBuilder()
                .setTaskId("")  // required *_id field left empty
                .setQueryId("query-001")
                .setPlanFragment(ByteString.copyFrom(new byte[]{1}))
                .build();

        List<String> violations = ProtoValidator.collectViolations(missingTaskId);

        assertThat(violations)
                .anyMatch(v -> v.contains("task_id") && v.contains("required string field is empty"));
    }

    @Test
    void validate_emptyQueryId_reportsViolation() {
        TaskRequest missingQueryId = TaskRequest.newBuilder()
                .setTaskId("task-001")
                .setQueryId("")  // required *_id field left empty
                .setPlanFragment(ByteString.copyFrom(new byte[]{1}))
                .build();

        List<String> violations = ProtoValidator.collectViolations(missingQueryId);

        assertThat(violations)
                .anyMatch(v -> v.contains("query_id") && v.contains("required string field is empty"));
    }

    @Test
    void validate_emptyConnectorId_reportsViolation() {
        SplitInfo missingConnector = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("")  // required connector_id is empty
                .setFilePath("/data/file.parquet")
                .setStartOffset(0)
                .setLength(100)
                .build();

        List<String> violations = ProtoValidator.collectViolations(missingConnector);

        assertThat(violations)
                .anyMatch(v -> v.contains("connector_id") && v.contains("required string field is empty"));
    }

    @Test
    void validate_emptyFilePath_reportsViolation() {
        SplitInfo missingPath = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath("")  // required file_path is empty
                .setStartOffset(0)
                .setLength(100)
                .build();

        List<String> violations = ProtoValidator.collectViolations(missingPath);

        assertThat(violations)
                .anyMatch(v -> v.contains("file_path") && v.contains("required string field is empty"));
    }

    @Test
    void validate_negativeNonNegativeField_reportsViolation() {
        ResourceLimit negativeMemory = ResourceLimit.newBuilder()
                .setMaxMemoryBytes(-1)  // must be non-negative (*_bytes)
                .setMaxThreads(4)
                .build();

        List<String> violations = ProtoValidator.collectViolations(negativeMemory);

        assertThat(violations)
                .anyMatch(v -> v.contains("max_memory_bytes") && v.contains("must be non-negative"));
    }

    @Test
    void validate_negativeMaxThreads_reportsViolation() {
        ResourceLimit negativeThreads = ResourceLimit.newBuilder()
                .setMaxMemoryBytes(1024)
                .setMaxThreads(-2)  // max_threads must be non-negative
                .build();

        List<String> violations = ProtoValidator.collectViolations(negativeThreads);

        assertThat(violations)
                .anyMatch(v -> v.contains("max_threads") && v.contains("must be non-negative"));
    }

    @Test
    void validate_negativeStartOffset_reportsViolation() {
        SplitInfo negativeOffset = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath("/data/file.parquet")
                .setStartOffset(-100)  // start_offset is not validated by the int32 rule (it's int64)
                .setLength(100)
                .build();

        // start_offset is not in the non-negative naming rules but let's check
        // the validation still passes for properly formed fields
        List<String> violations = ProtoValidator.collectViolations(negativeOffset);
        // start_offset is not matched by the current isNonNegativeField (no _bytes, _count, etc.)
        // but it IS: it checks name.equals("start_offset")
        assertThat(violations)
                .anyMatch(v -> v.contains("start_offset") && v.contains("must be non-negative"));
    }

    @Test
    void validate_idFieldExceedsMaxLength_reportsViolation() {
        String longId = "x".repeat(257);  // exceeds 256 char limit for *_id fields
        TaskRequest longTaskId = TaskRequest.newBuilder()
                .setTaskId(longId)
                .setQueryId("query-001")
                .setPlanFragment(ByteString.copyFrom(new byte[]{1}))
                .build();

        List<String> violations = ProtoValidator.collectViolations(longTaskId);

        assertThat(violations)
                .anyMatch(v -> v.contains("task_id") && v.contains("exceeds maximum 256"));
    }

    @Test
    void validate_pathFieldExceedsMaxLength_reportsViolation() {
        String longPath = "/".concat("x".repeat(4097));  // exceeds 4096 char limit for *_path
        SplitInfo longFilePath = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath(longPath)
                .setStartOffset(0)
                .setLength(100)
                .build();

        List<String> violations = ProtoValidator.collectViolations(longFilePath);

        assertThat(violations)
                .anyMatch(v -> v.contains("file_path") && v.contains("exceeds maximum 4096"));
    }

    @Test
    void validate_multipleViolations_reportsAll() {
        TaskRequest multipleErrors = TaskRequest.newBuilder()
                .setTaskId("")   // required field empty
                .setQueryId("")  // required field empty
                .build();

        List<String> violations = ProtoValidator.collectViolations(multipleErrors);

        assertThat(violations).hasSizeGreaterThanOrEqualTo(2);
        assertThat(violations).anyMatch(v -> v.contains("task_id"));
        assertThat(violations).anyMatch(v -> v.contains("query_id"));
    }

    @Test
    void validate_throwsProtoValidationException_onFailure() {
        TaskRequest invalid = TaskRequest.newBuilder()
                .setTaskId("")
                .setQueryId("q-1")
                .build();

        assertThatThrownBy(() -> ProtoValidator.validate(invalid))
                .isInstanceOf(ProtoValidationException.class)
                .satisfies(ex -> {
                    ProtoValidationException pve = (ProtoValidationException) ex;
                    assertThat(pve.messageType()).isEqualTo("TaskRequest");
                    assertThat(pve.violations()).isNotEmpty();
                });
    }

    @Test
    void validate_emptyPlanFragment_reportsViolation() {
        TaskRequest emptyPlan = TaskRequest.newBuilder()
                .setTaskId("task-001")
                .setQueryId("query-001")
                .setPlanFragment(ByteString.EMPTY)  // required bytes field empty
                .build();

        List<String> violations = ProtoValidator.collectViolations(emptyPlan);

        assertThat(violations)
                .anyMatch(v -> v.contains("plan_fragment") && v.contains("required bytes field is empty"));
    }

    @Test
    void validatingProtoCodec_parseAndValidate_validBytes() throws InvalidProtocolBufferException {
        SplitInfo original = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath("/data/table/part-0001.parquet")
                .setStartOffset(0)
                .setLength(65536)
                .build();

        byte[] bytes = original.toByteArray();

        SplitInfo parsed = ValidatingProtoCodec.parseAndValidate(SplitInfo.parser(), bytes);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void validatingProtoCodec_parseAndValidate_invalidMessage_throws() {
        // Build a SplitInfo with empty required fields
        SplitInfo invalid = SplitInfo.newBuilder()
                .setSplitId("")     // split_id is *_id -> required
                .setConnectorId("")  // connector_id -> required
                .setFilePath("")     // file_path -> required
                .build();

        byte[] bytes = invalid.toByteArray();

        assertThatThrownBy(() ->
                ValidatingProtoCodec.parseAndValidate(SplitInfo.parser(), bytes))
                .isInstanceOf(ProtoValidationException.class);
    }

    @Test
    void validatingProtoCodec_parseAndValidate_corruptBytes_throwsParseError() {
        byte[] garbage = new byte[]{0x7F, 0x7F, 0x7F, 0x7F, 0x7F};

        assertThatThrownBy(() ->
                ValidatingProtoCodec.parseAndValidate(SplitInfo.parser(), garbage))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void validatingProtoCodec_validate_alreadyParsedMessage() {
        SplitInfo valid = SplitInfo.newBuilder()
                .setSplitId("split-001")
                .setConnectorId("iceberg")
                .setFilePath("/data/file.parquet")
                .setStartOffset(0)
                .setLength(100)
                .build();

        SplitInfo result = ValidatingProtoCodec.validate(valid);

        assertThat(result).isSameAs(valid);
    }

    @Test
    void validatingProtoCodec_validate_invalidAlreadyParsed_throws() {
        SplitInfo invalid = SplitInfo.newBuilder()
                .setSplitId("")
                .setConnectorId("")
                .setFilePath("")
                .build();

        assertThatThrownBy(() -> ValidatingProtoCodec.validate(invalid))
                .isInstanceOf(ProtoValidationException.class);
    }
}
