package org.pangrova.protocol.property.rest;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteTaskOptions;

import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Structural guardrails for the intentional REST-control-plane to v2-data-plane boundary. */
class ProtocolConsistencyPropertyTest {

    @Test
    void taskIdentityAndExecutionOptionsAreNestedInsteadOfDuplicated() {
        assertThat(TaskRequest.getDescriptor().findFieldByName("attempt").getMessageType())
                .isEqualTo(TaskAttemptId.getDescriptor());
        assertThat(TaskRequest.getDescriptor().findFieldByName("execution_options").getMessageType())
                .isEqualTo(TaskExecutionOptions.getDescriptor());
        assertThat(TaskRequest.getDescriptor().findFieldByName("resource_grant").getMessageType())
                .isEqualTo(StageResourceGrant.getDescriptor());
        assertThat(TaskRequest.getDescriptor().findFieldByName("splits").getMessageType())
                .isEqualTo(DataSplit.getDescriptor());
    }

    @Test
    void restEnvelopeDoesNotMirrorCanonicalTaskFields() {
        assertThat(Arrays.stream(SubmitTaskRequest.class.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .map(field -> field.getName()).toList())
                .containsExactlyInAnyOrder(
                        "canonicalTaskRequest", "canonicalBytes", "taskRequest",
                        "copySourceRead");
        assertThat(TaskRequest.getDescriptor().getFields())
                .extracting(FieldDescriptor::getName)
                .doesNotContain("exchange_sources", "dynamic_filters", "output_config",
                        "vector_search", "search_request");
    }

    @Test
    void resourceAndWriteFieldsHaveCanonicalV2Destinations() {
        assertThat(SubmitTaskRequest.class.getDeclaredMethods())
                .extracting(method -> method.getName())
                .doesNotContain("resourceGrant", "resourceLimit", "writeConfig");
        assertThat(StageResourceGrant.getDescriptor().getFields())
                .extracting(FieldDescriptor::getName)
                .contains("native_memory_bytes", "cpu_slots", "spill_bytes")
                .doesNotContain("spill_directory");

        assertThat(Arrays.stream(WriteConfig.class.getRecordComponents())
                .map(RecordComponent::getName).toList())
                .containsExactly("targetTable", "writeMode", "partitionSpec",
                        "fileFormat", "targetFileSize", "outputBindingBase64");
        assertThat(WriteTaskOptions.getDescriptor().getFields())
                .extracting(FieldDescriptor::getName)
                .containsExactly("target_table", "write_mode", "partition_spec",
                        "file_format", "target_file_size", "output_binding");
        assertThat(WriteTaskOptions.getDescriptor().findFieldByName("output_binding")
                .getMessageType().getName()).isEqualTo("WriteOutputBinding");
    }

    @Test
    void publicRestRecordsContainNoObjectTypedProtocolFields() {
        assertThat(Arrays.stream(SubmitTaskRequest.class.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .anyMatch(field -> field.getType() == Object.class)).isFalse();
        assertThat(Arrays.stream(WriteConfig.class.getRecordComponents())
                .anyMatch(component -> component.getType() == Object.class)).isFalse();
    }
}
