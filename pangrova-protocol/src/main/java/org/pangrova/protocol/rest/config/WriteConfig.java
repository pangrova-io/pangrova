package org.pangrova.protocol.rest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration for write tasks targeting external tables.
 */
public record WriteConfig(
    @JsonProperty("target_table") String targetTable,
    @JsonProperty("write_mode") String writeMode,
    @JsonProperty("partition_spec") List<String> partitionSpec,
    @JsonProperty("file_format") String fileFormat,
    @JsonProperty("target_file_size") long targetFileSize,
    @JsonProperty("output_binding_base64") String outputBindingBase64
) {

    public WriteConfig {
        partitionSpec = partitionSpec == null ? List.of() : List.copyOf(partitionSpec);
    }
}
