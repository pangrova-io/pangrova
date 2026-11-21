package org.pangrova.ai.worker.function;

import org.pangrova.ai.worker.context.AiTestTaskAuthority;
import org.pangrova.types.value.ColumnValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Exercises the real default factory with a process-owned environment; makes no object-service request. */
public final class AiFunctionFactoryStorageProbe {
    private AiFunctionFactoryStorageProbe() {}

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        Path objects = Path.of(args[1]);
        Path staging = Path.of(args[2]);
        try (var factory = new AiFunctionFactory()) {
            var config = new HashMap<String, String>();
            config.put("file.storage-staging-root", staging.toString());
            config.put("file.capability-secret-reference", "env://PANGROVA_TEST_FILE_CAPABILITY_KEY");
            switch (mode) {
                case "local" -> writeLocal(factory, config, objects);
                case "invalid-local" -> {
                    config.put("file.storage-backend", "local");
                    expectRejected(factory, config, "file.storage.local.root is required");
                    config.put("file.storage-backend", "unsupported-backend");
                    expectRejected(factory, config, "Unsupported FILE storage backend");
                    require(!Files.exists(staging), "Rejected backend opened staging");
                }
                case "missing-oss" -> {
                    config.put("file.storage-backend", "oss");
                    config.put("file.storage.s3.bucket", "must-not-be-used");
                    expectRejected(factory, config, "file.storage.oss.bucket is required");
                    require(!Files.exists(staging), "Missing OSS config opened staging");
                }
                case "oss" -> {
                    config.putAll(Map.of("file.storage-backend", "oss",
                            "file.storage.oss.bucket", "factory-startup",
                            "file.storage.oss.region", "cn-hangzhou",
                            "file.storage.oss.endpoint", "https://oss-startup.invalid",
                            "file.storage.oss.sse", "AES256"));
                    require(factory.getScalarFunctions(config).stream().anyMatch(
                            function -> function.getMetadata().name().equals("file_from_bytes")),
                            "The production OSS FILE factory did not register");
                    require(Files.isDirectory(staging), "OSS staging was not initialized");
                }
                default -> throw new IllegalArgumentException("Unknown probe mode: " + mode);
            }
        }
        System.out.println("FILE_FACTORY_PROBE_PASSED:" + mode);
    }

    private static void writeLocal(AiFunctionFactory factory, Map<String, String> config, Path objects)
            throws Exception {
        config.put("file.storage-backend", "local");
        config.put("file.storage.local.root", objects.toString());
        var functions = factory.getScalarFunctions(config);
        require(Files.isDirectory(objects), "Local object directory was not initialized");
        var fromBytes = functions.stream()
                .filter(function -> function.getMetadata().name().equals("file_from_bytes"))
                .findFirst().orElseThrow();
        byte[] input = {1, 2, 3};
        var value = new ColumnValue.BytesValue(input);
        input[0] = 99;
        ColumnValue result = AiTestTaskAuthority.callWithRegisteredTask("tenant", "principal",
                java.time.Instant.now().plusSeconds(30).toEpochMilli(), 1024,
                () -> fromBytes.invoke(List.of(value, new ColumnValue.StringValue("text/plain"))));
        require(result instanceof ColumnValue.RowValue, "FILE_FROM_BYTES did not return its typed row");
        var row = (ColumnValue.RowValue) result;
        require(Long.valueOf(3).equals(row.fields().get(2)), "Stored object length changed");
        String checksum = java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(new byte[]{1, 2, 3}));
        require(checksum.equals(row.fields().get(3)), "Stored object checksum changed");
    }

    private static void expectRejected(AiFunctionFactory factory, Map<String, String> config, String message) {
        try {
            factory.getScalarFunctions(config);
        } catch (IllegalArgumentException expected) {
            require(expected.getMessage().contains(message), "Unexpected rejection: " + expected.getMessage());
            return;
        }
        throw new AssertionError("The invalid storage configuration was accepted");
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new AssertionError(message);
        }
    }
}
