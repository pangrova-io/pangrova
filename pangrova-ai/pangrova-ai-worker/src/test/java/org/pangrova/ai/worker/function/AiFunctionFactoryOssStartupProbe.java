package org.pangrova.ai.worker.function;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Child-process factory registration with real environment credentials; makes no object-service request. */
public final class AiFunctionFactoryOssStartupProbe {
    private AiFunctionFactoryOssStartupProbe() {}

    public static void main(String[] args) throws Exception {
        Path staging = Path.of(args[0]);
        try (var factory = new AiFunctionFactory()) {
            var functions = factory.getScalarFunctions(Map.of(
                    "file.storage-backend", "oss",
                    "file.storage.oss.bucket", "factory-startup",
                    "file.storage.oss.region", "cn-hangzhou",
                    "file.storage.oss.endpoint", "https://oss-startup.invalid",
                    "file.storage.oss.sse", "AES256",
                    "file.storage-staging-root", staging.toString(),
                    "file.capability-secret-reference", "test://file-capability"));
            if (functions.stream().noneMatch(function -> function.getMetadata().name().equals("file_from_bytes"))
                    || !Files.isDirectory(staging)) {
                throw new IllegalStateException("The production OSS FILE factory did not register");
            }
        }
        System.out.println("OSS_FACTORY_REGISTERED_AND_CLOSED");
    }
}
