package org.pangrova.testing.integration.e2e.file;

import java.util.Map;

/** One explicitly selected real FILE deployment retained until Native callbacks have drained. */
public sealed interface FileStorageDeployment extends AutoCloseable permits S3FileDeployment, LocalFileDeployment {
    String CAPABILITY_KEY_ENV = "PANGROVA_E2E_FILE_CAPABILITY_KEY";
    String BACKEND_PROPERTY = "pangrova.e2e.file.backend";

    /** Prepares production FILE configuration without initializing Native. */
    Map<String, String> prepareConfiguredDeployment() throws Exception;

    /** Selects one backend for the process-wide Native callback owner; unknown values fail closed. */
    static FileStorageDeployment configured() {
        return switch (System.getProperty(BACKEND_PROPERTY, "s3")) {
            case "s3" -> new S3FileDeployment();
            case "local" -> new LocalFileDeployment();
            default -> throw new IllegalArgumentException("Unknown E2E FILE backend: "
                    + System.getProperty(BACKEND_PROPERTY));
        };
    }
}
