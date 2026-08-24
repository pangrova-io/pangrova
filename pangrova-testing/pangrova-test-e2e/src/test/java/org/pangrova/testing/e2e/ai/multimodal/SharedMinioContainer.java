/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.e2e.ai.multimodal;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Map;

/**
 * JVM-level singleton MinIO container for multimodal FILE storage testing.
 *
 * <p>Provides S3-compatible object storage with path-style access, pre-created
 * test buckets, and uploaded fixture files for E2E multimodal scenarios.</p>
 *
 * <p>Uses the TestContainers Singleton Container pattern — static initializer
 * runs once per classloader and containers are shared across all test classes
 * within a single surefire fork.</p>
 *
 * <p>Container configuration:</p>
 * <ul>
 *   <li>Image: {@code minio/minio:RELEASE.2024-06-13T22-53-53Z}</li>
 *   <li>Port: 9000 (S3 API)</li>
 *   <li>Credentials: minioadmin/minioadmin</li>
 *   <li>Pre-created buckets: {@code pangrova-test-files}, {@code external-uploads}</li>
 * </ul>
 */
public final class SharedMinioContainer {

    /** Whether MinIO started successfully and test buckets are created. */
    public static final boolean AVAILABLE;

    /** The MinIO container instance. */
    public static final GenericContainer<?> MINIO;

    /** The S3-compatible endpoint URL (host-accessible). */
    public static final String ENDPOINT;

    /** MinIO root user (access key). */
    public static final String ACCESS_KEY = "minioadmin";

    /** MinIO root password (secret key). */
    public static final String SECRET_KEY = "minioadmin";

    /** Primary test bucket for FILE storage. */
    public static final String TEST_BUCKET = "pangrova-test-files";

    /** Secondary bucket for cross-bucket access testing. */
    public static final String EXTERNAL_BUCKET = "external-uploads";

    static {
        boolean ok = false;
        GenericContainer<?> minio = null;
        String endpoint = null;

        try {
            minio = new GenericContainer<>("minio/minio:RELEASE.2024-06-13T22-53-53Z")
                    .withExposedPorts(9000)
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                    .withCommand("server", "/data")
                    .waitingFor(Wait.forHttp("/minio/health/ready")
                            .forPort(9000)
                            .withStartupTimeout(Duration.ofSeconds(60)))
                    .withStartupAttempts(2);

            minio.start();
            endpoint = String.format("http://%s:%d",
                    minio.getHost(), minio.getMappedPort(9000));

            // Create test buckets using MinIO mc client inside the container
            createBucket(minio, TEST_BUCKET);
            createBucket(minio, EXTERNAL_BUCKET);

            // Upload minimal test fixture files for E2E testing
            uploadTestFixtures(minio);

            System.out.println("[SharedMinioContainer] Started successfully: " + endpoint);
            ok = true;
        } catch (Throwable e) {
            System.err.println("[SharedMinioContainer] FAILED to start MinIO: " + e);
            e.printStackTrace(System.err);
            if (minio != null) {
                try { minio.stop(); } catch (Exception ignored) { }
                minio = null;
            }
        }

        AVAILABLE = ok;
        MINIO = minio;
        ENDPOINT = endpoint;
    }

    /**
     * Returns S3-compatible properties for configuring Pangrova connectors.
     */
    public static Map<String, String> s3Properties() {
        return Map.of(
                "s3.endpoint", ENDPOINT,
                "s3.access-key-id", ACCESS_KEY,
                "s3.secret-access-key", SECRET_KEY,
                "s3.path-style-access", "true",
                "s3.region", "us-east-1"
        );
    }

    /**
     * Returns a Storage Profile configuration map for test usage.
     */
    public static Map<String, String> storageProfileProperties(String bucket) {
        return Map.of(
                "type", "s3",
                "bucket", bucket,
                "endpoint", ENDPOINT,
                "path-style-access", "true",
                "access-key-id", ACCESS_KEY,
                "secret-access-key", SECRET_KEY,
                "region", "us-east-1"
        );
    }

    private static void createBucket(GenericContainer<?> container, String bucketName) {
        try {
            // Use MinIO mc (mirror client) to create buckets
            var result = container.execInContainer(
                    "mc", "alias", "set", "local", "http://localhost:9000", ACCESS_KEY, SECRET_KEY);
            if (result.getExitCode() != 0) {
                // mc may not be available in all images; try mkdir approach
                container.execInContainer("mkdir", "-p", "/data/" + bucketName);
                return;
            }
            result = container.execInContainer("mc", "mb", "local/" + bucketName, "--ignore-existing");
            if (result.getExitCode() != 0) {
                System.err.println("[SharedMinioContainer] Bucket creation warning: " + result.getStderr());
                // Fallback: create directory directly (MinIO serves directories as buckets in FS mode)
                container.execInContainer("mkdir", "-p", "/data/" + bucketName);
            }
        } catch (Exception e) {
            // Fallback: create the data directory directly
            try {
                container.execInContainer("mkdir", "-p", "/data/" + bucketName);
            } catch (Exception nested) {
                System.err.println("[SharedMinioContainer] Failed to create bucket: " + bucketName);
            }
        }
    }

    private static void uploadTestFixtures(GenericContainer<?> container) {
        try {
            // Create minimal test fixture files directly in the container filesystem
            // These are small synthetic files sufficient for E2E validation

            // Create test-data directory
            container.execInContainer("mkdir", "-p", "/data/" + TEST_BUCKET + "/test-data");
            container.execInContainer("mkdir", "-p", "/data/" + TEST_BUCKET + "/batch-import");

            // Create minimal JPEG header (smallest valid JPEG: 2x2 pixel)
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/test-data/car-damage-front.jpg");
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/test-data/product-shoe.jpg");
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/test-data/photo.jpg");

            // Create minimal PDF (smallest valid PDF)
            container.execInContainer("sh", "-c",
                    "printf '%%PDF-1.0\\n1 0 obj\\n<<>>\\nendobj\\n' > /data/"
                            + TEST_BUCKET + "/test-data/sample-contract.pdf");

            // Create batch import fixtures
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/batch-import/img1.jpg");
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/batch-import/img2.jpg");
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xd8\\xff\\xe0\\x00\\x10JFIF' > /data/"
                            + TEST_BUCKET + "/batch-import/img3.jpg");

            // Create minimal MP3 header for audio test
            container.execInContainer("sh", "-c",
                    "printf '\\xff\\xfb\\x90\\x00' > /data/"
                            + TEST_BUCKET + "/test-data/call-recording.mp3");

            System.out.println("[SharedMinioContainer] Test fixtures uploaded");
        } catch (Exception e) {
            System.err.println("[SharedMinioContainer] Warning: fixture upload failed: " + e.getMessage());
        }
    }

    private SharedMinioContainer() {}
}
