package org.pangrova.testing.testkit;

import java.util.Locale;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * MinIO container factory for tests.
 *
 * <p>The upstream {@code minio/minio} Docker Hub repository is no longer
 * publicly pullable. The default is the last public Bitnami MinIO build, whose
 * image wraps the server in an init script and runs unprivileged; tests pass
 * {@code server /data} directly, so the factory normalizes the entrypoint and
 * user. Override the image with {@code -Dpangrova.test.minio-image=<image>}
 * when a registry mirror is available.</p>
 */
public final class MinioContainer {

    /** System property selecting an alternative MinIO image. */
    public static final String IMAGE_PROPERTY = "pangrova.test.minio-image";

    /** Last publicly pullable MinIO build used by the test suite. */
    public static final String DEFAULT_IMAGE = "bitnamilegacy/minio:2024.12.18";

    private MinioContainer() {}

    /** Creates an unstarted MinIO container with port 9000 exposed. */
    public static GenericContainer<?> create() {
        return create(System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE));
    }

    /** Creates an unstarted MinIO container from an explicit image reference. */
    public static GenericContainer<?> create(String image) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(image))
                .withExposedPorts(9000);
        if (image.toLowerCase(Locale.ROOT).contains("bitnami")) {
            container.withCreateContainerCmdModifier(command -> command
                    .withEntrypoint("/opt/bitnami/minio/bin/minio")
                    .withUser("root"));
        }
        return container;
    }
}
