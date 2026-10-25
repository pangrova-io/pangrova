package org.pangrova.worker.file;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.file.ObjectStoreAccessor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Builds and caches one {@link ObjectStoreAccessor} per storage backend.
 *
 * <p>Credential-free profiles (the only kind that may cross the coordinator
 * boundary) resolve their credentials from the supplied provider, which is the
 * Worker secret boundary. Profiles carrying credentials are honored only when
 * they are configured locally.</p>
 */
public final class ObjectStoreAccessorFactory implements AutoCloseable {

    private static final String DEFAULT_REGION = "us-east-1";

    private final AwsCredentialsProvider defaultCredentials;
    private final Map<String, ObjectStoreAccessor> accessors = new ConcurrentHashMap<>();
    private final Map<String, S3Client> clients = new ConcurrentHashMap<>();
    private final Map<String, S3Presigner> presigners = new ConcurrentHashMap<>();

    public ObjectStoreAccessorFactory(AwsCredentialsProvider defaultCredentials) {
        this.defaultCredentials = Objects.requireNonNull(defaultCredentials,
                "defaultCredentials must not be null");
    }

    /** Uses the standard AWS credential chain when a profile declares none. */
    public ObjectStoreAccessorFactory() {
        this(DefaultCredentialsProvider.create());
    }

    public ObjectStoreAccessor accessor(StorageProfile profile) {
        String key = profileKey(profile);
        return accessors.computeIfAbsent(key, ignored -> create(profile));
    }

    private ObjectStoreAccessor create(StorageProfile profile) {
        return switch (profile.type()) {
            case LOCAL -> new LocalObjectStoreAccessor(Path.of(profile.bucket()));
            case S3 -> new S3ObjectStoreAccessor(s3Client(profile), s3Presigner(profile));
            default -> throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Storage type " + profile.type() + " is not implemented for file spaces");
        };
    }

    private S3Client s3Client(StorageProfile profile) {
        return clients.computeIfAbsent(profileKey(profile),
                ignored -> buildS3Client(profile));
    }

    private S3Presigner s3Presigner(StorageProfile profile) {
        return presigners.computeIfAbsent(profileKey(profile),
                ignored -> buildS3Presigner(profile));
    }

    private S3Presigner buildS3Presigner(StorageProfile profile) {
        S3Presigner.Builder builder = S3Presigner.builder()
                .region(Region.of(profile.region().isEmpty() ? DEFAULT_REGION : profile.region()))
                .credentialsProvider(credentials(profile));
        if (!profile.endpoint().isEmpty()) {
            builder.endpointOverride(URI.create(profile.endpoint()));
        }
        return builder.build();
    }

    private S3Client buildS3Client(StorageProfile profile) {
        var builder = S3Client.builder()
                .region(Region.of(profile.region().isEmpty() ? DEFAULT_REGION : profile.region()))
                .credentialsProvider(credentials(profile))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(profile.isPathStyleAccess()).build());
        if (!profile.endpoint().isEmpty()) {
            builder.endpointOverride(URI.create(profile.endpoint()));
        }
        return builder.build();
    }

    private AwsCredentialsProvider credentials(StorageProfile profile) {
        String accessKey = profile.accessKeyId();
        String secretKey = profile.secretAccessKey();
        if (accessKey == null || accessKey.isBlank() || secretKey == null || secretKey.isBlank()) {
            return defaultCredentials;
        }
        String sessionToken = profile.sessionToken();
        if (sessionToken == null || sessionToken.isBlank()) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        }
        return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
    }

    private static String profileKey(StorageProfile profile) {
        String credentialScope = profile.accessKeyId() == null ? "" : profile.accessKeyId();
        return profile.name() + '|' + profile.type() + '|' + profile.bucket() + '|'
                + profile.prefix() + '|' + profile.endpoint() + '|' + profile.region() + '|'
                + profile.isPathStyleAccess() + '|' + credentialScope;
    }

    @Override
    public void close() {
        presigners.values().forEach(S3Presigner::close);
        presigners.clear();
        clients.values().forEach(S3Client::close);
        clients.clear();
        accessors.clear();
    }
}
