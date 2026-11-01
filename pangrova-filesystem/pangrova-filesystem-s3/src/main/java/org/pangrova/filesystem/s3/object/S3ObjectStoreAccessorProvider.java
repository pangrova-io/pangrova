package org.pangrova.filesystem.s3.object;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.pangrova.filesystem.api.object.ObjectStoreAccessorProvider;
import org.pangrova.common.storage.StorageType;
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
 * Owns S3 SDK clients, presigners and cached accessors for complete immutable profiles.
 *
 * <p>Credential-free profiles (the only kind that may cross the coordinator
 * boundary) resolve their credentials from the supplied provider, which is the
 * Worker secret boundary. Profiles carrying credentials are honored only when
 * they are configured locally.</p>
 */
public final class S3ObjectStoreAccessorProvider implements ObjectStoreAccessorProvider {

    private static final String DEFAULT_REGION = "us-east-1";
    private static final java.util.Set<String> S3_CREDENTIAL_KEYS = java.util.Set.of(
            "access-key-id", "secret-access-key", "session-token");

    private final AwsCredentialsProvider defaultCredentials;
    private final boolean ownsCredentials;
    private final java.util.Set<String> conditionalDeleteEndpoints;
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final CompletableFuture<Void> closeComplete = new CompletableFuture<>();
    private boolean closed;
    private final Map<StorageProfile, ObjectStoreAccessor> accessors = new ConcurrentHashMap<>();
    private final Map<StorageProfile, S3Client> clients = new ConcurrentHashMap<>();
    private final Map<StorageProfile, S3Presigner> presigners = new ConcurrentHashMap<>();

    /** Borrows the caller's credential provider; only this factory's clients are owned. */
    public S3ObjectStoreAccessorProvider(AwsCredentialsProvider defaultCredentials) {
        this(defaultCredentials, false);
    }

    /** Borrows credentials and installs exact custom endpoints qualified for conditional DELETE. */
    public S3ObjectStoreAccessorProvider(AwsCredentialsProvider defaultCredentials,
            java.util.Set<String> conditionalDeleteEndpoints) {
        this(defaultCredentials, false, conditionalDeleteEndpoints);
    }

    S3ObjectStoreAccessorProvider(AwsCredentialsProvider defaultCredentials, boolean ownsCredentials) {
        this(defaultCredentials, ownsCredentials, java.util.Set.of());
    }

    private S3ObjectStoreAccessorProvider(AwsCredentialsProvider defaultCredentials, boolean ownsCredentials,
            java.util.Set<String> conditionalDeleteEndpoints) {
        this.conditionalDeleteEndpoints = java.util.Set.copyOf(conditionalDeleteEndpoints);
        this.defaultCredentials = Objects.requireNonNull(defaultCredentials,
                "defaultCredentials must not be null");
        this.ownsCredentials = ownsCredentials;
    }

    /** Uses the standard AWS credential chain when a profile declares none. */
    public S3ObjectStoreAccessorProvider() {
        this(DefaultCredentialsProvider.builder().build(), true);
    }

    /** Resolves a cached accessor while the factory remains open. */
    @Override
    public ObjectStoreAccessor accessor(StorageProfile profile) {
        var lock = lifecycle.readLock();
        lock.lock();
        try {
            if (closed) throw new IllegalStateException("Object store accessor factory is closed");
            return accessors.computeIfAbsent(profile, this::create);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StorageType storageType() {
        return StorageType.S3;
    }

    private ObjectStoreAccessor create(StorageProfile profile) {
        if (profile.type() != StorageType.S3) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "S3 accessor provider cannot serve " + profile.type());
        }
        return new S3ObjectStoreAccessor(s3Client(profile), s3Presigner(profile), conditionalDeleteEndpoints);
    }

    private S3Client s3Client(StorageProfile profile) {
        return clients.computeIfAbsent(profile, this::buildS3Client);
    }

    private S3Presigner s3Presigner(StorageProfile profile) {
        return presigners.computeIfAbsent(profile, this::buildS3Presigner);
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
        // SDK clients and presigners close credential providers they receive. Borrow
        // through a non-closeable view; only this owner may retire an owned chain.
        if (profile.credentials().isEmpty()) return defaultCredentials::resolveCredentials;
        if (!S3_CREDENTIAL_KEYS.containsAll(profile.credentials().keySet())) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Unsupported explicit S3 credentials");
        }
        String accessKey = profile.accessKeyId();
        String secretKey = profile.secretAccessKey();
        if (accessKey == null || accessKey.isBlank() || secretKey == null || secretKey.isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Incomplete explicit S3 credentials");
        }
        String sessionToken = profile.sessionToken();
        if (sessionToken == null || sessionToken.isBlank()) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        }
        return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
    }

    @Override
    public void close() {
        var owned = retire();
        if (owned != null) {
            try {
                RuntimeException failure = closeOwned(owned);
                if (failure == null) closeComplete.complete(null);
                else closeComplete.completeExceptionally(failure);
            } catch (Error failure) {
                closeComplete.completeExceptionally(failure);
                throw failure;
            }
        }
        try {
            closeComplete.join();
        } catch (java.util.concurrent.CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new IllegalStateException("Object store resource cleanup failed", failure.getCause());
        }
    }

    private java.util.List<AutoCloseable> retire() {
        java.util.List<AutoCloseable> owned = new java.util.ArrayList<>();
        var lock = lifecycle.writeLock();
        lock.lock();
        try {
            if (closed) return null;
            closed = true;
            owned.addAll(presigners.values());
            owned.addAll(clients.values());
            if (ownsCredentials && defaultCredentials instanceof AutoCloseable closeable) owned.add(closeable);
            presigners.clear();
            clients.clear();
            accessors.clear();
        } finally {
            lock.unlock();
        }
        return owned;
    }

    static RuntimeException closeOwned(java.util.List<AutoCloseable> owned) {
        RuntimeException failure = null;
        Error fatal = null;
        for (var resource : owned) {
            try {
                resource.close();
            } catch (Exception closeFailure) {
                if (failure == null) failure = new IllegalStateException("Object store resource cleanup failed");
                failure.addSuppressed(closeFailure);
            } catch (Error closeFailure) {
                if (fatal == null) fatal = closeFailure;
                else if (fatal != closeFailure) {
                    fatal.addSuppressed(closeFailure);
                }
            }
        }
        if (fatal != null) {
            if (failure != null) fatal.addSuppressed(failure);
            throw fatal;
        }
        return failure;
    }
}
