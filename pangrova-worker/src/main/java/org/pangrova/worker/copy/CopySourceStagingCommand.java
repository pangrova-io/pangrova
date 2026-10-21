package org.pangrova.worker.copy;

import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.storage.CopySourceEndpointPolicy;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.worker.ai.blob.S3BlobStorage;
import org.pangrova.worker.ai.blob.CopyS3ClientPolicy;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** Worker-owned exact S3 read and managed staging for one admitted COPY attempt. */
public final class CopySourceStagingCommand {
    private final CopySourceReadClient admissions;
    private final AwsCredentialsProvider sourceCredentials;
    private final S3BlobStorage managedStorage;
    private final Path stagingRoot;
    private final long maxSourceBytes;
    private final boolean allowLoopbackSourceTest;

    /** Uses the Worker default credential chain and the admitted exact profile configuration. */
    public CopySourceStagingCommand(CopySourceReadClient admissions,
                                    S3BlobStorage managedStorage, Path stagingRoot, long maxSourceBytes) {
        this(admissions, DefaultCredentialsProvider.create(), managedStorage,
                stagingRoot, maxSourceBytes, false);
    }

    /** Uses an explicitly configured Worker-local credential provider. */
    public CopySourceStagingCommand(CopySourceReadClient admissions,
                                    AwsCredentialsProvider sourceCredentials,
                                    S3BlobStorage managedStorage, Path stagingRoot, long maxSourceBytes) {
        this(admissions, sourceCredentials, managedStorage, stagingRoot, maxSourceBytes, false);
    }

    /** Explicit local MinIO integration transport; cannot accept a remote HTTP source. */
    public static CopySourceStagingCommand forLoopbackTest(
            CopySourceReadClient admissions, AwsCredentialsProvider sourceCredentials,
            S3BlobStorage managedStorage, Path stagingRoot, long maxSourceBytes) {
        return new CopySourceStagingCommand(admissions, sourceCredentials,
                managedStorage, stagingRoot, maxSourceBytes, true);
    }

    private CopySourceStagingCommand(CopySourceReadClient admissions,
                                     AwsCredentialsProvider sourceCredentials,
                                     S3BlobStorage managedStorage, Path stagingRoot,
                                     long maxSourceBytes, boolean allowLoopbackSourceTest) {
        this.admissions = Objects.requireNonNull(admissions, "admissions");
        this.sourceCredentials = Objects.requireNonNull(sourceCredentials, "sourceCredentials");
        this.managedStorage = Objects.requireNonNull(managedStorage, "managedStorage");
        this.stagingRoot = Objects.requireNonNull(stagingRoot, "stagingRoot");
        if (maxSourceBytes <= 0) {
            throw new IllegalArgumentException("COPY source size limit must be positive");
        }
        this.maxSourceBytes = maxSourceBytes;
        this.allowLoopbackSourceTest = allowLoopbackSourceTest;
    }

    /** Rechecks authority before and after I/O; a retry reads only immutable managed content. */
    public CopySourceReadHttp.StagedSource execute(String nonce, CopySourceReadHttp.Scope scope)
            throws IOException {
        requireManagedStorage(scope);
        var redeemed = admissions.redeem(nonce, scope);
        StorageProfile sourceProfile = sourceProfile(redeemed.profile());
        S3VersionedCopyStager.StagedSource staged;
        try (S3Client source = sourceClient(sourceProfile)) {
            var stager = new S3VersionedCopyStager(source, managedStorage,
                    stagingRoot, maxSourceBytes);
            staged = redeemed.stagedSource() == null
                    ? stager.stage(sourceProfile, scope.canonicalSourceUrl(), scope.tenantId())
                    : stager.resume(sourceProfile, scope.canonicalSourceUrl(), scope.tenantId(),
                            existing(sourceProfile.name(), scope, redeemed.stagedSource()));
        }
        var proof = new CopySourceReadHttp.StagedSource(staged.sourceVersionId(),
                staged.managedObjectUri(), staged.contentSha256(), staged.contentSize(),
                staged.tenantScope());
        return admissions.record(nonce, scope, proof);
    }

    private void requireManagedStorage(CopySourceReadHttp.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        if (!managedStorage.rootUri().equals(scope.managedStorageRootUri())
                || !managedStorage.backendIdentitySha256()
                        .equals(scope.managedBackendIdentitySha256())) {
            throw new AccessDeniedException("COPY managed storage does not match admitted backend");
        }
    }

    /** Rechecks current authority before returning a locally cached attempt proof. */
    public void verifyRecordedProof(String nonce, CopySourceReadHttp.Scope scope,
                                    CopySourceReadHttp.StagedSource proof) {
        var redeemed = admissions.redeem(nonce, scope);
        if (!proof.equals(redeemed.stagedSource())) {
            throw new AccessDeniedException("COPY cached staged proof is no longer admitted");
        }
    }

    /** Produces a verified task-private Parquet file without trusting a network file path. */
    public VerifiedLocalCopySource executeForNative(String nonce, CopySourceReadHttp.Scope scope)
            throws IOException {
        return executeForNative(nonce, scope, CopyMaterialization.standalone());
    }

    /** Executing tasks must use their actual shared grant rather than standalone preparation. */
    public VerifiedLocalCopySource executeForNative(String nonce, CopySourceReadHttp.Scope scope,
                                                   CopyMaterialization ownership) throws IOException {
        Objects.requireNonNull(ownership, "ownership");
        requireManagedStorage(scope);
        var redeemed = admissions.redeem(nonce, scope);
        var proof = redeemed.stagedSource();
        if (proof == null) {
            throw new IllegalStateException("COPY Native scan requires a previously staged source proof");
        }
        StorageProfile profile = sourceProfile(redeemed.profile());
        VerifiedLocalCopySource local = null;
        try (S3Client source = sourceClient(profile)) {
            var stager = new S3VersionedCopyStager(source, managedStorage,
                    stagingRoot, maxSourceBytes);
            local = stager.materialize(profile, scope.canonicalSourceUrl(), scope.tenantId(),
                    existing(profile.name(), scope, proof), ownership);
        } catch (IOException | RuntimeException | Error failure) {
            closeOnFailure(local, failure);
            throw failure;
        }
        try {
            if (!proof.equals(admissions.redeem(nonce, scope).stagedSource())) {
                throw new AccessDeniedException("COPY staged proof changed after local scan materialization");
            }
            return local;
        } catch (RuntimeException | Error failure) {
            closeOnFailure(local, failure);
            throw failure;
        }
    }

    private static void closeOnFailure(VerifiedLocalCopySource local, Throwable failure) {
        if (local == null) {
            return;
        }
        try {
            local.close();
        } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    private static StorageProfile sourceProfile(CopySourceReadHttp.Profile publicProfile) {
        return new StorageProfile(publicProfile.name(), publicProfile.type(),
                publicProfile.bucket(), publicProfile.prefix(), publicProfile.endpoint(),
                publicProfile.region(), Map.of(), Map.of(
                        "path-style-access", Boolean.toString(publicProfile.pathStyleAccess())));
    }

    private S3Client sourceClient(StorageProfile profile) {
        if (profile.region().isBlank()) {
            throw new IllegalArgumentException("COPY S3 source region is required");
        }
        var builder = S3Client.builder().region(Region.of(profile.region()))
                .credentialsProvider(sourceCredentials)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(profile.isPathStyleAccess()).build());
        builder.endpointOverride(CopySourceEndpointPolicy.require(
                profile.endpoint(), allowLoopbackSourceTest));
        CopyS3ClientPolicy.configure(builder);
        return builder.build();
    }

    private static S3VersionedCopyStager.StagedSource existing(
            String profileName, CopySourceReadHttp.Scope scope,
            CopySourceReadHttp.StagedSource proof) {
        return new S3VersionedCopyStager.StagedSource(profileName,
                scope.canonicalSourceUrl(), proof.sourceVersionId(), proof.managedObjectUri(),
                proof.tenantScope(), proof.contentSha256(), proof.contentSize());
    }
}
