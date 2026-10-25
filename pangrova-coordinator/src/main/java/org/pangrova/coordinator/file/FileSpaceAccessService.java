package org.pangrova.coordinator.file;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.FileSpaceAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageProfileRegistry;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceLayout;
import org.pangrova.spi.storage.file.FileSpaceRegistry;
import org.pangrova.spi.storage.file.FileSpaceType;
import org.pangrova.spi.storage.file.ObjectLocation;

/**
 * Authorizes one file operation and issues the signed, credential-free descriptor the
 * Worker needs to execute it.
 *
 * <p>The coordinator never handles file bytes: it resolves the space root, checks the
 * Volume or Stage privilege, binds the descriptor to a single operation and expiry, and
 * signs it with the internal shared secret. Storage credentials stay inside the Worker
 * secret boundary.</p>
 */
public final class FileSpaceAccessService {

    /** Operations a descriptor may authorize. */
    static final Set<String> OPERATIONS = Set.of("head", "list", "read", "write", "delete");

    private final FileSpaceRegistry spaces;
    private final StorageProfileRegistry profiles;
    private final FileSpaceAccessControl access;
    private final FileSpaceAccessContext context;
    private final ObjectMapper mapper = JsonMapperFactory.create();
    private final AtomicInteger nextWorker = new AtomicInteger();

    /** Creates the issuer over durable space/profile registries and the caller's privileges. */
    public FileSpaceAccessService(FileSpaceRegistry spaces, StorageProfileRegistry profiles,
            FileSpaceAccessControl access, FileSpaceAccessContext context) {
        this.spaces = Objects.requireNonNull(spaces, "spaces must not be null");
        this.profiles = Objects.requireNonNull(profiles, "profiles must not be null");
        this.access = Objects.requireNonNull(access, "access must not be null");
        this.context = Objects.requireNonNull(context, "context must not be null");
    }

    /**
     * Issues a signed descriptor for one operation on one path.
     *
     * @param kind      {@code volumes} or {@code stages}; the two are separate surfaces
     * @param operation one of head, list, read, write, or delete
     */
    public FileAccessGrant grant(UserIdentity user, String kind, String catalog, String schema,
            String name, String path, String operation) {
        Resolved resolved = resolve(user, kind, catalog, schema, name, path, operation);
        FileAccessGrant grant = resolved.grant();
        context.audit().fileOperation(resolved.principal().principal(),
                tenantId(resolved.principal()), resolved.space(), grant.path(),
                grant.operation(), grant.workerUrl());
        return grant;
    }

    /**
     * Issues the descriptor behind a signed URL and records the issuance separately.
     *
     * <p>The grant is a plain {@code read} or {@code write} ticket so the Worker
     * applies the same operation check as any other transfer.</p>
     */
    public FileAccessGrant urlGrant(UserIdentity user, String kind, String catalog,
            String schema, String name, String path, boolean read, long ttlSeconds) {
        Resolved resolved = resolve(user, kind, catalog, schema, name, path,
                read ? "read" : "write");
        FileAccessGrant grant = resolved.grant();
        context.audit().urlIssued(resolved.principal().principal(),
                tenantId(resolved.principal()), resolved.space(), grant.path(),
                read ? "presigned-read" : "presigned-write", ttlSeconds, grant.workerUrl());
        return grant;
    }

    private Resolved resolve(UserIdentity user, String kind, String catalog, String schema,
            String name, String path, String operation) {
        UserIdentity principal = requireUser(user);
        FileSpaceType type = parseKind(kind);
        String normalizedOperation = normalizeOperation(operation);
        FileSpace space = spaces.find(type, catalog, schema, name)
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                        kind + " not found: " + catalog + "." + schema + "." + name));
        authorize(principal, space, normalizedOperation);
        return descriptor(principal, space, path, normalizedOperation);
    }

    /** Resolves and authorizes a long-lived Stage link while the caller holds the authority gate. */
    FileSpace stageLink(UserIdentity user, String catalog, String schema, String name, String path) {
        UserIdentity principal = requireUser(user);
        FileSpace space = spaces.find(FileSpaceType.STAGE, catalog, schema, name)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                        "Stage not found: " + catalog + "." + schema + "." + name));
        authorizeStageLink(principal, space);
        String relative = requireObjectPath(path);
        context.audit().urlIssued(principal.principal(), tenantId(principal), space, relative,
                "stage", 0, "");
        return space;
    }

    /** Re-authorizes one immutable Stage ID before issuing its bounded transfer ticket. */
    FileAccessGrant stageRead(UserIdentity user, UUID id, String path) {
        UserIdentity principal = requireUser(user);
        FileSpace space = spaces.find(id).filter(value -> value.type() == FileSpaceType.STAGE)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                        "Stage link no longer exists"));
        authorizeStageLink(principal, space);
        Resolved resolved = descriptor(principal, space, requireObjectPath(path), "read");
        context.audit().fileOperation(principal.principal(), tenantId(principal), space,
                resolved.grant().path(), "read", resolved.grant().workerUrl());
        return resolved.grant();
    }

    private void authorizeStageLink(UserIdentity user, FileSpace space) {
        if (space.external()) {
            access.checkCanUseStage(user, space.catalog(), space.schema(), space.name());
        } else {
            access.checkCanReadStage(user, space.catalog(), space.schema(), space.name());
        }
    }

    static String requireObjectPath(String path) {
        String normalized = normalizeRelativePath(path);
        if (normalized.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Stage file URL requires a non-empty object path");
        }
        return normalized;
    }

    private Resolved descriptor(UserIdentity principal, FileSpace space, String path,
            String normalizedOperation) {
        StorageProfile profile = profileFor(space);
        String baseKey = space.external()
                ? FileSpaceLayout.externalBaseKey(space, profile)
                : FileSpaceLayout.managedKey(space, profile);
        String normalizedPath = normalizeRelativePath(path);
        // Validating against the resolved root rejects escapes before a descriptor exists.
        new ObjectLocation(profile, baseKey).key(normalizedPath);
        String workerUrl = selectWorker();
        long expiresAt = context.clock().getAsLong() + context.ttl().toMillis();
        String encoded = encode(new FileAccessDescriptorPayload(profile.name(), profile.type(),
                profile.bucket(), profile.prefix(), profile.endpoint(), profile.region(),
                profile.isPathStyleAccess(), baseKey, normalizedPath, normalizedOperation,
                expiresAt));
        String signature = FileAccessSignature.sign(context.signingSecret(), encoded);
        return new Resolved(space, principal, new FileAccessGrant(workerUrl, encoded, signature,
                normalizedOperation, normalizedPath, expiresAt));
    }

    private void authorize(UserIdentity user, FileSpace space, String operation) {
        if (space.type() == FileSpaceType.VOLUME) {
            access.checkCanReadVolume(user, space.catalog(), space.schema(), space.name());
            if (writes(operation)) {
                access.checkCanWriteVolume(user, space.catalog(), space.schema(), space.name());
            }
            return;
        }
        access.checkCanUseStage(user, space.catalog(), space.schema(), space.name());
        if (writes(operation)) {
            access.checkCanWriteStage(user, space.catalog(), space.schema(), space.name());
        } else {
            access.checkCanReadStage(user, space.catalog(), space.schema(), space.name());
        }
    }

    private static boolean writes(String operation) {
        return "write".equals(operation) || "delete".equals(operation);
    }

    private StorageProfile profileFor(FileSpace space) {
        String name = space.storageProfile().isBlank()
                ? context.managedProfileName() : space.storageProfile();
        if (name.isBlank()) {
            throw notConfigured(space, "no storage profile is configured");
        }
        Optional<StorageProfile> profile = profiles.get(name);
        if (profile.isEmpty()) {
            throw notConfigured(space, "storage profile '" + name + "' does not exist");
        }
        return profile.get();
    }

    private String selectWorker() {
        List<String> endpoints = context.workerEndpoints().get().stream()
                .filter(endpoint -> endpoint != null && !endpoint.isBlank())
                .toList();
        if (endpoints.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "No active Worker is available to serve file bytes");
        }
        return endpoints.get(Math.floorMod(nextWorker.getAndIncrement(), endpoints.size()));
    }

    private String encode(FileAccessDescriptorPayload payload) {
        try {
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(mapper.writeValueAsBytes(payload));
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("File access descriptor is not serializable", failure);
        }
    }

    private static UserIdentity requireUser(UserIdentity user) {
        if (user == null || user.principal() == null || user.principal().isBlank()) {
            throw new AccessDeniedException("Authenticated principal is required for file access");
        }
        return user;
    }

    private static FileSpaceType parseKind(String kind) {
        String normalized = kind == null ? "" : kind.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "volumes", "volume" -> FileSpaceType.VOLUME;
            case "stages", "stage" -> FileSpaceType.STAGE;
            default -> throw new PangrovaException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "File space kind must be 'volumes' or 'stages'");
        };
    }

    private static String normalizeOperation(String operation) {
        String normalized = operation == null ? "" : operation.trim().toLowerCase(Locale.ROOT);
        if (!OPERATIONS.contains(normalized)) {
            throw new PangrovaException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "File operation must be one of head, list, read, write, delete");
        }
        return normalized;
    }

    /** Returns the space-relative path kept in the descriptor after rejecting unsafe input. */
    private static String normalizeRelativePath(String path) {
        String relative = path == null ? "" : path.trim();
        while (relative.startsWith("/")) {
            relative = relative.substring(1);
        }
        if (relative.isEmpty()) {
            return "";
        }
        if (relative.indexOf('%') >= 0 || relative.indexOf('\\') >= 0
                || relative.indexOf('?') >= 0 || relative.indexOf('#') >= 0) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "File path contains a reserved character");
        }
        for (String segment : relative.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                        "File path contains an unsafe segment");
            }
        }
        return relative;
    }

    private static String tenantId(UserIdentity user) {
        return user.attributes().get("tenant_id");
    }

    private static PangrovaException notConfigured(FileSpace space, String detail) {
        return new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                "Storage is not configured for " + space.qualifiedName() + ": " + detail);
    }

    /**
     * Immutable settings for descriptor issuance.
     *
     * @param managedProfileName fallback profile for spaces without an explicit one
     * @param ttl                descriptor lifetime
     * @param clock              epoch-millisecond clock
     * @param workerEndpoints    active Worker base URLs
     * @param audit              file-operation audit sink
     * @param signingSecret      coordinator/Worker shared secret
     */
    public record FileSpaceAccessContext(String managedProfileName, Duration ttl,
            LongSupplier clock, Supplier<List<String>> workerEndpoints,
            FileSpaceAuditEmitter audit, String signingSecret) {

        public FileSpaceAccessContext {
            Objects.requireNonNull(ttl, "ttl must not be null");
            Objects.requireNonNull(clock, "clock must not be null");
            Objects.requireNonNull(workerEndpoints, "workerEndpoints must not be null");
            Objects.requireNonNull(audit, "audit must not be null");
            if (ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException("descriptor ttl must be positive");
            }
            if (signingSecret == null || signingSecret.isBlank()) {
                throw new IllegalArgumentException("descriptor signing secret is required");
            }
            managedProfileName = managedProfileName == null ? "" : managedProfileName.trim();
        }

        /** Convenience factory with the standard five-minute descriptor lifetime. */
        public static FileSpaceAccessContext of(String managedProfileName,
                Supplier<List<String>> workerEndpoints, FileSpaceAuditEmitter audit,
                String signingSecret) {
            return new FileSpaceAccessContext(managedProfileName, Duration.ofMinutes(5),
                    System::currentTimeMillis, workerEndpoints, audit, signingSecret);
        }
    }

    /**
     * Wire shape of a signed descriptor; field names must match the Worker record exactly.
     */
    record FileAccessDescriptorPayload(String profileName,
            org.pangrova.spi.storage.StorageType type, String bucket, String prefix,
            String endpoint, String region, boolean pathStyleAccess, String baseKey,
            String path, String operation, long expiresAtEpochMillis) {
    }

    /** One authorized file operation, ready to send to the named Worker. */
    public record FileAccessGrant(String workerUrl, String descriptor, String signature,
            String operation, String path, long expiresAtEpochMillis) {

        public Instant expiresAt() {
            return Instant.ofEpochMilli(expiresAtEpochMillis);
        }
    }

    private record Resolved(FileSpace space, UserIdentity principal, FileAccessGrant grant) {
    }
}
