package org.pangrova.file.core.space;

import org.pangrova.file.api.access.FileAccessGrant;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.file.api.space.FileOperationAudit;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.security.api.FileSpaceAccessControl;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.file.api.profile.StorageProfileRegistry;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceLayout;
import org.pangrova.file.api.space.FileSpaceRegistry;
import org.pangrova.common.file.FileSpaceType;
import org.pangrova.file.api.object.ObjectLocation;

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
        return context.authority().read(() -> {
            Resolved resolved = resolve(user, kind, catalog, schema, name, path, operation);
            FileAccessGrant grant = resolved.grant();
            context.audit().fileOperation(resolved.principal().principal(),
                    tenantId(resolved.principal()), resolved.space(), grant.path(),
                    grant.operation(), grant.workerUrl());
            return grant;
        });
    }

    /**
     * Captures authorization for a provider URL; issuance is recorded only after signing succeeds.
     *
     * <p>The grant is a plain {@code read} or {@code write} ticket so the Worker
     * applies the same operation check as any other transfer.</p>
     */
    UrlGrant urlGrant(UserIdentity user, String kind, String catalog,
            String schema, String name, String path, boolean read, long ttlSeconds) {
        return context.authority().read(() -> {
            Resolved resolved = resolve(user, kind, catalog, schema, name, path,
                    read ? "read" : "write");
            return new UrlGrant(resolved.grant(), resolved.space(), resolved.principal());
        });
    }

    void urlIssued(UrlGrant authorized, boolean read, long ttlSeconds) {
        context.audit().urlIssued(authorized.principal().principal(), tenantId(authorized.principal()),
                authorized.space(), authorized.grant().path(), read ? "presigned-read" : "presigned-write",
                ttlSeconds, authorized.grant().workerUrl());
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
        principal = subject(principal, space);
        authorize(principal, space, normalizedOperation);
        return descriptor(principal, space, path, normalizedOperation);
    }

    /** Resolves and authorizes a long-lived Stage link within the shared authority boundary. */
    public StageLink stageLink(UserIdentity user, String catalog, String schema, String name, String path) {
        return context.authority().read(() -> stageLinkAuthorized(user, catalog, schema, name, path));
    }

    private StageLink stageLinkAuthorized(UserIdentity user, String catalog, String schema, String name, String path) {
        UserIdentity principal = requireUser(user);
        FileSpace space = spaces.find(FileSpaceType.STAGE, catalog, schema, name)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                        "Stage not found: " + catalog + "." + schema + "." + name));
        principal = subject(principal, space);
        authorizeStageLink(principal, space);
        String relative = requireObjectPath(path);
        return new StageLink(space, principal, relative);
    }

    public void stageLinkIssued(StageLink authorized) {
        context.audit().urlIssued(authorized.principal().principal(), tenantId(authorized.principal()),
                authorized.space(), authorized.path(), "stage", 0, "");
    }

    /** Re-authorizes one immutable Stage ID before issuing its bounded transfer ticket. */
    public FileAccessGrant stageRead(UserIdentity user, UUID id, String path) {
        return context.authority().read(() -> stageReadAuthorized(user, id, path));
    }

    private FileAccessGrant stageReadAuthorized(UserIdentity user, UUID id, String path) {
        UserIdentity principal = requireUser(user);
        FileSpace space = spaces.find(id).filter(value -> value.type() == FileSpaceType.STAGE)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                        "Stage link no longer exists"));
        principal = subject(principal, space);
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

    /** Normalizes a relative object path and rejects an empty one. */
    public static String requireObjectPath(String path) {
        String normalized = normalizeRelativePath(path);
        if (normalized.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Stage file URL requires a non-empty object path");
        }
        return normalized;
    }

    private Resolved descriptor(UserIdentity principal, FileSpace space, String path,
            String normalizedOperation) {
        var revision = profileFor(space);
        StorageProfile profile = revision.profile();
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
                expiresAt, space.id().toString(), principal.principal(), tenantId(principal),
                revision.incarnationId(), revision.revision()));
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

    private org.pangrova.file.api.profile.StorageProfileRevision profileFor(FileSpace space) {
        String name = space.storageProfile().isBlank()
                ? context.managedProfileName() : space.storageProfile();
        if (name.isBlank()) {
            throw notConfigured(space, "no storage profile is configured");
        }
        var profile = profiles.getCurrentRevision(name);
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

    private UserIdentity subject(UserIdentity authenticated, FileSpace space) {
        var resolved = context.authority().resolve(authenticated, space);
        if (resolved == null || !authenticated.principal().equals(resolved.principal())
                || tenantId(resolved) == null || tenantId(resolved).isBlank()) {
            throw new AccessDeniedException("File authority must retain the subject and resolve its current tenant");
        }
        return resolved;
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
     * @param authority          durable read gate and tenant membership using the registries' database
     */
    public record FileSpaceAccessContext(String managedProfileName, Duration ttl,
            LongSupplier clock, Supplier<List<String>> workerEndpoints,
            FileOperationAudit audit, String signingSecret,
            org.pangrova.file.spi.authority.FileSpaceAuthority authority) {

        public FileSpaceAccessContext {
            Objects.requireNonNull(ttl, "ttl must not be null");
            Objects.requireNonNull(clock, "clock must not be null");
            Objects.requireNonNull(workerEndpoints, "workerEndpoints must not be null");
            Objects.requireNonNull(audit, "audit must not be null");
            Objects.requireNonNull(authority, "authority must not be null");
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
                Supplier<List<String>> workerEndpoints, FileOperationAudit audit,
                String signingSecret, org.pangrova.file.spi.authority.FileSpaceAuthority authority) {
            return new FileSpaceAccessContext(managedProfileName, Duration.ofMinutes(5),
                    System::currentTimeMillis, workerEndpoints, audit, signingSecret, authority);
        }
    }

    /**
     * Wire shape of a signed descriptor; field names must match the Worker record exactly.
     */
    record FileAccessDescriptorPayload(String profileName,
            org.pangrova.common.storage.StorageType type, String bucket, String prefix,
            String endpoint, String region, boolean pathStyleAccess, String baseKey,
            String path, String operation, long expiresAtEpochMillis, String spaceId,
            String subject, String tenantId, String profileIncarnationId, long profileRevision) {
    }

    private record Resolved(FileSpace space, UserIdentity principal, FileAccessGrant grant) {
    }

    public record StageLink(FileSpace space, UserIdentity principal, String path) {}

    record UrlGrant(FileAccessGrant grant, FileSpace space, UserIdentity principal) {
        @Override public String toString() { return "UrlGrant[authorized]"; }
    }
}
