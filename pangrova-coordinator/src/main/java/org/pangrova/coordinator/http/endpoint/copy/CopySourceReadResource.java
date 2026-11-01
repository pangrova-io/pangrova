package org.pangrova.coordinator.http.endpoint.copy;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissions;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/** Coordinator-controlled redemption for a task-private COPY source-read ticket. */
@Path("/v1/internal/copy-source-read")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class CopySourceReadResource {
    private final JdbcCopySourceReadAdmissions admissions;
    private final HighAvailabilityCoordinationService coordination;

    public CopySourceReadResource(JdbcCopySourceReadAdmissions admissions,
                                  HighAvailabilityCoordinationService coordination) {
        CopySourceWireLoggingPolicy.requireSafeJettyLogging();
        this.admissions = Objects.requireNonNull(admissions, "admissions");
        this.coordination = Objects.requireNonNull(coordination, "coordination");
    }

    /** Resolves only the exact current profile admitted for this private task. */
    @POST
    @Path("/redeem")
    public CopySourceReadHttp.RedeemResponse redeem(CopySourceReadHttp.RedeemRequest request,
                                                    @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        Objects.requireNonNull(request, "request");
        var scope = scope(request.scope());
        var anchor = coordination.requireExecutionAnchor(scope.executionId());
        var result = admissions.redeem(new JdbcCopySourceReadAdmissions.Ticket(request.nonce()),
                scope, anchor);
        var revision = result.profile();
        var profile = revision.profile();
        if (!profile.credentials().isEmpty()) {
            throw new IllegalStateException("COPY profile contains credentials outside the Worker secret boundary");
        }
        requireNonSecretEndpoint(profile.endpoint());
        var publicProfile = new CopySourceReadHttp.Profile(profile.name(), profile.type(),
                profile.bucket(), profile.prefix(), profile.endpoint(), profile.region(),
                profile.isPathStyleAccess(), revision.incarnationId(), revision.revision());
        return new CopySourceReadHttp.RedeemResponse(publicProfile,
                result.stagedSource().map(CopySourceReadResource::proof).orElse(null));
    }

    /** Records one immutable staged object under the same live owner and READ grant. */
    @POST
    @Path("/record")
    public CopySourceReadHttp.StagedSource record(CopySourceReadHttp.RecordRequest request,
                                                 @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        Objects.requireNonNull(request, "request");
        var scope = scope(request.scope());
        var anchor = coordination.requireExecutionAnchor(scope.executionId());
        return proof(admissions.recordStagedSource(
                new JdbcCopySourceReadAdmissions.Ticket(request.nonce()), scope,
                staged(request.stagedSource()), anchor));
    }

    private static JdbcCopySourceReadAdmissions.Scope scope(CopySourceReadHttp.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return new JdbcCopySourceReadAdmissions.Scope(scope.tenantId(), scope.principal(),
                scope.profileName(), scope.profileIncarnationId(), scope.profileRevision(),
                scope.canonicalSourceUrl(), scope.managedStorageRootUri(),
                scope.managedBackendIdentitySha256(),
                scope.executionId(), scope.stagingAttemptId(), scope.ownerId(),
                scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                scope.deadlineEpochMs());
    }

    private static JdbcCopySourceReadAdmissions.StagedSource staged(
            CopySourceReadHttp.StagedSource proof) {
        Objects.requireNonNull(proof, "stagedSource");
        return new JdbcCopySourceReadAdmissions.StagedSource(proof.sourceVersionId(),
                proof.managedObjectUri(), proof.contentSha256(), proof.contentSize(),
                proof.tenantScope());
    }

    private static CopySourceReadHttp.StagedSource proof(
            JdbcCopySourceReadAdmissions.StagedSource proof) {
        return new CopySourceReadHttp.StagedSource(proof.sourceVersionId(),
                proof.managedObjectUri(), proof.contentSha256(), proof.contentSize(),
                proof.tenantScope());
    }

    private static void requireNonSecretEndpoint(String endpoint) {
        if (endpoint.isBlank()) {
            return;
        }
        try {
            URI uri = new URI(endpoint);
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IllegalStateException("COPY source endpoint is not a credential-free HTTP endpoint");
            }
        } catch (URISyntaxException failure) {
            throw new IllegalStateException("COPY source endpoint is not a credential-free HTTP endpoint", failure);
        }
    }
}
