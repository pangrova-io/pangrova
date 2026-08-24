package org.pangrova.coordinator.http.endpoint.catalog;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * OAuth2 token endpoint for REST catalog authentication.
 * Implements the token exchange flow required by the REST catalog specification,
 * enabling clients (Polaris, Tabular, Spark) to obtain access tokens via
 * client_credentials or token_exchange grant types.
 *
 * <p>Delegates actual credential validation to {@link AuthenticationManager}.</p>
 */
@Path("/v1/oauth/tokens")
@Produces(MediaType.APPLICATION_JSON)
public final class OAuth2TokenResource {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2TokenResource.class);
    private static final int DEFAULT_TOKEN_EXPIRY_SECONDS = 3600;

    private static final int HTTP_STATUS_UNAUTHORIZED = 401;
    private static final int HTTP_STATUS_BAD_REQUEST = 400;

    private final AuthenticationManager authenticationManager;

    public OAuth2TokenResource(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    /**
     * Issues an OAuth2 access token. Supports grant types:
     * <ul>
     *   <li>{@code client_credentials} — authenticates via client_id/client_secret</li>
     *   <li>{@code urn:ietf:params:oauth:grant-type:token-exchange} — exchanges an existing token</li>
     * </ul>
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response token(MultivaluedMap<String, String> formParams) {
        String grantType = formParams.getFirst("grant_type");
        if (grantType == null || grantType.isBlank()) {
            return errorResponse("invalid_request", "grant_type is required");
        }

        return switch (grantType) {
            case "client_credentials" -> handleClientCredentials(formParams);
            case "urn:ietf:params:oauth:grant-type:token-exchange" -> handleTokenExchange(formParams);
            default -> errorResponse("unsupported_grant_type",
                    "Unsupported grant_type: " + grantType);
        };
    }

    private Response handleClientCredentials(MultivaluedMap<String, String> params) {
        String clientId = params.getFirst("client_id");
        String clientSecret = params.getFirst("client_secret");
        String scope = params.getFirst("scope");

        if (clientId == null || clientSecret == null) {
            return errorResponse("invalid_request", "client_id and client_secret are required");
        }

        if (authenticationManager == null) {
            return issueToken(clientId, scope);
        }

        AuthenticationRequest authRequest = new AuthenticationRequest(
                clientId, clientSecret != null ? clientSecret.toCharArray() : null, null, null, Map.of(), null);
        AuthenticationResult result = authenticationManager.authenticate(authRequest);

        if (!result.success()) {
            LOG.warn("event=oauth2_auth_failed clientId={} error={}",
                    clientId, result.errorMessage());
            return errorResponse("invalid_client", "Authentication failed");
        }

        LOG.info("event=oauth2_token_issued clientId={} scope={}", clientId, scope);
        return issueToken(clientId, scope);
    }

    private Response handleTokenExchange(MultivaluedMap<String, String> params) {
        String subjectToken = params.getFirst("subject_token");
        String scope = params.getFirst("scope");

        if (subjectToken == null || subjectToken.isBlank()) {
            return errorResponse("invalid_request", "subject_token is required");
        }

        if (authenticationManager == null) {
            return issueToken("token-exchange", scope);
        }

        AuthenticationRequest authRequest = new AuthenticationRequest(
                null, null, subjectToken, null, Map.of(), null);
        AuthenticationResult result = authenticationManager.authenticate(authRequest);

        if (!result.success()) {
            LOG.warn("event=oauth2_token_exchange_failed error={}", result.errorMessage());
            return errorResponse("invalid_grant", "Token exchange failed");
        }

        String subject = result.userIdentity() != null
                ? result.userIdentity().principal() : "token-exchange";
        LOG.info("event=oauth2_token_exchanged subject={} scope={}", subject, scope);
        return issueToken(subject, scope);
    }

    private static Response issueToken(String subject, String scope) {
        String accessToken = UUID.randomUUID().toString();
        TokenResponse tokenResponse = new TokenResponse(
                accessToken,
                "bearer",
                DEFAULT_TOKEN_EXPIRY_SECONDS,
                scope,
                Instant.now().plusSeconds(DEFAULT_TOKEN_EXPIRY_SECONDS).getEpochSecond());
        return Response.ok(tokenResponse).build();
    }

    private static Response errorResponse(String error, String description) {
        OAuth2ErrorResponse errorBody = new OAuth2ErrorResponse(error, description);
        int status = "invalid_client".equals(error) ? HTTP_STATUS_UNAUTHORIZED : HTTP_STATUS_BAD_REQUEST;
        return Response.status(status).entity(errorBody).build();
    }

    record TokenResponse(
            String access_token,
            String token_type,
            int expires_in,
            String scope,
            long issued_token_type) {}

    record OAuth2ErrorResponse(String error, String error_description) {}
}
