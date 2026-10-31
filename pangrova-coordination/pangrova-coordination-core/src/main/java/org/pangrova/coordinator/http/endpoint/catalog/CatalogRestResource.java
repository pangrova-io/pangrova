package org.pangrova.coordinator.http.endpoint.catalog;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ErrorBody;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ErrorResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ConfigResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ListNamespacesResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ListTablesResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.ListViewsResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.TableIdentifier;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * REST Catalog API resource implementing the open table format REST catalog specification.
 * Provides namespace, table, and view management endpoints compatible with
 * Polaris, Tabular, and any REST-catalog-compatible client.
 *
 * <p>Endpoint paths follow the REST catalog specification v1:
 * <ul>
 *   <li>{@code /v1/namespaces} — namespace CRUD</li>
 *   <li>{@code /v1/namespaces/{ns}/tables} — table CRUD within a namespace</li>
 *   <li>{@code /v1/namespaces/{ns}/views} — view listing within a namespace</li>
 * </ul>
 */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class CatalogRestResource {

    /** Whitelist pattern for namespace and table identifiers: alphanumeric, underscore, dot, dash. */
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-zA-Z0-9_.\\-]{1,256}");

    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final int HTTP_STATUS_NOT_FOUND = 404;

    private final CatalogRestOperations operations;

    public CatalogRestResource(CatalogRestOperations operations) {
        this.operations = checkNotNull(operations, "operations must not be null");
    }

    /**
     * Validates that a path parameter matches the safe identifier pattern.
     * Rejects overly long or specially crafted identifiers to prevent
     * injection or resource exhaustion attacks.
     *
     * @param identifier the path parameter value
     * @param paramName  human-readable name for error messages
     * @return the validated identifier
     */
    private static String validateIdentifier(String identifier, String paramName) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException(paramName + " must not be null or blank");
        }
        if (!SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    paramName + " contains invalid characters or exceeds length limit: " + identifier);
        }
        return identifier;
    }

    // ─── Configuration ───────────────────────────────────────────────────────────

    @GET
    @Path("/config")
    public Response getConfig() {
        ConfigResponse config = new ConfigResponse(Map.of(), Map.of());
        return Response.ok(config).build();
    }

    // ─── Namespace Operations ────────────────────────────────────────────────────

    @GET
    @Path("/namespaces")
    public Response listNamespaces(@QueryParam("parent") String parent) {
        List<List<String>> namespaces = operations.listNamespaces(parent);
        return Response.ok(new ListNamespacesResponse(namespaces)).build();
    }

    /** Creates a new namespace with optional properties. */
    @POST
    @Path("/namespaces")
    public Response createNamespace(CreateNamespaceRequest request) {
        if (request == null || request.namespace() == null || request.namespace().isEmpty()) {
            return errorResponse(HTTP_STATUS_BAD_REQUEST, "BadRequestException",
                    "Namespace identifier is required");
        }
        var result = operations.createNamespace(request.namespace(), request.properties());
        return Response.ok(result).build();
    }

    /** Loads a single namespace's metadata. */
    @GET
    @Path("/namespaces/{namespace}")
    public Response getNamespace(@PathParam("namespace") String namespace) {
        validateIdentifier(namespace, "namespace");
        var result = operations.getNamespace(namespace);
        if (result == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.ok(result).build();
    }

    /** Checks whether a namespace exists. */
    @HEAD
    @Path("/namespaces/{namespace}")
    public Response namespaceExists(@PathParam("namespace") String namespace) {
        validateIdentifier(namespace, "namespace");
        boolean exists = operations.namespaceExists(namespace);
        if (!exists) {
            return Response.status(HTTP_STATUS_NOT_FOUND).build();
        }
        return Response.noContent().build();
    }

    /** Drops an existing namespace. */
    @DELETE
    @Path("/namespaces/{namespace}")
    public Response dropNamespace(@PathParam("namespace") String namespace) {
        validateIdentifier(namespace, "namespace");
        boolean dropped = operations.dropNamespace(namespace);
        if (!dropped) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.noContent().build();
    }

    /** Updates (adds/removes) properties on an existing namespace. */
    @POST
    @Path("/namespaces/{namespace}/properties")
    public Response updateNamespaceProperties(
            @PathParam("namespace") String namespace,
            UpdateNamespacePropertiesRequest request) {
        validateIdentifier(namespace, "namespace");
        var result = operations.updateNamespaceProperties(
                namespace, request.removals(), request.updates());
        if (result == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.ok(result).build();
    }

    // ─── Table Operations ────────────────────────────────────────────────────────

    /** Lists all tables within a namespace. */
    @GET
    @Path("/namespaces/{namespace}/tables")
    public Response listTables(@PathParam("namespace") String namespace) {
        validateIdentifier(namespace, "namespace");
        List<TableIdentifier> tables = operations.listTables(namespace);
        if (tables == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.ok(new ListTablesResponse(tables)).build();
    }

    /** Creates a new table within a namespace. */
    @POST
    @Path("/namespaces/{namespace}/tables")
    public Response createTable(
            @PathParam("namespace") String namespace,
            CreateTableRequest request) {
        validateIdentifier(namespace, "namespace");
        if (request == null || request.name() == null || request.name().isBlank()) {
            return errorResponse(HTTP_STATUS_BAD_REQUEST, "BadRequestException",
                    "Table name is required");
        }
        validateIdentifier(request.name(), "table");
        var result = operations.createTable(namespace, request);
        if (result == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.ok(result).build();
    }

    /** Loads a table's metadata. */
    @GET
    @Path("/namespaces/{namespace}/tables/{table}")
    public Response loadTable(
            @PathParam("namespace") String namespace,
            @PathParam("table") String table) {
        validateIdentifier(namespace, "namespace");
        validateIdentifier(table, "table");
        var result = operations.loadTable(namespace, table);
        if (result == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchTableException",
                    "Table does not exist: " + namespace + "." + table);
        }
        return Response.ok(result).build();
    }

    /** Checks whether a table exists. */
    @HEAD
    @Path("/namespaces/{namespace}/tables/{table}")
    public Response tableExists(
            @PathParam("namespace") String namespace,
            @PathParam("table") String table) {
        validateIdentifier(namespace, "namespace");
        validateIdentifier(table, "table");
        boolean exists = operations.tableExists(namespace, table);
        if (!exists) {
            return Response.status(HTTP_STATUS_NOT_FOUND).build();
        }
        return Response.noContent().build();
    }

    /** Drops an existing table. */
    @DELETE
    @Path("/namespaces/{namespace}/tables/{table}")
    public Response dropTable(
            @PathParam("namespace") String namespace,
            @PathParam("table") String table) {
        validateIdentifier(namespace, "namespace");
        validateIdentifier(table, "table");
        boolean dropped = operations.dropTable(namespace, table);
        if (!dropped) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchTableException",
                    "Table does not exist: " + namespace + "." + table);
        }
        return Response.noContent().build();
    }

    // ─── View Operations ─────────────────────────────────────────────────────────

    /** Lists all views within a namespace. */
    @GET
    @Path("/namespaces/{namespace}/views")
    public Response listViews(@PathParam("namespace") String namespace) {
        validateIdentifier(namespace, "namespace");
        List<TableIdentifier> views = operations.listViews(namespace);
        if (views == null) {
            return errorResponse(HTTP_STATUS_NOT_FOUND, "NoSuchNamespaceException",
                    "Namespace does not exist: " + namespace);
        }
        return Response.ok(new ListViewsResponse(views)).build();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────────

    private static Response errorResponse(int status, String type, String message) {
        ErrorResponse error = new ErrorResponse(new ErrorBody(message, type, status));
        return Response.status(status).entity(error).build();
    }
}
