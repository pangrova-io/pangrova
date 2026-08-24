package org.pangrova.coordinator.http.endpoint.catalog;

import java.util.List;
import java.util.Map;

/**
 * Typed response DTOs for the REST catalog API, following the open table format REST specification.
 * Each nested record represents a specific response shape for compile-time type safety.
 */
public final class CatalogRestResponse {

    private CatalogRestResponse() {}

    /** GET /v1/config response. */
    public record ConfigResponse(
            Map<String, String> overrides,
            Map<String, String> defaults) {}

    /** GET /v1/namespaces response. */
    public record ListNamespacesResponse(List<List<String>> namespaces) {}

    /** GET /v1/namespaces/{ns} response. */
    public record GetNamespaceResponse(
            List<String> namespace,
            Map<String, String> properties) {}

    /** POST /v1/namespaces response (same shape as GET). */
    public record CreateNamespaceResponse(
            List<String> namespace,
            Map<String, String> properties) {}

    /** POST /v1/namespaces/{ns}/properties response. */
    public record UpdatePropertiesResponse(
            List<String> updated,
            List<String> removed,
            List<String> missing) {}

    /** GET /v1/namespaces/{ns}/tables response. */
    public record ListTablesResponse(List<TableIdentifier> identifiers) {}

    /** Table identifier within a namespace. */
    public record TableIdentifier(List<String> namespace, String name) {}

    /** GET /v1/namespaces/{ns}/tables/{table} response (simplified metadata). */
    public record LoadTableResponse(
            String metadataLocation,
            TableMetadataPayload metadata) {}

    /** Simplified table metadata payload for REST catalog responses. */
    public record TableMetadataPayload(
            int formatVersion,
            String tableUuid,
            String location,
            List<SchemaPayload> schemas,
            int currentSchemaId,
            List<PartitionSpecPayload> partitionSpecs,
            int defaultSpecId,
            Map<String, String> properties) {}

    /** Schema representation in REST catalog responses. */
    public record SchemaPayload(
            int schemaId,
            List<FieldPayload> fields) {}

    /** Field representation in REST catalog responses. */
    public record FieldPayload(
            int id,
            String name,
            String type,
            boolean required) {}

    /** Partition spec representation. */
    public record PartitionSpecPayload(
            int specId,
            List<PartitionFieldPayload> fields) {}

    /** Partition field representation. */
    public record PartitionFieldPayload(
            String name,
            String transform,
            int sourceId,
            int fieldId) {}

    /** GET /v1/namespaces/{ns}/views response. */
    public record ListViewsResponse(List<TableIdentifier> identifiers) {}

    /** Error response following REST catalog specification. */
    public record ErrorResponse(ErrorBody error) {}

    /** Error body with message, type, and code. */
    public record ErrorBody(String message, String type, int code) {}
}
