package org.pangrova.metadata.api.asset;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.pangrova.metadata.api.schema.ColumnMetadata;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * {@link AssetType#MODEL} asset: a machine-learning model registered in the
 * catalog.
 *
 * <p>The catalog tracks model identity, version, framework, and where the
 * artifact lives. Inference SQL ({@code SELECT predict('catalog.schema.model',
 * features)}) resolves the model through the same catalog as tables and views,
 * which is the central goal of the "universal catalog" framing.</p>
 *
 * <p>Each {@code (catalog, schema, name)} can have many versions; a single
 * {@code ModelAsset} represents one specific version. Model registries layer
 * "current-pointer" semantics on top.</p>
 *
 * @param catalog       containing catalog
 * @param schema        containing schema
 * @param name          model name
 * @param version       version identifier (semver, integer, hash — opaque to
 *                      the catalog)
 * @param framework     framework label: {@code TENSORFLOW}, {@code PYTORCH},
 *                      {@code ONNX}, {@code XGBOOST}, ...
 * @param artifactPath  storage URI of the serialized artifact (e.g.
 *                      {@code s3://models/iris/v3/model.onnx})
 * @param inputSchema   declared input feature columns
 * @param outputSchema  declared output / prediction columns
 * @param tags          curator-applied tags ({@code stage=production}, ...)
 * @param properties    framework-specific metadata (training parameters,
 *                      metrics, hashes)
 * @param createdAt     creation timestamp
 * @param updatedAt     last-update timestamp
 */
public record ModelAsset(
        String catalog,
        String schema,
        String name,
        String version,
        String framework,
        String artifactPath,
        List<ColumnMetadata> inputSchema,
        List<ColumnMetadata> outputSchema,
        List<String> tags,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public ModelAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(version, "version must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        framework = framework == null ? "" : framework;
        artifactPath = artifactPath == null ? "" : artifactPath;
        inputSchema = inputSchema == null ? List.of() : List.copyOf(inputSchema);
        outputSchema = outputSchema == null ? List.of() : List.copyOf(outputSchema);
        tags = tags == null ? List.of() : List.copyOf(tags);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.MODEL;
    }
}
