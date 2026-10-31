package org.pangrova.governance.lineage.provider.openlineage;

import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.api.lineage.LineageTransportException;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Sends lineage events to an OpenLineage-compatible backend (Marquez, DataHub)
 * via the OpenLineage Java SDK HTTP transport.
 *
 * <p>Translates internal {@link LineageEvent} records into OpenLineage RunEvents
 * and emits them through the configured HTTP endpoint. The environment constructor consumes
 * OPENLINEAGE_URL and OPENLINEAGE_API_KEY;
 * an explicit SDK HttpConfig supports programmatic deployment configuration.</p>
 */
public final class OpenLineageHttpTransport implements LineageBackend, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageHttpTransport.class);
    private static final int DEFAULT_HTTP_TIMEOUT_MS = 5000;
    private static final int HTTP_SUCCESS_MIN = 200;
    private static final int HTTP_SUCCESS_LIMIT = 300;
    private static final int MILLIS_PER_SECOND = 1000;
    private static final String PRODUCER = "https://github.com/pangrova/pangrova-governance";

    private final OpenLineageClient client;
    private final OpenLineage openLineage;
    private final String namespace;
    private final HttpTransport ownedTransport;

    /**
     * Creates an HTTP transport using the default OpenLineage client configuration.
     * Reads {@code OPENLINEAGE_URL} and optional {@code OPENLINEAGE_API_KEY}; a missing URL fails startup.
     *
     * @param namespace default namespace for datasets
     */
    public OpenLineageHttpTransport(String namespace) {
        this(environmentConfiguration(), namespace);
    }

    /** Owns the SDK HTTP transport configured for one deployment. */
    public OpenLineageHttpTransport(HttpConfig config, String namespace) {
        Objects.requireNonNull(config, "config");
        URI url = Objects.requireNonNull(config.getUrl(), "OpenLineage HTTP URL");
        if (!url.isAbsolute() || url.getHost() == null
                || !("https".equals(url.getScheme()) || "http".equals(url.getScheme()))
                || url.getUserInfo() != null) {
            throw new IllegalArgumentException("OpenLineage requires an absolute HTTP(S) URL without credentials");
        }
        if (url.getPath() != null && !url.getPath().isEmpty()
                && config.getEndpoint() != null && !config.getEndpoint().isBlank()) {
            throw new IllegalArgumentException("OpenLineage URL path and endpoint cannot both be configured");
        }
        this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
        this.ownedTransport = ownedHttpTransport(config);
        this.client = new OpenLineageClient(ownedTransport);
        this.openLineage = new OpenLineage(URI.create(PRODUCER));
    }

    private static HttpTransport ownedHttpTransport(HttpConfig config) {
        double configured = DEFAULT_HTTP_TIMEOUT_MS;
        if (config.getTimeout() != null) {
            configured = config.getTimeout() * MILLIS_PER_SECOND;
        } else if (config.getTimeoutInMillis() != null) {
            configured = config.getTimeoutInMillis().doubleValue();
        }
        if (!Double.isFinite(configured) || configured < 1 || configured > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("OpenLineage requires a finite positive HTTP timeout");
        }
        int timeoutMs = (int) configured;
        var request = org.apache.http.client.config.RequestConfig.custom()
                .setConnectTimeout(timeoutMs).setConnectionRequestTimeout(timeoutMs)
                .setSocketTimeout(timeoutMs).build();
        var http = org.apache.http.impl.client.HttpClientBuilder.create()
                .setDefaultRequestConfig(request).disableRedirectHandling().disableAutomaticRetries()
                .addInterceptorLast((org.apache.http.HttpResponseInterceptor) (response, context) -> {
                    int status = response.getStatusLine().getStatusCode();
                    if (status < HTTP_SUCCESS_MIN || status >= HTTP_SUCCESS_LIMIT) {
                        throw new java.io.IOException("OpenLineage ingestion was not accepted: HTTP " + status);
                    }
                }).build();
        try {
            return new HttpTransport(http, config);
        } catch (RuntimeException | Error failure) {
            try {
                http.close();
            } catch (java.io.IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static HttpConfig environmentConfiguration() {
        String url = System.getenv("OPENLINEAGE_URL");
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("HTTP lineage requires OPENLINEAGE_URL");
        }
        var config = new HttpConfig();
        config.setUrl(URI.create(url));
        config.setTimeoutInMillis(DEFAULT_HTTP_TIMEOUT_MS);
        String apiKey = System.getenv("OPENLINEAGE_API_KEY");
        if (apiKey != null && !apiKey.isBlank()) {
            var token = new io.openlineage.client.transports.ApiKeyTokenProvider();
            token.setApiKey(apiKey);
            config.setAuth(token);
        }
        return config;
    }

    /** Called by the emitter consumer after its final physical send/drain. */
    @Override
    public void close() throws java.io.IOException {
        ownedTransport.close();
    }

    @Override
    public void send(LineageEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        try {
            OpenLineage.RunEvent runEvent = toRunEvent(event);
            client.emit(runEvent);
            LOG.debug("event=openlineage_sent runId={} type={} inputs={} outputs={}",
                    event.runId(), event.eventType(),
                    event.inputDatasets().size(), event.outputDatasets().size());
        } catch (Exception e) {
            LOG.error("event=openlineage_send_failed runId={} error={}",
                    event.runId(), e.getMessage(), e);
            throw new LineageTransportException(
                    "Failed to send lineage event: " + event.runId(), e);
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    /**
     * Normalizes a run identifier to a valid UUID.
     * <ul>
     *   <li>If the input is already a valid UUID string, it is parsed directly.</li>
     *   <li>If null or blank, a random UUID is generated.</li>
     *   <li>Otherwise, a deterministic UUID v3 (name-based MD5) is derived from the input.</li>
     * </ul>
     * This ensures Pangrova query/stage IDs (e.g. "q-1") never cause UUID parse failures
     * in the OpenLineage SDK, while remaining deterministic for replay/correlation.
     */
    public static UUID normalizeRunId(String runId) {
        if (runId == null || runId.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(runId);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(runId.getBytes(StandardCharsets.UTF_8));
        }
    }

    private OpenLineage.RunEvent toRunEvent(LineageEvent event) {
        var runBuilder = openLineage.newRunBuilder().runId(normalizeRunId(event.runId()));
        String error = event.facets().get("error");
        if (error != null) {
            runBuilder.facets(openLineage.newRunFacetsBuilder().errorMessage(
                    openLineage.newErrorMessageRunFacetBuilder().message(error)
                            .programmingLanguage("SQL").build()).build());
        }
        OpenLineage.Run run = runBuilder.build();

        var jobBuilder = openLineage.newJobBuilder().namespace(namespace).name(event.jobName());
        if (event.sql() != null) {
            jobBuilder.facets(openLineage.newJobFacetsBuilder().sql(
                    openLineage.newSQLJobFacetBuilder().query(event.sql()).build()).build());
        }
        OpenLineage.Job job = jobBuilder.build();

        List<OpenLineage.InputDataset> inputs = event.inputDatasets().stream()
                .map(this::toInputDataset)
                .toList();

        List<OpenLineage.OutputDataset> outputs = event.outputDatasets().stream()
                .map(this::toOutputDataset)
                .toList();

        ZonedDateTime eventTime = event.timestamp().atZone(ZoneOffset.UTC);

        return openLineage.newRunEventBuilder()
                .eventType(mapEventType(event.eventType()))
                .eventTime(eventTime)
                .run(run)
                .job(job)
                .inputs(inputs)
                .outputs(outputs)
                .build();
    }

    private OpenLineage.InputDataset toInputDataset(DatasetRef ref) {
        return openLineage.newInputDatasetBuilder()
                .namespace(ref.namespace() != null ? ref.namespace() : namespace)
                .name(ref.name())
                .build();
    }

    private OpenLineage.OutputDataset toOutputDataset(DatasetRef ref) {
        OpenLineage.OutputDatasetBuilder builder = openLineage.newOutputDatasetBuilder()
                .namespace(ref.namespace() != null ? ref.namespace() : namespace)
                .name(ref.name());

        if (!ref.columnLineage().isEmpty()) {
            builder.facets(openLineage.newDatasetFacetsBuilder()
                    .columnLineage(buildColumnLineageFacet(ref))
                    .build());
        }

        return builder.build();
    }

    /**
     * Builds an OpenLineage column-level lineage facet from a {@link DatasetRef}.
     * Each {@link DatasetRef.ColumnLineage} entry maps an output column to its
     * source columns in the input datasets.
     */
    private OpenLineage.ColumnLineageDatasetFacet buildColumnLineageFacet(DatasetRef ref) {
        OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
                openLineage.newColumnLineageDatasetFacetFieldsBuilder();

        for (DatasetRef.ColumnLineage col : ref.columnLineage()) {
            List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> inputFields =
                    col.sourceColumns().stream()
                            .map(this::toInputField)
                            .toList();

            String transformation = switch (col.derivation()) {
                case DIRECT -> "IDENTITY";
                case TRANSFORM -> "TRANSFORMATION";
                case AGGREGATE -> "AGGREGATION";
            };
            OpenLineage.ColumnLineageDatasetFacetFieldsAdditional fieldAdditional =
                    openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .inputFields(inputFields)
                            .transformationType(transformation)
                            .build();

            fieldsBuilder.put(col.outputColumn(), fieldAdditional);
        }

        return openLineage.newColumnLineageDatasetFacetBuilder()
                .fields(fieldsBuilder.build())
                .build();
    }

    /**
     * Converts a structured source column reference to an OpenLineage input field.
     * No string parsing required — the reference already carries namespace, dataset, and column.
     */
    private OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields toInputField(
            DatasetRef.SourceColumnRef ref) {
        return openLineage.newColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                .namespace(ref.namespace())
                .name(ref.dataset())
                .field(ref.column())
                .build();
    }

    private static OpenLineage.RunEvent.EventType mapEventType(LineageEvent.EventType type) {
        return switch (type) {
            case START -> OpenLineage.RunEvent.EventType.START;
            case COMPLETE -> OpenLineage.RunEvent.EventType.COMPLETE;
            case FAIL -> OpenLineage.RunEvent.EventType.FAIL;
            case ABORT -> OpenLineage.RunEvent.EventType.ABORT;
        };
    }
}
