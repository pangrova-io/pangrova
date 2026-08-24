package org.pangrova.governance.lineage;

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
 * and emits them through the configured HTTP endpoint. Configuration is loaded
 * from the standard OpenLineage YAML config file or environment variables.</p>
 */
public final class OpenLineageHttpTransport implements LineageBackend {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageHttpTransport.class);
    private static final String PRODUCER = "https://github.com/pangrova/pangrova-governance";

    private final OpenLineageClient client;
    private final OpenLineage openLineage;
    private final String namespace;

    /**
     * Creates an HTTP transport using the default OpenLineage client configuration.
     * The client reads transport settings from {@code openlineage.yml} or environment
     * variables ({@code OPENLINEAGE_URL}, {@code OPENLINEAGE_API_KEY}).
     *
     * @param namespace default namespace for datasets
     */
    public OpenLineageHttpTransport(String namespace) {
        this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
        HttpConfig httpConfig = new HttpConfig();
        HttpTransport transport = new HttpTransport(httpConfig);
        this.client = new OpenLineageClient(transport);
        this.openLineage = new OpenLineage(URI.create(PRODUCER));
    }

    /**
     * Creates an HTTP transport with an explicit pre-configured client.
     * Useful for testing or when programmatic configuration is preferred.
     *
     * @param client    pre-configured OpenLineage client
     * @param namespace default namespace for datasets
     */
    public OpenLineageHttpTransport(OpenLineageClient client, String namespace) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
        this.openLineage = new OpenLineage(URI.create(PRODUCER));
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
    static UUID normalizeRunId(String runId) {
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
        OpenLineage.Run run = openLineage.newRunBuilder()
                .runId(normalizeRunId(event.runId()))
                .build();

        OpenLineage.Job job = openLineage.newJobBuilder()
                .namespace(namespace)
                .name(event.jobName())
                .build();

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

            OpenLineage.ColumnLineageDatasetFacetFieldsAdditional fieldAdditional =
                    openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .inputFields(inputFields)
                            .transformationType("IDENTITY")
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
        };
    }
}
