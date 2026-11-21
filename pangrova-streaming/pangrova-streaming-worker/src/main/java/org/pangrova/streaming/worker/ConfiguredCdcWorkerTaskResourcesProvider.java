package org.pangrova.streaming.worker;

import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.cdc.CdcRangeBatchWriter;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.worker.catalog.CatalogResourceResolver;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Production resource provider for one Worker CDC task.
 *
 * <p>Catalog resources are bound to the exact task before source access. The
 * connector writer stages delete/insert fragments without publishing, and the
 * task's catalog lease is released only after the reader closes.</p>
 */
public final class ConfiguredCdcWorkerTaskResourcesProvider
        implements CdcWorkerTaskResourcesProvider {

    private final CdcSourceRegistry sources;
    private final CdcRecordConverterRegistry converters;
    private final CatalogResourceResolver catalogs;
    private final Function<TaskRequest, Map<String, String>> sourceProperties;

    /** Creates the provider with discovered source/format registries and the bound catalog resolver. */
    public ConfiguredCdcWorkerTaskResourcesProvider(
            CdcSourceRegistry sources,
            CdcRecordConverterRegistry converters,
            CatalogResourceResolver catalogs,
            Function<TaskRequest, Map<String, String>> sourceProperties) {
        this.sources = Objects.requireNonNull(sources, "sources");
        this.converters = Objects.requireNonNull(converters, "converters");
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
        this.sourceProperties = Objects.requireNonNull(sourceProperties, "sourceProperties");
    }

    @Override
    public CdcWorkerTaskResources open(TaskRequest request) {
        CdcIngestBinding binding = request.getCdcIngest();
        String taskId = request.getAttempt().getTaskId();
        String topic = requireSingleTopic(binding);
        Map<String, String> properties = Map.copyOf(
                Objects.requireNonNull(sourceProperties.apply(request), "source properties"));
        if (!topic.equals(properties.get("topic"))
                || !"false".equalsIgnoreCase(
                        properties.getOrDefault("enable.auto.commit", "false"))) {
            throw new IllegalArgumentException(
                    "Worker CDC source properties must bind the admitted topic and disable auto-commit");
        }
        CdcSourceReader reader = null;
        BoundCdcPageSinkWriter writer = null;
        boolean bound = false;
        try {
            catalogs.bindTask(taskId, request);
            bound = true;
            var resources = catalogs.resolve(taskId);
            reader = sources.open(binding.getSourceProvider(),
                    binding.getStreamId() + ":" + taskId + ":" + binding.getSourceOwnershipEpoch(),
                    properties);
            CdcRecordConverter converter = converters.get(binding.getFormatId());
            writer = new BoundCdcPageSinkWriter(
                    resources.pageSinkProvider(), resources.session(), resources.outputHandle());
            return new Resources(reader, converter, writer, catalogs, taskId);
        } catch (RuntimeException | Error failure) {
            closeQuietly(reader);
            if (bound) {
                try {
                    catalogs.releaseTask(taskId);
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    private static String requireSingleTopic(CdcIngestBinding binding) {
        String topic = null;
        for (CdcSourceRange range : binding.getRangesList()) {
            if (topic == null) {
                topic = range.getTopic();
            } else if (!topic.equals(range.getTopic())) {
                throw new IllegalArgumentException(
                        "One Worker CDC task cannot span multiple source topics");
            }
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Worker CDC task has no source topic");
        }
        return topic;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // The primary failure remains authoritative; the catalog lease is still released.
        }
    }

    private static final class Resources implements CdcWorkerTaskResources {
        private final CdcSourceReader reader;
        private final CdcRecordConverter converter;
        private final BoundCdcPageSinkWriter writer;
        private final CatalogResourceResolver catalogs;
        private final String taskId;

        private Resources(
                CdcSourceReader reader,
                CdcRecordConverter converter,
                BoundCdcPageSinkWriter writer,
                CatalogResourceResolver catalogs,
                String taskId) {
            this.reader = reader;
            this.converter = converter;
            this.writer = writer;
            this.catalogs = catalogs;
            this.taskId = taskId;
        }

        @Override
        public CdcSourceReader sourceReader() {
            return reader;
        }

        @Override
        public CdcRecordConverter converter() {
            return converter;
        }

        @Override
        public CdcRangeBatchWriter writer() {
            return writer;
        }

        @Override
        public void close() {
            RuntimeException failure = null;
            try {
                reader.close();
            } catch (RuntimeException observed) {
                failure = observed;
            }
            try {
                catalogs.releaseTask(taskId);
            } catch (RuntimeException observed) {
                failure = combine(failure, observed);
            }
            if (failure != null) {
                throw failure;
            }
        }

        private static RuntimeException combine(RuntimeException first, RuntimeException next) {
            if (first == null) {
                return next;
            }
            first.addSuppressed(next);
            return first;
        }
    }
}
