package org.pangrova.server.worker.bootstrap;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.WorkerOptions;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.worker.CdcWorkerTaskBackend;
import org.pangrova.streaming.worker.CdcWorkerTaskOptions;
import org.pangrova.streaming.worker.ConfiguredCdcWorkerTaskResourcesProvider;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/** Installs the production Worker-JVM CDC backend from deployment configuration. */
public final class CdcWorkerRuntimeBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(CdcWorkerRuntimeBootstrap.class);
    private static final String KAFKA_PROPERTY_PREFIX = "worker.cdc.kafka.properties.";
    private static final int MAX_RECORDS_PER_POLL = 1_000;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(60);

    /** Utility owner; installation happens through the static bootstrap entry point. */
    private CdcWorkerRuntimeBootstrap() {}

    /** Installs the configured Kafka-backed Worker CDC backend before task startup. */
    public static void install(PangrovaConfiguration config, WorkerBootstrap bootstrap) {
        if (!config.getBoolean(WorkerOptions.CDC_ENABLED)) {
            return;
        }
        String bootstrapServers = config.getString(WorkerOptions.CDC_KAFKA_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            LOG.warn("event=worker_cdc_backend_unconfigured "
                    + "reason=kafka_bootstrap_servers_absent");
            return;
        }
        var catalogResolver = bootstrap.catalogResourceResolver();
        if (catalogResolver == null) {
            throw new IllegalStateException(
                    "CDC Worker backend requires the production catalog runtime");
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        var sources = CdcSourceRegistry.discover(loader);
        var converters = CdcRecordConverterRegistry.discover(loader);
        Map<String, String> configured = Map.copyOf(config.toMap());
        var provider = new ConfiguredCdcWorkerTaskResourcesProvider(
                sources, converters, catalogResolver,
                request -> sourceProperties(request, bootstrapServers.trim(), configured));
        bootstrap.setWorkerExecutedTaskBackend(new CdcWorkerTaskBackend(
                provider, new CdcWorkerTaskOptions(
                        MAX_RECORDS_PER_POLL, POLL_TIMEOUT, IDLE_TIMEOUT)));
        LOG.info("event=worker_cdc_backend_installed provider=kafka");
    }

    static Map<String, String> sourceProperties(
            TaskRequest request, String bootstrapServers, Map<String, String> configured) {
        var binding = request.getCdcIngest();
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
        Map<String, String> properties = new java.util.HashMap<>();
        configured.forEach((key, value) -> {
            if (key.startsWith(KAFKA_PROPERTY_PREFIX)) {
                String property = key.substring(KAFKA_PROPERTY_PREFIX.length());
                if (!property.isBlank() && value != null) {
                    properties.put(property, value);
                }
            }
        });
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("topic", topic);
        properties.put("group.id", "pangrova-cdc-worker-"
                + binding.getStreamId() + "-" + request.getAttempt().getTaskId());
        properties.put("enable.auto.commit", "false");
        return Map.copyOf(properties);
    }
}
