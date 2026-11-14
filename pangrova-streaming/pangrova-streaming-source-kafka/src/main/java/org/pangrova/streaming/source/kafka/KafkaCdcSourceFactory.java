package org.pangrova.streaming.source.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.CdcSourceFactory;

import java.util.HashMap;
import java.util.Map;

/** Creates a dedicated byte-record Kafka consumer with publication-controlled offset commits. */
public final class KafkaCdcSourceFactory implements CdcSourceFactory {
    @Override
    public String identifier() {
        return "kafka";
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public void validate(Map<String, String> properties) {
        require(properties, "bootstrap.servers");
        require(properties, "topic");
        if (properties.containsKey("group.id")) {
            require(properties, "group.id");
        }
        validateDeserializer(properties, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        validateDeserializer(properties, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (!"false".equalsIgnoreCase(properties.getOrDefault("enable.auto.commit", "false"))) {
            throw new IllegalArgumentException("Kafka CDC requires enable.auto.commit=false");
        }
    }

    private static void validateDeserializer(Map<String, String> properties, String key) {
        if (!ByteArrayDeserializer.class.getName().equals(properties.getOrDefault(key, ByteArrayDeserializer.class.getName()))) {
            throw new IllegalArgumentException("Kafka CDC requires a byte array deserializer for " + key);
        }
    }

    private static String require(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Kafka source requires a nonblank '" + key + "'");
        }
        return value;
    }

    @Override
    public CdcSourceReader open(String checkpointId, Map<String, String> properties) {
        validate(properties);
        var kafka = new HashMap<>(properties);
        String topic = kafka.remove("topic");
        return new KafkaCdcSourceReader(kafka.get("bootstrap.servers"), topic,
                kafka.getOrDefault("group.id", "pangrova-cdc-" + checkpointId), kafka);
    }
}
