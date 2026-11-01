package org.pangrova.streaming.cdc;

import org.pangrova.streaming.watermark.LateDataConfig;
import org.pangrova.streaming.watermark.LateDataPolicy;
import org.pangrova.streaming.watermark.WatermarkConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Grouped runtime policies for one CDC pipeline.
 *
 * @param watermark       watermark column, delay, and idle timeout
 * @param lateData        late-record policy and allowed lateness
 * @param deadLetter      conversion-failure policy
 * @param pipelineEpoch   HA epoch this pipeline was started with
 */
public record CdcPipelinePolicies(
        WatermarkConfig watermark,
        LateDataConfig lateData,
        DeadLetterPolicy deadLetter,
        long pipelineEpoch
) {

    /** Stream property controlling the watermark emission delay. */
    public static final String WATERMARK_DELAY_PROPERTY = "watermark_delay";
    /** Stream property selecting the late-data policy: {@code drop} or {@code update}. */
    public static final String LATE_DATA_POLICY_PROPERTY = "late_data_policy";
    /** Stream property controlling how far behind the watermark a record may be. */
    public static final String ALLOWED_LATENESS_PROPERTY = "allowed_lateness";

    public CdcPipelinePolicies {
        Objects.requireNonNull(watermark, "watermark must not be null");
        Objects.requireNonNull(lateData, "lateData must not be null");
        Objects.requireNonNull(deadLetter, "deadLetter must not be null");
    }

    /** Safe defaults: delayed watermark, UPDATE late data, fail on poison records. */
    public static CdcPipelinePolicies defaults() {
        return new CdcPipelinePolicies(
                WatermarkConfig.defaults(),
                LateDataConfig.update(Duration.ZERO),
                DeadLetterPolicy.FAIL_PIPELINE,
                0L);
    }

    /**
     * Resolves per-stream policies from the persisted stream properties.
     *
     * <p>Recognized properties (all optional, values validated strictly):</p>
     * <ul>
     *   <li>{@code watermark_delay} — duration (ISO-8601, {@code 10s}, {@code 500ms},
     *       {@code 5m}, {@code 1h}, {@code 2d}, or bare milliseconds); default {@code 10s}</li>
     *   <li>{@code late_data_policy} — {@code update} or {@code drop}; default {@code update}</li>
     *   <li>{@code allowed_lateness} — duration as above; default {@code 0s}</li>
     * </ul>
     *
     * <p>Invalid values fail the pipeline at startup instead of silently
     * falling back, so a misconfigured stream is visible in the FAILED state.</p>
     *
     * @param properties   persisted {@code stream_properties}
     * @param pipelineEpoch HA epoch the pipeline runs under
     * @throws IllegalArgumentException when a property value is unparseable
     */
    public static CdcPipelinePolicies fromStreamProperties(Map<String, String> properties,
                                                           long pipelineEpoch) {
        WatermarkConfig defaults = WatermarkConfig.defaults();
        Duration watermarkDelay = parseDurationProperty(
                properties, WATERMARK_DELAY_PROPERTY, defaults.watermarkDelay());
        LateDataPolicy lateDataPolicy = parseLateDataPolicy(properties);
        Duration allowedLateness = parseDurationProperty(
                properties, ALLOWED_LATENESS_PROPERTY, Duration.ZERO);
        return new CdcPipelinePolicies(
                new WatermarkConfig(defaults.watermarkColumn(), watermarkDelay,
                        defaults.idleTimeout()),
                new LateDataConfig(lateDataPolicy, allowedLateness),
                DeadLetterPolicy.FAIL_PIPELINE,
                pipelineEpoch);
    }

    private static LateDataPolicy parseLateDataPolicy(Map<String, String> properties) {
        String raw = properties.get(LATE_DATA_POLICY_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return LateDataPolicy.UPDATE;
        }
        String normalized = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "update" -> LateDataPolicy.UPDATE;
            case "drop" -> LateDataPolicy.DROP;
            default -> throw new IllegalArgumentException(
                    "Invalid '" + LATE_DATA_POLICY_PROPERTY + "' value '" + raw
                            + "': expected 'update' or 'drop'");
        };
    }

    private static Duration parseDurationProperty(Map<String, String> properties,
                                                  String key, Duration defaultValue) {
        String raw = properties.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return parseDuration(raw);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid '" + key + "' value '" + raw + "': " + e.getMessage(), e);
        }
    }

    /**
     * Accepts ISO-8601 ({@code PT10S}), a numeric suffix ({@code 10s},
     * {@code 500ms}, {@code 5m}, {@code 1h}, {@code 2d}), or bare milliseconds.
     */
    private static Duration parseDuration(String raw) {
        String trimmed = raw.trim();
        if (trimmed.toUpperCase(java.util.Locale.ROOT).startsWith("PT")
                || trimmed.toUpperCase(java.util.Locale.ROOT).startsWith("-PT")) {
            return Duration.parse(trimmed.toUpperCase(java.util.Locale.ROOT));
        }
        List<Map.Entry<String, java.util.function.Function<Long, Duration>>> suffixes = List.of(
                Map.entry("ms", Duration::ofMillis),
                Map.entry("s", Duration::ofSeconds),
                Map.entry("m", Duration::ofMinutes),
                Map.entry("h", Duration::ofHours),
                Map.entry("d", Duration::ofDays));
        for (Map.Entry<String, java.util.function.Function<Long, Duration>> entry : suffixes) {
            if (trimmed.endsWith(entry.getKey())) {
                long value = Long.parseLong(
                        trimmed.substring(0, trimmed.length() - entry.getKey().length()).trim());
                return entry.getValue().apply(value);
            }
        }
        return Duration.ofMillis(Long.parseLong(trimmed));
    }
}
