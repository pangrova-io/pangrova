package org.pangrova.resilience.config;

import org.pangrova.resilience.config.ResiliencePolicyConfig.BulkheadSettings;
import org.pangrova.resilience.config.ResiliencePolicyConfig.CircuitBreakerSettings;
import org.pangrova.resilience.config.ResiliencePolicyConfig.RateLimiterSettings;
import org.pangrova.resilience.config.ResiliencePolicyConfig.RetrySettings;
import org.pangrova.resilience.config.ResiliencePolicyConfig.TimeLimiterSettings;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads per-service resilience policies from a YAML configuration file.
 *
 * <p>Expected YAML format:</p>
 * <pre>
 * resilience:
 *   services:
 *     coordinator-to-metadata:
 *       circuitBreaker:
 *         failureRateThreshold: 50
 *         slidingWindowSize: 100
 *       retry:
 *         maxAttempts: 3
 *       bulkhead:
 *         maxConcurrentCalls: 10
 *     coordinator-to-worker:
 *       circuitBreaker:
 *         failureRateThreshold: 60
 * </pre>
 */
public final class ResilienceConfigLoader {

    private ResilienceConfigLoader() {}

    /**
     * Loads resilience policies from the given input stream.
     *
     * @param input YAML input stream
     * @return unmodifiable map of service name to policy config
     */
    @SuppressWarnings("unchecked")
    public static Map<String, ResiliencePolicyConfig> load(InputStream input) {
        if (input == null) {
            return Collections.emptyMap();
        }
        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(input);
        if (root == null || !root.containsKey("resilience")) {
            return Collections.emptyMap();
        }
        Map<String, Object> resilience = (Map<String, Object>) root.get("resilience");
        if (resilience == null || !resilience.containsKey("services")) {
            return Collections.emptyMap();
        }
        Map<String, Object> services = (Map<String, Object>) resilience.get("services");
        if (services == null) {
            return Collections.emptyMap();
        }
        Map<String, ResiliencePolicyConfig> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : services.entrySet()) {
            String serviceName = entry.getKey();
            Map<String, Object> serviceConfig = (Map<String, Object>) entry.getValue();
            result.put(serviceName, parseServiceConfig(serviceName, serviceConfig));
        }
        return Collections.unmodifiableMap(result);
    }

    @SuppressWarnings("unchecked")
    private static ResiliencePolicyConfig parseServiceConfig(String name, Map<String, Object> config) {
        CircuitBreakerSettings cb = config.containsKey("circuitBreaker")
                ? parseCircuitBreaker((Map<String, Object>) config.get("circuitBreaker"))
                : null;
        RetrySettings retry = config.containsKey("retry")
                ? parseRetry((Map<String, Object>) config.get("retry"))
                : null;
        BulkheadSettings bulkhead = config.containsKey("bulkhead")
                ? parseBulkhead((Map<String, Object>) config.get("bulkhead"))
                : null;
        RateLimiterSettings rateLimiter = config.containsKey("rateLimiter")
                ? parseRateLimiter((Map<String, Object>) config.get("rateLimiter"))
                : null;
        TimeLimiterSettings timeLimiter = config.containsKey("timeLimiter")
                ? parseTimeLimiter((Map<String, Object>) config.get("timeLimiter"))
                : null;
        return new ResiliencePolicyConfig(name, cb, retry, bulkhead, rateLimiter, timeLimiter);
    }

    private static CircuitBreakerSettings parseCircuitBreaker(Map<String, Object> map) {
        CircuitBreakerSettings d = CircuitBreakerSettings.defaults();
        return new CircuitBreakerSettings(
                floatVal(map, "failureRateThreshold", d.failureRateThreshold()),
                floatVal(map, "slowCallRateThreshold", d.slowCallRateThreshold()),
                longVal(map, "slowCallDurationMs", d.slowCallDurationMs()),
                intVal(map, "minimumNumberOfCalls", d.minimumNumberOfCalls()),
                intVal(map, "slidingWindowSize", d.slidingWindowSize()),
                longVal(map, "waitDurationInOpenStateMs", d.waitDurationInOpenStateMs()),
                intVal(map, "permittedNumberOfCallsInHalfOpenState", d.permittedNumberOfCallsInHalfOpenState()));
    }

    private static RetrySettings parseRetry(Map<String, Object> map) {
        RetrySettings d = RetrySettings.defaults();
        return new RetrySettings(
                intVal(map, "maxAttempts", d.maxAttempts()),
                longVal(map, "waitDurationMs", d.waitDurationMs()),
                doubleVal(map, "backoffMultiplier", d.backoffMultiplier()),
                longVal(map, "maxWaitDurationMs", d.maxWaitDurationMs()));
    }

    private static BulkheadSettings parseBulkhead(Map<String, Object> map) {
        BulkheadSettings d = BulkheadSettings.defaults();
        return new BulkheadSettings(
                intVal(map, "maxConcurrentCalls", d.maxConcurrentCalls()),
                longVal(map, "maxWaitDurationMs", d.maxWaitDurationMs()));
    }

    private static RateLimiterSettings parseRateLimiter(Map<String, Object> map) {
        RateLimiterSettings d = RateLimiterSettings.defaults();
        return new RateLimiterSettings(
                intVal(map, "limitForPeriod", d.limitForPeriod()),
                longVal(map, "limitRefreshPeriodMs", d.limitRefreshPeriodMs()),
                longVal(map, "timeoutDurationMs", d.timeoutDurationMs()));
    }

    private static TimeLimiterSettings parseTimeLimiter(Map<String, Object> map) {
        TimeLimiterSettings d = TimeLimiterSettings.defaults();
        return new TimeLimiterSettings(
                longVal(map, "timeoutDurationMs", d.timeoutDurationMs()),
                boolVal(map, "cancelRunningFuture", d.cancelRunningFuture()));
    }

    private static int intVal(Map<String, Object> map, String key, int defaultVal) {
        Object val = map.get(key);
        return val instanceof Number n ? n.intValue() : defaultVal;
    }

    private static long longVal(Map<String, Object> map, String key, long defaultVal) {
        Object val = map.get(key);
        return val instanceof Number n ? n.longValue() : defaultVal;
    }

    private static float floatVal(Map<String, Object> map, String key, float defaultVal) {
        Object val = map.get(key);
        return val instanceof Number n ? n.floatValue() : defaultVal;
    }

    private static double doubleVal(Map<String, Object> map, String key, double defaultVal) {
        Object val = map.get(key);
        return val instanceof Number n ? n.doubleValue() : defaultVal;
    }

    private static boolean boolVal(Map<String, Object> map, String key, boolean defaultVal) {
        Object val = map.get(key);
        return val instanceof Boolean b ? b : defaultVal;
    }
}
