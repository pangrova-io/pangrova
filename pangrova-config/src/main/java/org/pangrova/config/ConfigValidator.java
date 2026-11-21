package org.pangrova.config;

import org.pangrova.config.exception.ConfigValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates configuration values at startup, ensuring type correctness,
 * range validity, and required field presence with clear error messages.
 *
 * <p>Usage: register required options, then validate against a loaded
 * configuration. Throws {@link ConfigValidationException} on first
 * batch of violations found.</p>
 */
public final class ConfigValidator {

    private final List<ConfigConstraint<?>> constraints = new ArrayList<>();

    /**
     * Registers a required option that must have a non-null value.
     *
     * @param option the config option that must be present
     * @return this validator for chaining
     */
    public <T> ConfigValidator require(ConfigOption<T> option) {
        constraints.add(new RequiredConstraint<>(option));
        return this;
    }

    /**
     * Registers an integer option with a valid range.
     *
     * @param option the integer config option
     * @param min    minimum allowed value (inclusive)
     * @param max    maximum allowed value (inclusive)
     * @return this validator for chaining
     */
    public ConfigValidator intRange(ConfigOption<Integer> option, int min, int max) {
        constraints.add(new IntRangeConstraint(option, min, max));
        return this;
    }

    /**
     * Registers a long option with a valid range.
     *
     * @param option the long config option
     * @param min    minimum allowed value (inclusive)
     * @param max    maximum allowed value (inclusive)
     * @return this validator for chaining
     */
    public ConfigValidator longRange(ConfigOption<Long> option, long min, long max) {
        constraints.add(new LongRangeConstraint(option, min, max));
        return this;
    }

    /**
     * Registers a positive integer constraint (value must be > 0).
     *
     * @param option the integer config option
     * @return this validator for chaining
     */
    public ConfigValidator positiveInt(ConfigOption<Integer> option) {
        constraints.add(new IntRangeConstraint(option, 1, Integer.MAX_VALUE));
        return this;
    }

    /**
     * Registers a positive long constraint (value must be > 0).
     *
     * @param option the long config option
     * @return this validator for chaining
     */
    public ConfigValidator positiveLong(ConfigOption<Long> option) {
        constraints.add(new LongRangeConstraint(option, 1L, Long.MAX_VALUE));
        return this;
    }

    /**
     * Validates the given configuration against all registered constraints.
     *
     * @param config the configuration to validate
     * @throws ConfigValidationException if any constraint is violated
     */
    public void validate(PangrovaConfiguration config) {
        Objects.requireNonNull(config, "config must not be null");
        List<String> violations = new ArrayList<>();

        for (ConfigConstraint<?> constraint : constraints) {
            String violation = constraint.check(config);
            if (violation != null) {
                violations.add(violation);
            }
        }

        if (!violations.isEmpty()) {
            throw new ConfigValidationException(violations);
        }
    }

    // --- Constraint implementations ---

    private sealed interface ConfigConstraint<T> {
        String check(PangrovaConfiguration config);
    }

    private record RequiredConstraint<T>(ConfigOption<T> option) implements ConfigConstraint<T> {
        @Override
        public String check(PangrovaConfiguration config) {
            T value = config.get(option);
            if (value == null) {
                return "Required config '" + option.key() + "' is not set and has no default";
            }
            if (value instanceof String s && s.isBlank()) {
                return "Required config '" + option.key() + "' must not be blank";
            }
            return null;
        }
    }

    private record IntRangeConstraint(ConfigOption<Integer> option, int min,
                                      int max) implements ConfigConstraint<Integer> {
        @Override
        public String check(PangrovaConfiguration config) {
            Integer value = config.get(option);
            if (value == null) {
                return null; // not required, skip range check
            }
            if (value < min || value > max) {
                return "Config '" + option.key() + "' value " + value
                        + " is out of range [" + min + ", " + max + "]";
            }
            return null;
        }
    }

    private record LongRangeConstraint(ConfigOption<Long> option, long min,
                                       long max) implements ConfigConstraint<Long> {
        @Override
        public String check(PangrovaConfiguration config) {
            Long value = config.get(option);
            if (value == null) {
                return null;
            }
            if (value < min || value > max) {
                return "Config '" + option.key() + "' value " + value
                        + " is out of range [" + min + ", " + max + "]";
            }
            return null;
        }
    }
}
