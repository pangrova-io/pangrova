package org.pangrova.testing.fuzzer;

import java.util.List;
import java.util.Objects;

/**
 * Coordinates C++ fuzzer execution from the Java side via a native bridge.
 */
public final class FuzzerCoordinator {

    public enum FuzzerType {
        EXPRESSION, JOIN, AGGREGATION, MEMORY, WRITE, PLAN
    }

    public record FuzzerResult(
        FuzzerType type,
        int totalTests,
        int passed,
        int failed,
        List<String> failures) {}

    @FunctionalInterface
    public interface NativeFuzzerBridge {
        FuzzerResult run(FuzzerType type, int iterations);
    }

    private final NativeFuzzerBridge bridge;

    public FuzzerCoordinator(NativeFuzzerBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge);
    }

    public FuzzerResult runFuzzer(FuzzerType type, int iterations) {
        return bridge.run(type, iterations);
    }
}
