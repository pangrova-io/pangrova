package org.pangrova.testing.fuzzer;

import org.pangrova.testing.fuzzer.enums.FuzzerType;

import java.util.List;
import java.util.Objects;

/**
 * Coordinates C++ fuzzer execution from the Java side using a supplied runner.
 */
public final class FuzzerCoordinator {

    public record FuzzerResult(
        FuzzerType type,
        int totalTests,
        int passed,
        int failed,
        List<String> failures) {
        public FuzzerResult {
            failures = List.copyOf(failures);
        }

        @Override public List<String> failures() { return List.copyOf(failures); }
    }

    @FunctionalInterface
    public interface NativeFuzzerRunner {
        FuzzerResult run(FuzzerType type, int iterations);
    }

    private final NativeFuzzerRunner runner;

    public FuzzerCoordinator(NativeFuzzerRunner runner) {
        this.runner = Objects.requireNonNull(runner);
    }

    public FuzzerResult runFuzzer(FuzzerType type, int iterations) {
        return runner.run(type, iterations);
    }
}
