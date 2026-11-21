#include "ExpressionFuzzer.h"
#include "JoinFuzzer.h"
#include "AggregationFuzzer.h"
#include "MemoryFuzzer.h"
#include "WriteFuzzer.h"
#include "PlanFuzzer.h"
#include "ReferenceEngine.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace pangrova::testing;
using Clock = std::chrono::steady_clock;

namespace {

struct RunStats {
    int totalPassed = 0;
    int totalFailed = 0;
};

enum class RunMode {
    ALL,        // Round-robin all fuzzers (default)
    EXPRESSION, // Expression fuzzer only
    PLAN,       // Plan fuzzer only (scan/filter/project/agg/join/sort)
    OPERATOR    // Expression + Plan combined (operator fuzzer mode)
};

void printUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]\n"
        << "\nOptions:\n"
        << "  --duration <seconds>     Run for specified duration (default: 60)\n"
        << "  --iterations <count>     Run specified number of iterations per fuzzer\n"
        << "                           (overrides --duration when set)\n"
        << "  --seed <value>           Random seed for reproducibility\n"
        << "  --mode <all|expression|plan|operator>\n"
        << "                           Fuzzer mode (default: all)\n"
        << "                           operator = expression + plan combined\n"
        << "  --batch-size <count>     Iterations per batch (default: 50)\n"
        << "  --output <path>          Write JSON results to file\n"
        << "  --help                   Show this help\n";
}

int parseDurationSeconds(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
            return std::atoi(argv[i + 1]);
        }
    }
    return 60;
}

int parseIterations(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            return std::atoi(argv[i + 1]);
        }
    }
    return 0;
}

uint64_t parseSeed(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            return static_cast<uint64_t>(std::atoll(argv[i + 1]));
        }
    }
    return static_cast<uint64_t>(
        Clock::now().time_since_epoch().count());
}

RunMode parseMode(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            if (std::strcmp(argv[i + 1], "expression") == 0) return RunMode::EXPRESSION;
            if (std::strcmp(argv[i + 1], "plan") == 0) return RunMode::PLAN;
            if (std::strcmp(argv[i + 1], "operator") == 0) return RunMode::OPERATOR;
        }
    }
    return RunMode::ALL;
}

int parseBatchSize(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--batch-size") == 0 && i + 1 < argc) {
            return std::atoi(argv[i + 1]);
        }
    }
    return 50;
}

const char* parseOutput(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            return argv[i + 1];
        }
    }
    return nullptr;
}

bool hasFlag(int argc, char* argv[], const char* flag) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], flag) == 0) return true;
    }
    return false;
}

const char* modeToString(RunMode mode) {
    switch (mode) {
        case RunMode::ALL: return "all";
        case RunMode::EXPRESSION: return "expression";
        case RunMode::PLAN: return "plan";
        case RunMode::OPERATOR: return "operator";
    }
    return "unknown";
}

void writeJsonResults(const char* path, RunMode mode, uint64_t seed,
                      int totalPassed, int totalFailed, int rounds,
                      double elapsedSec) {
    std::ofstream out(path);
    if (!out.is_open()) {
        std::cerr << "WARNING: Could not write results to " << path << std::endl;
        return;
    }
    out << "{\n"
        << "  \"mode\": \"" << modeToString(mode) << "\",\n"
        << "  \"seed\": " << seed << ",\n"
        << "  \"totalPassed\": " << totalPassed << ",\n"
        << "  \"totalFailed\": " << totalFailed << ",\n"
        << "  \"totalTests\": " << (totalPassed + totalFailed) << ",\n"
        << "  \"rounds\": " << rounds << ",\n"
        << "  \"elapsedSeconds\": " << elapsedSec << "\n"
        << "}\n";
    out.close();
}

} // namespace

int main(int argc, char* argv[]) {
    if (hasFlag(argc, argv, "--help")) {
        printUsage(argv[0]);
        return 0;
    }

    int durationSec = parseDurationSeconds(argc, argv);
    int iterations = parseIterations(argc, argv);
    uint64_t seed = parseSeed(argc, argv);
    RunMode mode = parseMode(argc, argv);
    int batchSize = parseBatchSize(argc, argv);
    const char* outputPath = parseOutput(argc, argv);

    std::cout << "=== Pangrova Query Fuzzer ===" << std::endl;
    std::cout << "Mode: " << modeToString(mode);
    if (iterations > 0) {
        std::cout << " | Iterations: " << iterations;
    } else {
        std::cout << " | Duration: " << durationSec << "s";
    }
    std::cout << " | Seed: " << seed << " | Batch: " << batchSize << std::endl;

    ReferenceEngine engine;
    RunStats stats;
    auto startTime = Clock::now();

    ExpressionFuzzer exprFuzzer(engine, seed);
    JoinFuzzer joinFuzzer(engine, seed + 1);
    AggregationFuzzer aggFuzzer(engine, seed + 2);
    MemoryFuzzer memFuzzer(engine, seed + 3);
    WriteFuzzer writeFuzzer(engine, seed + 4);
    PlanFuzzer planFuzzer(engine, seed + 5);

    auto runOne = [&](const char* name, auto& fuzzer, int count, int round) {
        auto result = fuzzer.run(count);
        stats.totalPassed += result.passed;
        stats.totalFailed += result.failed;
        if (!result.failures.empty()) {
            std::cerr << "[" << name << "] " << result.failed
                << " failures in round " << round << std::endl;
            for (const auto& f : result.failures) {
                std::cerr << "  " << f << std::endl;
            }
        }
    };

    int round = 0;

    if (iterations > 0) {
        int remaining = iterations;
        while (remaining > 0) {
            ++round;
            int batch = std::min(batchSize, remaining);

            switch (mode) {
                case RunMode::EXPRESSION:
                    runOne("Expression", exprFuzzer, batch, round);
                    break;
                case RunMode::PLAN:
                    runOne("Plan", planFuzzer, batch, round);
                    break;
                case RunMode::OPERATOR:
                    runOne("Expression", exprFuzzer, batch, round);
                    runOne("Plan", planFuzzer, batch, round);
                    break;
                case RunMode::ALL:
                    runOne("Expression", exprFuzzer, batch, round);
                    runOne("Query/Join", joinFuzzer, batch, round);
                    runOne("Aggregation", aggFuzzer, batch, round);
                    runOne("Memory", memFuzzer, batch, round);
                    runOne("Write", writeFuzzer, batch, round);
                    runOne("Plan", planFuzzer, batch, round);
                    break;
            }
            remaining -= batch;
        }
    } else {
        auto deadline = Clock::now() + std::chrono::seconds(durationSec);
        while (Clock::now() < deadline) {
            ++round;
            auto timeCheck = [&]() { return Clock::now() < deadline; };

            switch (mode) {
                case RunMode::EXPRESSION:
                    if (timeCheck()) runOne("Expression", exprFuzzer, batchSize, round);
                    break;
                case RunMode::PLAN:
                    if (timeCheck()) runOne("Plan", planFuzzer, batchSize, round);
                    break;
                case RunMode::OPERATOR:
                    if (timeCheck()) runOne("Expression", exprFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Plan", planFuzzer, batchSize, round);
                    break;
                case RunMode::ALL:
                    if (timeCheck()) runOne("Expression", exprFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Query/Join", joinFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Aggregation", aggFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Memory", memFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Write", writeFuzzer, batchSize, round);
                    if (timeCheck()) runOne("Plan", planFuzzer, batchSize, round);
                    break;
            }
        }
    }

    auto endTime = Clock::now();
    double elapsedSec = std::chrono::duration<double>(endTime - startTime).count();
    int total = stats.totalPassed + stats.totalFailed;

    std::cout << "\n=== Fuzzer Results ===" << std::endl;
    std::cout << "Mode: " << modeToString(mode) << std::endl;
    std::cout << "Total: " << total << " | Passed: " << stats.totalPassed
        << " | Failed: " << stats.totalFailed << std::endl;
    std::cout << "Rounds: " << round << " | Seed: " << seed
        << " | Elapsed: " << elapsedSec << "s" << std::endl;

    if (outputPath) {
        writeJsonResults(outputPath, mode, seed, stats.totalPassed,
                         stats.totalFailed, round, elapsedSec);
        std::cout << "Results written to: " << outputPath << std::endl;
    }

    if (stats.totalFailed > 0) {
        std::cout << "RESULT: FAILURES DETECTED" << std::endl;
        return 1;
    }
    std::cout << "RESULT: ALL PASSED" << std::endl;
    return 0;
}
