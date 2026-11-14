package org.pangrova.governance.runtime.quality;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleStore;

/**
 * In-memory {@link QualityRuleStore} backed by {@link ConcurrentHashMap}.
 * Default implementation when no external persistence is configured.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for rule storage,
 * ConcurrentLinkedQueue for bounded result history.</p>
 */
@ThreadSafe
public final class InMemoryQualityRuleStore implements QualityRuleStore {

    private static final int MAX_RESULT_HISTORY = 1000;

    private final ConcurrentHashMap<Long, DataQualityRule> rules = new ConcurrentHashMap<>();
    private final Queue<QualityCheckResult> resultHistory = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger resultCount =
            new java.util.concurrent.atomic.AtomicInteger(0);

    @Override
    public void saveRule(DataQualityRule rule) {
        rules.put(rule.ruleId(), rule);
    }

    @Override
    public Optional<DataQualityRule> getRule(long ruleId) {
        return Optional.ofNullable(rules.get(ruleId));
    }

    @Override
    public List<DataQualityRule> getRulesForTable(String catalogName, String schemaName,
                                                   String tableName) {
        return rules.values().stream()
                .filter(r -> catalogName.equals(r.catalogName())
                        && schemaName.equals(r.schemaName())
                        && tableName.equals(r.tableName()))
                .toList();
    }

    @Override
    public List<DataQualityRule> getAllEnabledRules() {
        return rules.values().stream()
                .filter(DataQualityRule::enabled)
                .toList();
    }

    @Override
    public void saveResult(QualityCheckResult result) {
        resultHistory.offer(result);
        if (resultCount.incrementAndGet() > MAX_RESULT_HISTORY) {
            resultHistory.poll();
            resultCount.decrementAndGet();
        }
    }

    /**
     * Returns the most recent quality check results, up to {@value MAX_RESULT_HISTORY}.
     */
    public List<QualityCheckResult> getRecentResults() {
        return List.copyOf(resultHistory);
    }
}
