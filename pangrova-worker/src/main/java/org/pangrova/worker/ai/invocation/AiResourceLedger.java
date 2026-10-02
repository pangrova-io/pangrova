package org.pangrova.worker.ai.invocation;

import static org.pangrova.worker.ai.invocation.AiInvocationBudgetManager.budgetExceeded;

/** Remaining kernel resources; accessed only under the containing task's monitor. */
final class AiResourceLedger {
    private final AiResourceAmounts limit;
    private AiResourceAmounts remaining;

    AiResourceLedger(AiResourceAmounts limit) {
        this.limit = limit;
        this.remaining = limit;
    }

    void requireAvailable(AiResourceAmounts charge) {
        if (charge.items() > remaining.items() || charge.attempts() > remaining.attempts()
                || charge.requestBytes() > remaining.requestBytes()
                || charge.responseBytes() > remaining.responseBytes()
                || charge.mediaBytes() > remaining.mediaBytes()) {
            throw budgetExceeded("AI provider exceeds the remaining exact resource grant");
        }
    }

    void debit(AiResourceAmounts charge) {
        requireAvailable(charge);
        remaining = remaining.minus(charge);
    }

    void refund(AiResourceAmounts charge) {
        AiResourceAmounts next = remaining.plus(charge);
        limit.minus(next);
        remaining = next;
    }

    AiResourceAmounts used() { return limit.minus(remaining); }
}
