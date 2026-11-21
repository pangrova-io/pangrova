package org.pangrova.ai.worker.invocation.property;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiOperation;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.capability.AiRequestConstraints;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiPhysicalResourceEnvelopePropertyTest {
    @Property(tries = 60, seed = "8675309")
    void exactPhysicalEnvelopeIsInclusiveAndRejectedRetriesCannotChargeAgain(
            @ForAll @IntRange(min = 1, max = 4) int attempts,
            @ForAll @IntRange(min = 1, max = 64) int bytes) {
        long cumulativeBytes = (long) attempts * bytes;
        var limits = new AiTransportTestAdmission.Limits(2, 1, attempts,
                cumulativeBytes, cumulativeBytes, 0, new AiRequestConstraints(100, 100, bytes, bytes));
        try (var budget = new AiTransportTestAdmission(AiOperation.TEXT_GENERATION,
                0, 1, 1, limits, Instant.now().plusSeconds(30))) {
            for (int i = 0; i < attempts; i++) {
                try (var send = budget.openAttempt(bytes)) { send.acceptResponseBytes(bytes); }
            }
            assertThatThrownBy(() -> budget.openAttempt(bytes)).isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> assertThat(((AiInvocationException) failure).category())
                            .isEqualTo(AiFailureCategory.BUDGET_EXCEEDED));
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(attempts);
            assertThat(budget.usage().getProviderItems()).isEqualTo(1);
            assertThat(budget.usage().getProviderRequestBytes()).isEqualTo(cumulativeBytes);
            assertThat(budget.usage().getProviderResponseBytes()).isEqualTo(cumulativeBytes);
            assertThat(budget.usage().getObservedProviderResponseBytes()).isEqualTo(cumulativeBytes);
        }
    }
}
