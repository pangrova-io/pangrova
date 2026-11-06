package org.pangrova.shuffle.api.identity;

import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExchangeIdentityTest {
    private static final String UUID = "00000000-0000-0000-0000-000000000001";

    @Test
    void channelKeepsTaskPartitionDistinctFromExchangePartitionAndPreservesFences() {
        var fence = new ExchangeAttemptFence(Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
        var owner = new ShuffleExecutionOwner("owner", "incarnation", Long.MAX_VALUE);
        var attempt = new ExchangeTaskAttempt("query", 3, 7, "task", Long.MAX_VALUE, fence, owner, UUID, UUID);
        var channel = new ExchangeChannelContext(attempt, 11, Long.MAX_VALUE);
        assertThat(channel.taskPartitionId()).isEqualTo(7);
        assertThat(channel.exchangePartitionId()).isEqualTo(11);
        assertThat(channel.planEpoch()).isEqualTo(Long.MAX_VALUE);
        assertThat(channel.fencingToken()).isEqualTo(Long.MAX_VALUE);
        assertThat(channel.attemptId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(channel.controlEpoch()).isEqualTo(Long.MAX_VALUE);
        assertThat(channel.executionOwner()).isEqualTo(owner);
        assertThat(channel.taskAttempt()).isEqualTo(attempt);
    }

    @Test
    void rejectsInvalidFenceAdmissionIdentityAndDeadlineBeforeProviderUse() {
        assertThatThrownBy(() -> new ExchangeAttemptFence(0, 0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExchangeAttemptFence(1, -1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExchangeAttemptFence(1, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        var fence = new ExchangeAttemptFence(1, 0, 1);
        var owner = new ShuffleExecutionOwner("owner", "incarnation", 1);
        assertThatThrownBy(() -> new ExchangeTaskAttempt("q", 1, 0, "task", 1, fence, owner,
                "1-1-1-1-1", UUID)).isInstanceOf(IllegalArgumentException.class);
        var attempt = new ExchangeTaskAttempt("q", 1, 0, "task", 1, fence, owner, UUID, UUID);
        assertThatThrownBy(() -> new ExchangeChannelContext(attempt, -1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExchangeChannelContext(attempt, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
