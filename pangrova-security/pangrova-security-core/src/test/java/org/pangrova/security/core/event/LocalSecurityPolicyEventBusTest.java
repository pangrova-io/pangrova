package org.pangrova.security.core.event;

import org.pangrova.testing.testkit.security.event.LocalSecurityPolicyEventBus;

import org.pangrova.security.api.event.SecurityPolicyChangeEvent;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.4 — LocalSecurityPolicyEventBus dispatches events with fault isolation.
 */
class LocalSecurityPolicyEventBusTest {

    @Test
    void publishesEventToAllListeners() {
        LocalSecurityPolicyEventBus publisher = new LocalSecurityPolicyEventBus();
        List<SecurityPolicyChangeEvent> received1 = new ArrayList<>();
        List<SecurityPolicyChangeEvent> received2 = new ArrayList<>();
        publisher.subscribe(received1::add);
        publisher.subscribe(received2::add);

        SecurityPolicyChangeEvent event = new SecurityPolicyChangeEvent.RlsPolicyChanged(
                "iceberg", "default", "orders", "policy1");
        publisher.publish(event);

        assertThat(received1).containsExactly(event);
        assertThat(received2).containsExactly(event);
    }

    @Test
    void isolatesListenerFailures() {
        LocalSecurityPolicyEventBus publisher = new LocalSecurityPolicyEventBus();
        List<SecurityPolicyChangeEvent> received = new ArrayList<>();
        publisher.subscribe(e -> { throw new RuntimeException("boom"); });
        publisher.subscribe(received::add);

        SecurityPolicyChangeEvent event = new SecurityPolicyChangeEvent.ColumnMaskChanged(
                "iceberg", "default", "users", "email");
        publisher.publish(event);

        // Second listener still receives the event despite first failing
        assertThat(received).containsExactly(event);
    }

    @Test
    void rejectsNullListener() {
        LocalSecurityPolicyEventBus publisher = new LocalSecurityPolicyEventBus();
        assertThatThrownBy(() -> publisher.subscribe(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullEvent() {
        LocalSecurityPolicyEventBus publisher = new LocalSecurityPolicyEventBus();
        assertThatThrownBy(() -> publisher.publish(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void roleAssignmentChangedEventHasEmptyTableFields() {
        SecurityPolicyChangeEvent event = new SecurityPolicyChangeEvent.RoleAssignmentChanged(
                "alice", "analyst");
        assertThat(event.catalog()).isEmpty();
        assertThat(event.schema()).isEmpty();
        assertThat(event.table()).isEmpty();
    }
}
