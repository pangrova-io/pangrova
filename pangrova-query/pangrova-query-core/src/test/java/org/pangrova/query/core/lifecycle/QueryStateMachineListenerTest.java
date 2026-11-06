package org.pangrova.query.core.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.query.api.lifecycle.QueryState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying QueryStateMachine listener mechanism for journal integration.
 * Ensures listeners fire on transitions and do not block the CAS thread.
 */
class QueryStateMachineListenerTest {

    @Test
    void listenerFiresOnEveryTransition() {
        QueryStateMachine sm = new QueryStateMachine();
        List<QueryState> observed = Collections.synchronizedList(new ArrayList<>());
        sm.addStateChangeListener((oldState, newState) -> observed.add(newState));

        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.tryTransition(QueryState.FINISHING);
        sm.tryTransition(QueryState.FINISHED);

        assertEquals(List.of(QueryState.PLANNING, QueryState.SCHEDULING, QueryState.RUNNING,
                QueryState.FINISHING, QueryState.FINISHED), observed);
    }

    @Test
    void listenerDoesNotBlockTransition() throws InterruptedException {
        QueryStateMachine sm = new QueryStateMachine();
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch listenerRelease = new CountDownLatch(1);

        sm.addStateChangeListener((oldState, newState) -> {
            if (newState == QueryState.PLANNING) {
                listenerEntered.countDown();
                try {
                    // Simulate slow listener — should NOT block other transitions
                    listenerRelease.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // Trigger PLANNING transition (listener will block)
        Thread t = new Thread(() -> sm.tryTransition(QueryState.PLANNING));
        t.start();
        assertTrue(listenerEntered.await(1, TimeUnit.SECONDS),
                "Listener should have been invoked");

        // The transition already completed (CAS succeeded before listener runs)
        assertEquals(QueryState.PLANNING, sm.getState());

        // Release the blocked listener
        listenerRelease.countDown();
        t.join(2000);
    }

    @Test
    void failedTransitionDoesNotFireListener() {
        QueryStateMachine sm = new QueryStateMachine();
        List<QueryState> observed = Collections.synchronizedList(new ArrayList<>());
        sm.addStateChangeListener((oldState, newState) -> observed.add(newState));

        // QUEUED → RUNNING is not a valid transition
        boolean result = sm.tryTransition(QueryState.RUNNING);

        assertFalse(result);
        assertTrue(observed.isEmpty(), "Listener should not fire on failed transition");
    }

    @Test
    void multipleListenersAllFire() {
        QueryStateMachine sm = new QueryStateMachine();
        List<String> log = Collections.synchronizedList(new ArrayList<>());

        sm.addStateChangeListener((old, next) -> log.add("A:" + next));
        sm.addStateChangeListener((old, next) -> log.add("B:" + next));

        sm.tryTransition(QueryState.PLANNING);

        assertEquals(2, log.size());
        assertTrue(log.contains("A:PLANNING"));
        assertTrue(log.contains("B:PLANNING"));
    }

    @Test
    void terminalStateIsNotPublishedBeforeDurableRecorderSucceeds() {
        QueryStateMachine sm = new QueryStateMachine();
        AtomicInteger calls = new AtomicInteger();
        sm.setBeforeTerminalTransition(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("durable intent unavailable");
            }
        });
        assertThrows(IllegalStateException.class, () -> sm.tryTransition(QueryState.FAILED));
        assertEquals(QueryState.QUEUED, sm.getState());
        assertTrue(sm.tryTransition(QueryState.FAILED));
        assertEquals(QueryState.FAILED, sm.getState());
        assertEquals(2, calls.get());
    }
}
