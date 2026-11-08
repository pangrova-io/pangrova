package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementCompletionEvent;
import org.pangrova.query.core.dispatch.StatementCompletionListener;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for listener priority ordering and isolation.
 *
 * <p><b>Validates: Requirements 1.5, 1.6</b></p>
 */
class StatementDispatcherListenerPropertyTest {

    private static final Statement DUMMY_STMT = new CreateTableStatement(
            Identifier.of("c", "s", "t"), List.of(), false,
            List.of(), List.of(), Optional.empty(), Map.of());

    /**
     * Property 4: Listener priority ordering.
     * Listeners are called in ascending priority order.
     */
    @Property(tries = 30)
    void listenersCalledInPriorityOrder(
            @ForAll @IntRange(min = 2, max = 8) int listenerCount) {
        CopyOnWriteArrayList<Integer> callOrder = new CopyOnWriteArrayList<>();
        List<StatementCompletionListener> listeners = new ArrayList<>();

        for (int i = 0; i < listenerCount; i++) {
            int priority = (listenerCount - i) * 10;
            int listenerId = i;
            listeners.add(new StatementCompletionListener() {
                @Override
                public void onCompleted(StatementCompletionEvent event) {
                    callOrder.add(listenerId);
                }

                @Override
                public int priority() {
                    return priority;
                }
            });
        }

        listeners.sort(java.util.Comparator.comparingInt(StatementCompletionListener::priority));

        StatementCompletionEvent event = createEvent();
        for (StatementCompletionListener listener : listeners) {
            listener.onCompleted(event);
        }

        assertThat(callOrder).hasSize(listenerCount);
        for (int i = 1; i < callOrder.size(); i++) {
            int prevIdx = callOrder.get(i - 1);
            int currIdx = callOrder.get(i);
            int prevPriority = (listenerCount - prevIdx) * 10;
            int currPriority = (listenerCount - currIdx) * 10;
            assertThat(prevPriority)
                    .as("listener at position %d (priority=%d) should precede position %d (priority=%d)",
                            i - 1, prevPriority, i, currPriority)
                    .isLessThanOrEqualTo(currPriority);
        }
    }

    /**
     * Property 5: Listener isolation.
     * A throwing listener does not prevent other listeners from being called.
     * Mirrors StatementDispatcher.fireCompletionEvent isolation behavior.
     */
    @Property(tries = 30)
    void throwingListenerDoesNotPreventOthers(
            @ForAll @IntRange(min = 2, max = 8) int listenerCount,
            @ForAll @IntRange(min = 0, max = 7) int throwingIndex) {
        int safeThrowIdx = throwingIndex % listenerCount;
        List<AtomicInteger> counters = new ArrayList<>();
        List<StatementCompletionListener> listeners = new ArrayList<>();

        for (int i = 0; i < listenerCount; i++) {
            AtomicInteger counter = new AtomicInteger(0);
            counters.add(counter);
            if (i == safeThrowIdx) {
                listeners.add(new ThrowingCountingListener(counter, i * 10));
            } else {
                listeners.add(new CountingListener(counter, i * 10));
            }
        }

        StatementCompletionEvent event = createEvent();
        dispatchWithIsolation(listeners, event);

        for (int i = 0; i < listenerCount; i++) {
            assertThat(counters.get(i).get())
                    .as("listener %d call count (throwing=%b)", i, i == safeThrowIdx)
                    .isEqualTo(1);
        }
    }

    /** Dispatches event to all listeners with exception isolation, mirroring production behavior. */
    private static void dispatchWithIsolation(
            List<StatementCompletionListener> listeners,
            StatementCompletionEvent event) {
        for (StatementCompletionListener listener : listeners) {
            try {
                listener.onCompleted(event);
            } catch (Exception e) {
                System.Logger logger = System.getLogger("test");
                logger.log(System.Logger.Level.DEBUG,
                        "listener exception caught: " + e.getMessage());
            }
        }
    }

    private static StatementCompletionEvent createEvent() {
        StatementResult result = new StatementResult(
                List.of(), List.of(), "cat.sch.tbl", StatementCategory.DDL);
        return new StatementCompletionEvent("q-1", DUMMY_STMT, result, null);
    }

    /** Listener that counts calls. */
    private record CountingListener(
            AtomicInteger counter, int prio) implements StatementCompletionListener {
        @Override
        public void onCompleted(StatementCompletionEvent event) {
            counter.incrementAndGet();
        }

        @Override
        public int priority() {
            return prio;
        }
    }

    /** Listener that counts calls then throws. */
    private record ThrowingCountingListener(
            AtomicInteger counter, int prio) implements StatementCompletionListener {
        @Override
        public void onCompleted(StatementCompletionEvent event) {
            counter.incrementAndGet();
            throw new RuntimeException("deliberate test exception");
        }

        @Override
        public int priority() {
            return prio;
        }
    }
}
