package org.pangrova.coordinator.bootstrap;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that bootstrap emits structured DEBUG logs when optional
 * modules are disabled and null components are encountered.
 *
 * <p>Uses Logback ListAppender to capture log output (no mocks).</p>
 *
 * <p>Validates: Requirements R2.4 — Null Propagation Logging</p>
 */
class NullComponentBootstrapLoggingTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger bootstrapLogger;
    private CoordinatorBootstrap bootstrap;

    @BeforeEach
    void setUp() {
        bootstrapLogger = (Logger) LoggerFactory.getLogger(QueryBootstrap.class);
        appender = new ListAppender<>();
        appender.start();
        bootstrapLogger.addAppender(appender);
        bootstrapLogger.setLevel(Level.DEBUG);
    }

    @AfterEach
    void tearDown() {
        bootstrapLogger.detachAppender(appender);
        appender.stop();
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    @Test
    void resilienceDisabled_emitsCircuitBreakerSkipLog() {
        CoordinatorBootstrapConfig config =
                CoordinatorBootstrapConfig.withDefaults("coord-cb-log-test", 0);
        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        assertThat(appender.list.stream()
                .filter(e -> e.getLevel() == Level.DEBUG)
                .map(ILoggingEvent::getFormattedMessage)
                .anyMatch(msg -> msg.contains("event=circuit_breaker_skip")
                        && msg.contains("reason=resilience_disabled")))
                .as("Expected DEBUG log with event=circuit_breaker_skip reason=resilience_disabled")
                .isTrue();
    }
}
