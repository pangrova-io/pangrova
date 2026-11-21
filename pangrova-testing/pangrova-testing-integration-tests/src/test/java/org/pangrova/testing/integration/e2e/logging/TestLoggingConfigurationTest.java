package org.pangrova.testing.integration.e2e.logging;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class TestLoggingConfigurationTest {

    @Test
    void dockerWireLoggingCannotPrintRequestArguments() throws IOException {
        var providers = Thread.currentThread().getContextClassLoader()
                .getResources("META-INF/services/org.slf4j.spi.SLF4JServiceProvider");
        assertThat(Collections.list(providers)).hasSize(1);
        assertThat(LoggerFactory.getILoggerFactory()).isInstanceOf(LoggerContext.class);
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        assertThat(context.getLogger("ROOT").getEffectiveLevel()).isEqualTo(Level.INFO);
        assertThat(context.getLogger("com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.wire")
                .getEffectiveLevel()).isEqualTo(Level.WARN);
        assertThat(context.getLogger("org.testcontainers.shaded.com.github.dockerjava.core.command.AbstrDockerCmd")
                .getEffectiveLevel()).isEqualTo(Level.INFO);
        assertThat(context.getLogger("org.eclipse.jetty.http.HttpParser").getEffectiveLevel().toInt())
                .isGreaterThanOrEqualTo(Level.INFO_INT);
    }
}
