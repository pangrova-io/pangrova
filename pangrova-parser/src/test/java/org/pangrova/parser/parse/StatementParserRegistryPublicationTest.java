package org.pangrova.parser.parse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.api.parse.SqlParser;
import org.pangrova.parser.api.parse.StatementParser;

import static org.assertj.core.api.Assertions.assertThat;

class StatementParserRegistryPublicationTest {
    @Test
    void readersObserveOldCompleteRegistrationUntilSortedReplacementIsPublished() throws Exception {
        var registry = new StatementParserRegistry();
        var original = parser("ping", "PING", 10);
        registry.register(original);
        registry.register(parser("health", "HEALTH", 20));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        StatementParser replacement = new StatementParser() {
            public String name() { return "ping"; }
            public int priority() {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("sort not released");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(failure);
                }
                return 5;
            }
            public boolean canParse(String sql) { return sql.equals("PING"); }
            public Statement parse(String sql, SqlParser parent) { return new ShowCatalogsStatement(); }
        };
        var executor = Executors.newSingleThreadExecutor();
        try {
            var registration = executor.submit(() -> registry.register(replacement));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(registry.findParser("PING")).containsSame(original);
            assertThat(registry.registeredParsers()).hasSize(2).contains(original);
            release.countDown();
            registration.get(5, TimeUnit.SECONDS);
            assertThat(registry.findParser("PING")).containsSame(replacement);
            assertThat(registry.registeredParsers()).hasSize(2).doesNotContain(original);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static StatementParser parser(String name, String sql, int priority) {
        return new StatementParser() {
            public String name() { return name; }
            public int priority() { return priority; }
            public boolean canParse(String input) { return input.equals(sql); }
            public Statement parse(String input, SqlParser parent) { return new ShowCatalogsStatement(); }
        };
    }
}
