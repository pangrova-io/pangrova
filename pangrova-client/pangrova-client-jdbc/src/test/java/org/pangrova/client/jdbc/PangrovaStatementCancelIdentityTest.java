package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual HTTP failures and response races; identity ownership, not remote execution-drain proof. */
class PangrovaStatementCancelIdentityTest {
    @Test
    void failedRpcRetainsIdentityForExplicitRetry() throws Exception {
        retryAfterRejectedReply(new StatementCancellationPeer.Reply(403,
                "{\"error_code\":\"FORBIDDEN\",\"message\":\"cancel denied\"}"));
    }

    @Test
    void malformedAcknowledgementRetainsIdentityForExplicitRetry() throws Exception {
        retryAfterRejectedReply(new StatementCancellationPeer.Reply(202, "{\"outcome\":\"CANCEL_ACCEPTED\"}"));
    }

    @Test
    void notFoundCannotRetireAnActiveIdentity() throws Exception {
        retryAfterRejectedReply(StatementCancellationPeer.absent("query-retry"));
    }

    private static void retryAfterRejectedReply(StatementCancellationPeer.Reply response) throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("query-retry", null);
            query.replies.add(response);
            var execution = peer.executeAsync();
            assertThat(query.polled.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> peer.statement().cancel()).isInstanceOf(SQLException.class)
                    .hasMessageContaining(query.id);
            assertThat(query.cancelRequests.get()).isEqualTo(1);
            peer.statement().cancel();
            assertThat(query.cancelRequests.get()).isGreaterThanOrEqualTo(2);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
            assertThat((Throwable) execution.get(5, TimeUnit.SECONDS)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("cancelled");
        }
    }

    @Test
    void responseForOldQueryCannotClearANewerActiveIdentity() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var oldQuery = peer.query("query-old", null);
            var first = peer.executeAsync();
            assertThat(oldQuery.polled.await(5, TimeUnit.SECONDS)).isTrue();
            try (var response = oldQuery.blockCancel()) {
                var oldCancel = peer.cancelAsync();
                assertThat(response.entered.await(5, TimeUnit.SECONDS)).isTrue();
                oldQuery.state.set("CANCELLED");
                assertThat((Throwable) first.get(5, TimeUnit.SECONDS)).isInstanceOf(SQLException.class)
                        .hasMessageContaining("cancelled");
                var newQuery = peer.query("query-new", null);
                var second = peer.executeAsync();
                assertThat(newQuery.polled.await(5, TimeUnit.SECONDS)).isTrue();
                response.close();
                assertThat(oldCancel.get(5, TimeUnit.SECONDS)).isNull();
                peer.statement().cancel();
                assertThat(newQuery.cancelRequests.get()).isGreaterThanOrEqualTo(1);
                assertThat(newQuery.lastCancelled.get()).isEqualTo(newQuery.id);
                assertThat((Throwable) second.get(5, TimeUnit.SECONDS)).isInstanceOf(SQLException.class)
                        .hasMessageContaining("cancelled");
            }
        }
    }
}
