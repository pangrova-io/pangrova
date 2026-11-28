package org.pangrova.server.coordinator.testinfra;

import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.runtime.QueryManager;

import static org.assertj.core.api.Assertions.assertThat;

public final class QuerySubmissionTestSupport {
    private QuerySubmissionTestSupport() {}

    public static QueryStateMachine.Status awaitResultOrTerminal(QueryManager manager, SubmitQueryResponse receipt)
            throws Exception {
        assertThat(receipt.state()).isEqualTo("QUEUED");
        assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.PENDING);
        assertThat(receipt.resultEndpoint()).isNull();
        var owner = manager.getStateMachine(receipt.queryId());
        assertThat(owner).isNotNull();
        return owner.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, 0).get(10, TimeUnit.SECONDS);
    }

    public static void assertAdmissionThenWorkerFailure(QueryManager manager, HttpResponse<String> response)
            throws Exception {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        var receipt = JsonMapperFactory.shared().readValue(response.body(), SubmitQueryResponse.class);
        var outcome = awaitResultOrTerminal(manager, receipt);
        assertThat(outcome.state()).isEqualTo(QueryState.FAILED);
        assertThat(outcome.error().errorCode()).isEqualTo("COORDINATOR-0001");
        assertThat(outcome.error().message()).contains("No available worker for task dispatch");
    }
}
