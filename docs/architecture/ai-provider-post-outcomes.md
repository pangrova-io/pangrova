# AI provider POST outcomes

Worker invokes an independently operated model endpoint through its governed AI
provider transport. Each embedding, inference, transcription, and reranking POST
requires physical-attempt admission, an absolute deadline, and bounded request and
response bytes. The `Idempotency-Key` header carries the stable invocation identity,
but its presence alone does not establish that the endpoint deduplicates requests.

An I/O failure after a POST starts leaves the model outcome unknown: the endpoint
may have computed a result before the response was lost. The transport reports
`PROVIDER_OUTCOME_UNCERTAIN` with `retryable=false`, preserving the original cause.
An interrupted send has the same conservative outcome. A returned non-success HTTP
status retains its status-derived error category, but does not authorize automatic
POST replay. Local rate admission can still wait or fail before the send.

The Worker SQL function bridge rethrows `AiInvocationException` unchanged, so
provider category and retryability survive Java function dispatch. Other Java
function failures keep the ordinary JNI exception wrapper.

The current OpenAI-compatible provider paths send a given POST once. Any future
replay requires a verified endpoint contract: durable deduplication by the exact
request identity and payload, a queryable outcome or equivalent receipt, and a
deadline and retention window that cover recovery. A client-supplied key without
those server properties is insufficient. This follows the non-idempotent-method
retry constraints in [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)
and the pre-application transparent-retry boundary in the
[gRPC retry guide](https://grpc.io/docs/guides/retry/).

Configured model capabilities and the default provider SPI advertise
`RetryPolicy.noRetry()` so logical admission reserves one physical attempt per
provider request. The retry policy participates in the immutable model identity;
deployment metadata and planned bindings must be generated from the same version.

The physical Task owner must treat an unknown outcome as terminal for its current
invocation and avoid publishing a model result. The JNI callback asks the Worker
bridge for a bounded canonical `ExecutionError`, records it against the exact
Native executor before propagating the Java exception to Velox, and retains the
first classified error when the Task enters FAILED. The callback authority and
executor handle both originate from the task-owned Native query configuration.
The Worker model-error projection, actual packaged JNI registration, and Native
terminal serializer have focused tests. A single real SQL Task through the
callback, cancellation, result publication, and coordinator retry boundaries
still needs an independent end-to-end test; these component checks do not prove
that chain or endpoint-side deduplication.

## Complete response deadlines

On JDK 17 the HTTP request timeout stops at response headers. The governed
transport therefore waits for its asynchronous response with an explicit deadline
covering the entire body, capped by the remaining task lifetime and configured
provider timeout. See the [OpenJDK timeout change for JDK 26](https://inside.java/2026/03/04/jdk-26-http-client/).

Deadline expiry and caller interruption seal the body subscriber before releasing
physical admission. The seal serializes with any in-progress byte accounting and
rejects later subscriptions or buffers. The client cancels its HTTP exchange;
this proves that the client will not retain or publish further response data. It
does not prove that the remote model stopped or never executed. These paths retain
`PROVIDER_OUTCOME_UNCERTAIN` and never authorize replay. A later explicitly admitted
invocation can acquire the released client concurrency slot.

## Task cancellation and batch ownership

Each transport exchange registers with the exact task budget before submission.
Task revocation seals admission, cancels all registered exchanges, and joins their
sending-thread finalization outside the budget lock. Body callbacks are sealed
before physical permits are released. Cancellation between registration and send
prevents submission. A sender or body callback cannot claim its own exit by
joining itself; that attempt fails explicitly and retains the task for cleanup.
This is client resource termination, not a remote model cancellation guarantee.

Both chat-completions and Responses batches retain every accepted child, even if
a later executor submission is rejected. They wait for accepted suppliers to exit
without treating future cancellation as termination. Results retain input order.
If any child has an unknown POST outcome, that category takes precedence over a
known HTTP failure or submission rejection and remains non-retryable. Other child
failures are retained as suppressed exceptions on the aggregate typed failure.
