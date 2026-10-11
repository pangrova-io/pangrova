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
