# Control-plane transport TLS and ownership

The Coordinator acknowledgement endpoint uses Java Arrow Flight; query rows continue through the Native Worker result endpoint. Coordinator bootstrap uses the existing configured internal transport certificate, private key and root CA for the acknowledgement server. Configured TLS requires client certificates and advertises `grpc+tls`; the interceptor constructor honors the configured allocator limit.

Arrow Java 19.0.0 `FlightServer.Builder.useMTlsClientVerification` configures `ClientAuth.REQUIRE` and the supplied trust manager. Enabling server TLS and printing an mTLS log does not verify client certificates. Source: [official versioned implementation](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-core/src/main/java/org/apache/arrow/flight/FlightServer.java).

The server now rejects partial certificate/key configuration and requested mTLS without a trust chain, verifies readable regular files, opens certificate inputs with owned streams, and actually configures client verification. Start failure performs owned cleanup. Lifecycle methods serialize start/stop; a close interrupted before termination or an allocator with live bytes retains the owner and reports failure, permitting cleanup retry after actual release. A stopped instance cannot reuse a closed allocator.

The private actual component uses JDK-generated CA/leaf identities and production Arrow Flight, TransportServer and CoordinatorDataProducer. Trusted client RPC succeeds; absent and foreign client certificates fail. An actual live 8192-byte Arrow allocation prevents successful stop, retains its bytes, and releases before successful retry. This is component evidence, not full Native/query-plane/TLS deployment qualification. The expected negative allocation diagnostic is retained in the log; it is not a zero-runtime-warning claim.

SpillDiskMonitor now reports the actual configured directory's FileStore usable bytes. A regular file or absent directory is not a configured spill directory. No fake storage or transport implementation is used by the added integration test.

The owning Maven run enables Arrow allocator checks and exposed that calling `close()` with live memory closes the allocator before throwing. The server now rejects close before calling the allocator when actual allocated bytes remain, so the original buffer can still release and shutdown can retry. Earlier standalone runs without allocator checks did not qualify this failure-recovery boundary; the failed owning run is preserved.

Coordinator-to-Worker task dispatch and cancellation, Worker registration, heartbeats,
and status reports use managed internal HTTP clients with an explicit direct proxy
selector. JVM or desktop HTTP proxy settings must not reroute these cluster control
messages. JDK 17 specifies `ProxySelector.of(null)` as the selector that disables
proxying; an absent selector can still inherit a default one. The selected Worker
address remains subject to the normal connection and identity checks.

An owning integration test starts the actual Coordinator acknowledgement bootstrap with JDK-generated trust material, verifies its encrypted endpoint and allocator limit, performs an authenticated Flight RPC, and rejects a missing client certificate. This verifies the configured bootstrap boundary; distributed query-plane and deployment-wide TLS remain separate qualification requirements.

Result consumers may pass an absolute epoch-millisecond query deadline to
`FlightResultFetcher.fetchStream`. The deadline bounds stream opening and each
batch wait in addition to the idle timeout, so continuously arriving batches cannot
keep a cancelled-by-deadline read alive. Deadline failure closes the owned stream;
the fetcher remains available for subsequent reads. The existing overload retains
its idle-timeout behavior.

`ArrowBatchDecoder.decode` also accepts an aggregate row limit and a per-decode
allocator limit. It checks each batch against the remaining row budget before
creating Java row objects. A row-limit failure closes the reader and allocator.
The allocator limit bounds Arrow allocation, not total JVM heap usage; consumers
remain responsible for bounding serialized input and retained results.
