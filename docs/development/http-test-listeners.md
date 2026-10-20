# HTTP listener addresses and dynamically assigned ports

Bind a local fixture to the same literal address advertised to its clients.
For example, construct a Coordinator with
`new CoordinatorHttpServer(new InetSocketAddress("127.0.0.1", 0), context)`
and publish `http://127.0.0.1:<actual bound port>`. The shared HTTP factory also
accepts an `InetSocketAddress`. The existing integer-port entry point retains
wildcard binding for services that need it.

For kernel-assigned ports (`0`), the factory disables `SO_REUSEADDR`. These
listeners have no fixed restart port. On the tested BSD host, address reuse
allowed an existing loopback SSH listener and a wildcard HTTP listener to bind
the same port; requests to the more specific address reached SSH. Merely
increasing task-report retries cannot correct the endpoint identity.

An explicit loopback binding also prevents a later listener from taking the
same exact address. Disabling reuse on a wildcard listener does not reserve
every individual interface address against other processes. Fixed-port
listeners retain Jetty's address-reuse behavior for restart compatibility.
The JDK defines the exact effects of
[`SO_REUSEADDR`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/StandardSocketOptions.html#SO_REUSEADDR)
as platform dependent; validation therefore includes real sockets.

`EmbeddedHttpServerPortOwnershipTest` checks both directions with live sockets:
an explicit loopback server rejects a second listener, and the ephemeral-port
binding policy rejects an already occupied address-specific port. The latter
injects the conflicting port through Jetty's public connector API while the
first socket remains open, avoiding a released-port reservation race. Failed
startup cleanup uses a zero graceful-stop timeout because the servlet context
never started. No test requires a VM or an SSH daemon.
