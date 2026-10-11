# Bounded control result publication

DDL/SHOW/DML acknowledgements use the existing Coordinator result service and Arrow IPC transport. Relational query rows remain Native-owned. The control buffer accounts the actual heap byte-array payload retained by each entry; its metric is `bufferedBytes`, not off-heap resident memory.

The previous buffer independently checked ConcurrentHashMap size and a byte counter, silently returned when full, and scheduled expiry after publication. Eight actual concurrent six-byte publications could all report success against a ten-byte limit; a stopped real scheduler left an entry published after rejected expiry scheduling. The service invoked its completion callback even after silent quota rejection. Closing the buffer did not seal publication or cancel its original expiry tasks.

One short ownership monitor now serializes capacity admission, exact query identity, publication and rollback. Quota failure propagates before the service completion callback. A repeated identity accepts only the same bytes; conflicting bytes are rejected. Failed real scheduler admission rolls back the same entry and debit. Entry-specific expiry cannot remove a later publication with the same key. Close seals further publication and cancels the actual retained expiry futures before dropping the map-owned payload.

This map-owned payload accounting does not measure all JVM resident memory or borrowed in-flight transport buffers. The transport allocator has its own actual bound and drain requirement. End-to-end shutdown and distributed query validation remain required.

Primary references: [JDK17 ConcurrentHashMap](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html) documents per-entry concurrency separately from aggregate operations; [JDK17 ScheduledThreadPoolExecutor](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ScheduledThreadPoolExecutor.html) documents rejected execution and cancelled delayed-task retention. The implementation retains the supplied scheduler and cancels only its own entry futures; it does not modify a shared executor policy.

The started control transport exposes its actual buffer to the query lifecycle owner.
After transport readers join, shutdown retires the buffered acknowledgement bytes
and cancels their owned expiry futures before closing the writer allocator.
Startup failure also closes this buffer. The direct bootstrap integration test
checks that the published acknowledgement is retained until explicit buffer close;
full Coordinator lifecycle and distributed shutdown qualification remain pending.
