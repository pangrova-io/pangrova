# Local spool storage

`pangrova-shuffle-local` provides the `local-fs` implementation of `SpoolStoreFactory`.
It is a synchronous, single-owner file storage primitive. The production Coordinator
and Worker launchers do not currently assemble it as an exchange Provider. It does
not provide exchange receipts, network serving, distributed recovery or Celeborn integration.

## Configuration

Pass these properties in `SpoolStoreContext` when constructing the factory:

| Property | Default | Meaning |
| --- | --- | --- |
| `spool.local-fs.base-dir` | `${java.io.tmpdir}/pangrova-spool` | Exclusive directory for one Store |
| `spool.local-fs.max-open-files` | `1024` | Positive integer limiting data-file reservations across readers and writers |

These are factory properties, not newly supported Server YAML options. The Store's
directory-lock channel is additional to the data-file budget. Each open writer
reserves one slot; each reader reserves one slot per explicitly selected producer
attempt. Admission fails immediately if the complete selection cannot fit. No
producer is silently dropped, and no waiting queue is created.

## Ownership and completion

Use a directory private to the engine process, with no external writer modifying
its contents while the Store owns it. The Store holds an exclusive lock until all
its resources have closed. Filesystems must supply directory identities through
`BasicFileAttributes.fileKey()`; unavailable identities cause an explicit failure.

The reader opens every selected completed segment before `read` returns. Replacing
a selected path after that point cannot redirect a later batch to another file.
The Store rejects symbolic links in data paths and detects root or partition
replacement before mutations. These identity checks are not race-free OS sandboxing
against an untrusted local process changing paths between a check and an operation.

Writers publish one versioned, checksummed attempt segment with an atomic move.
There is no non-atomic fallback. This completion does not publish an exchange or
prove durability across a power failure. An empty producer still needs a completed
segment; a missing segment is an error. Readers use the exact producer/attempt
selection rather than selecting the latest attempt.

Always close readers and writers, including after EOF or successful completion.
Their reservations and query cleanup leases last until close. A failed partial
reader open closes the channels already acquired and returns its reservation.
Closing the Store closes remaining resources before releasing its directory lock.
If cleanup fails, the failure is reported and ownership is retained for a later
close attempt; foreign replacement directories are not silently cleaned up.

I/O is serialized by the Store monitor. This component does not claim asynchronous
backpressure, nonblocking cancellation or cross-node availability. A raised
file budget should account for all other process file descriptors as well.
