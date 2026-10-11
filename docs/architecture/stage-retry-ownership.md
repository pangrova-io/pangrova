# Stage retry ownership

The production query dispatcher installs the retry handler when its real
ResilienceRegistry is present. Its configured maximum attempts and the actual
Stage retry limit both apply. Retry counts belong to the Stage of one execution;
there is no shared counter keyed only by numeric stage ID.

Missing, empty, unknown and ordinary control-cancellation error codes cannot be
reclassified as generic internal failures. Known node budget exhaustion remains
nonretryable. A duplicate failure notification cannot reset the newly prepared
attempt. The handler verifies the event's execution identity and exact DAG owner,
and serializes preparation on that Stage.

Actual Stage/Resilience4j regression tests cover these decisions; they do not
qualify Native task drain, cancelled-provider recovery, durable shuffle replay or
safe replay of external effects. Those full-chain contracts remain pending.
