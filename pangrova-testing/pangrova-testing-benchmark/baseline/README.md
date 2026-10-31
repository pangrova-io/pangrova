# Baseline ownership

The historical SF10 summaries and zero-valued SF100 file remain unchanged as
historical evidence. They do not satisfy current calibration contracts.

The ordinary suite comparator accepts three complete, successful, independent
windows with matching workload, data/snapshots, harness/client runtime, hardware,
resource and cache/configuration fingerprints. Create a bundle explicitly with
`tpc_suite_compare.py --calibrate`; first successful runs do not silently replace
a baseline. Keep immutable raw-window artifacts with the reviewed bundle.

Dedicated-host configuration names ordinary bundles
`<benchmark>-sf<scale>-streams<count>-baseline.json`. Tail calibration is separate:
`tpch-sf<scale>-tail-baseline.json`, explicitly created by
`calibrate_tpc_tail.py` from real complete samples and a verified tail context.
It retains the unchanged strict complete-query R7/P99 contract. Fixtures, transport-test IDs, maxima and copied historical summaries
must never populate either baseline.

See [the runbook](../../../docs/testing/tpc-benchmark-methodology.md).
