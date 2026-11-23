# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete production Worker
advertisement, negotiated through JNI `GET_CAPABILITIES` and enriched by
`NativeCapabilityAdvertisement` with the configured model declarations. The
packaged Native runtime initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

## Current capture

On 2026-11-23 the packaged macOS arm64 Worker handshake and matching function
exporter captured the typed scalar subquery relation. The function definitions,
registered signatures, vector companions and type projections match the prior capture.
Both fixtures carry descriptor SHA-256 `af6dc87f7a3364cce8618001c6a1d223b110d5a52fd0038c76039e695a3eed83`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | --- |
| Native library | 322902117 | `2445a748f1c0f11e0b8e241669850d668d1fe551a2f73ac03d1251b72892b5f9` |  |
| Worker inventory | 796871 | `99b9e47647c2d2e22e9a090cf678034fcc028658c29a9a43ddd30a0fc53ea360` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `9b0733d62d378bf5e1188bedc7684bfc8c04736166e0b42af31dd28bdf4b7391` | 9 / 92 / 0 |

## Earlier November 23 operator wait capture

On 2026-11-23 the packaged macOS arm64 Worker handshake and matching function
exporter captured the task-report protocol with per-operator blocked time and
selected wait causes. Both fixtures carry descriptor SHA-256
`584ccf8d14fd47a231c009051615d3d559c694cd03025cc12d1ce7ce3c0203a3`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | --- |
| Native library | 322897797 | `746ca93308ac46d33f923ecc182fe81d437595b00c1bd637593925f5cc2ca38a` | |
| Worker inventory | 796871 | `431651966b187974912be8f4bf390c614daa28fb6e292c1bd9109186a7cf6ea9` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `82ece65ae65d4feff6b8f5b9c3850614d78ff976796a889175d4eebe17f959fd` | 9 / 92 / 0 |

## Earlier November 23 sparse-key capture

On 2026-11-23 the packaged macOS arm64 Worker handshake and matching function
exporter captured the protocol for bounded disjoint build-key ranges used by
cross-stage Iceberg pruning. Both fixtures carry descriptor SHA-256
`37f4135b6b03274ca856e45c14238b44c476c8ebead26a3fe2d3bbebf25c571a`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | --- |
| Native library | 322897717 | `8e85b615bfdce500145f4d6c98dd91ae228b30a5fab6e773d3a4410e5b172cbf` | |
| Worker inventory | 796871 | `8261167dc59c96fd96d9a2df3fa28d74a4b493460c0a3dd4a3baf572362089a8` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `b7c7ef05a4f07a60b004a60741fb65ca04efe995e72ee8f597468221936170f5` | 9 / 92 / 0 |

## Earlier November 23 external Shuffle capture

On 2026-11-23 the packaged macOS arm64 handshake and function exporter captured
the protocol for reading contiguous groups of committed external Shuffle
partitions. Both fixtures carry descriptor SHA-256
`a15b8c21a150880296a1f9f942009e69fcbf1fec730fb1357d59d28cc459ab3a`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | --- |
| Native library | 322896581 | `3c7475121ba3eb9c18a1af2dd0afd84ebf50e1211ba2491a1030e94ab2f82c67` | |
| Worker inventory | 796871 | `c5c4c7f05b54731056510e76c230cebe23888e6b1f56a880829d0edd95b5d882` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `e660a45599823f027f1d56f6287556b75769d8cc87c664b36256259ae9f5a719` | 9 / 92 / 0 |

## November 22 build-key capture

On 2026-11-22 the packaged macOS arm64 handshake and function exporter captured
the protocol that carries the optional build-key ordinal and terminal range for
cross-stage Iceberg pruning. Both fixtures carry descriptor SHA-256
`f4678c47cbd8d809ae3ca4af404793664ad1d37875ffdde6a41e182eb752f4f9`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | --- |
| Native library | 322896533 | `e5a39846999af429968696658114d045aaf017af0d1a4d71edc2edc326ff8820` | |
| Worker inventory | 796871 | `7ae52d8f18ea4292d261f8593b9cfa46de44888d2579e6301a8f99d348821b88` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `243696b73fcb0187c34ba2946555fed89195d81366a7b915abe54430fb6b3561` | 9 / 92 / 0 |

## Earlier November 22 capture

On 2026-11-22 the packaged macOS arm64 handshake and function exporter captured
the protocol that carries typed file details in `DataSplit.file_detail`. Both
fixtures carry descriptor SHA-256
`9807e183c7e8e898eba126e76edf5c5340b5e1e8cd362adbb80909c36379f2f8`.

| Artifact | Bytes | SHA-256 | Capabilities / functions / models |
| --- | ---: | --- | ---: |
| Native library | 322875461 | `9bda01fe69b969102ee20daf9756522d88a3561485698351a35658ec70dc626d` | |
| Worker inventory | 796871 | `2bbef365dedbb9a90e6530532da414d114dc765c8c17f926f0a9d1c78bc98e92` | 19 / 108 / 2 |
| Function exporter catalogue | 784252 | `0a7561649d9f1989fd651ee746a3615de1710b3fb9c9627d761abd812660655d` | 9 / 92 / 0 |

## November 14 capture

On 2026-11-14 the macOS arm64 Worker handshake exported the current inventory
from the packaged JNI runtime with the immutable cache, governed FILE callbacks
and both configured models. The runtime exports 21 Worker native methods
under `org.pangrova.worker.jni.NativeRuntime`. The split descriptor includes
the published index artifact length and SHA-256. The cache-disabled catalogue came from the
`pangrova_function_export --catalogue` binary built alongside that runtime.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Native library | 322827909 | `6eab152f31b1f305763a87cd14d77baad01959fb4fef2a00a91a03859bc85e7d` |
| Worker inventory | 795600 | `9c4691f55ad19577ccd623af88b8344d0b4dd45d938157a9206afc6915831aa1` |
| Function exporter catalogue | 782981 | `66e4d0a405102d3d5e1b181b39201e5efe95a1401a8773856e4d9c3e32d8ed07` |

Both fixtures carry canonical descriptor SHA-256
`aeeee7695d8a30162edc9d4b0100a0a71651b0bd0e101c755b11d4976f52cbce`,
including the Coordinator partition publication evidence in the external exchange binding.
The Worker advertised 19 capabilities, 103 functions and two deployed models.
The exporter reported nine capabilities, 87 functions and zero models.

A later macOS arm64 handshake used the public Worker DML operation type,
its checked native operation codes, and task-local storage and statistics ownership.
Library SHA-256
`f93a50f7b7053256d2b1c5c439a9f4bee76255cf06f96cf42b7b05ee2c780256`
advertised the same 19 capabilities, 103 functions and descriptor. All Native
fields matched the fixture exactly. The complete 795,600-byte publication
from this deployment had SHA-256
`441c94151c3a27a3815bd939956879439fd6015f1706877960e7c7fdb08f102b`.
The two model records contain deployment-specific identities.

## October capture

The complete cache-enabled snapshot was recaptured on **2026-10-25** through
`NativeCapabilityHandshakeE2ETest`, after the worker-incarnation protocol change.
The current-source Native target built successfully and the packaged library
matched the build artifact. The real PostgreSQL/Ollama/JNI handshake passed 1/1
with no skipped tests and retained all 19 capabilities, 103 functions (including
the governed FILE callbacks), and two configured models. October capture receipts:

- Native library SHA-256: `6dbd54c01694177de9347cd9482121a1d91af95cc7ef3bbcfcc4ea1310942f38`.
- Inventory: 795,600 bytes, SHA-256
  `cd658bdd93f7e0ce23ba964ef3e997c300f7dafedba9f02495cb89339118bf87`.
- Canonical descriptor SHA-256:
  `8e19c1d352460f0f950613fb744ac0228048a6656da001b945dde5568bd46244`.
- Prepared Ollama image:
  `sha256:07fd8f7607f360ed6c29d2d976daa7ebcbe200a63b81e9daecbf561866d1cb66`.

The E2E harness now wires the production controlled-maintenance executor to real
PostgreSQL policy and lease stores. Passing `null` to the mandatory executor had
prevented coordinator startup before the handshake could run. Neither the
ownership checks nor the capability validator were relaxed for this recapture.
The protocol owner suite then passed all 465 tests across 81 classes, with zero
failures, errors, or skips, including all seven actual-inventory contract cases.
This receipt qualifies handshake and publication, not SQL feature completeness
or the full production regression.

## Historical capture and function exporter

The preceding snapshot was captured on 2026-10-21 after exposing the actual Native
`to_ieee754_32` and `to_ieee754_64` registrations through the shared SQL bindings.
This historical complete inventory came from the rebuilt runtime containing the
canonical nested-vector name binding, microsecond TIME bridge and UUID
identity/unsigned-ordering repairs. Both catalogues retain all 22 physical
projections, including UUID as its distinct logical type over HUGEINT.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `88d049d5767d84dc11349e6a6afcf2724ca1b344c34e453dadb1d6dc8d5fa8a6`):

- Exported catalogue: 782,981 raw bytes, SHA-256
  `1c462e4c925da95480712065b6c90f8b2e69aecdbe95d7e9602e673fee225dc0`
  (9 capabilities and 87 functions).

That historical complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `8131c2beb2797ca186cd1114d81dd2a8e79cfb2439b72efb6196b9942524144d`.
- Inventory: 795,600 raw bytes, SHA-256
  `5dc30e6ed0a7c4c18a77841ce3ecfa8a23d2e5011a138c899f6f47cb790855d1`.

The E2E capture retained all 19 capabilities and 103 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol owner run covered 448 tests against the UUID fixtures. Its three
position-dependent declaration checks failed after adding the UUID projection;
they now locate projections by declared kind and all three passed on targeted
rerun. The rolling union covers all 448 unique cases without failures or skips;
static analysis and installation also passed. The complete UUID capture passed
1/1, checking every Native field against the production Worker publication, then
asserting both configured model identities and exporting every model and
capability. Its source run also exposed UUID SQL/parser and cold Arrow
materialization failures; handshake success does not qualify those SQL paths.
Model instance signatures and binding hashes vary with the serving
instance, so these bytes are not compared as a global constant. The fixture
update preserves the full production publication.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; the ordinary handshake
E2E now retains the permanent optional capture entry point below. The fixture qualifies inventory validation against
that captured library. Subsequent UUID CAST-length and Parquet-reader repairs have
separate matching-library runtime evidence; this historical binary is not a claim
that it was recaptured from every later build. It does not qualify artifact execution, model invocation, or
end-to-end SQL.

## Recapturing the complete JNI inventory

After rebuilding and packaging the matching Native library, install its Java
consumer chain and run the ordinary packaged handshake E2E. It validates the
complete raw JNI inventory and verifies that Worker publication preserves every
Native field while adding both deployed models, before optionally exporting the
unmodified publication. Raw JNI inventory alone does not contain these Worker
model declarations. The
output path must be absolute. Use the existing local model fixture prerequisites
and a FILE capability key so the deployed model and FILE descriptors are retained:

```bash
export PANGROVA_E2E_FILE_CAPABILITY_KEY="$(openssl rand -base64 32)"
mvn -q -pl pangrova-testing/pangrova-testing-integration-tests -Pfull-regression \
  -Dtest=NativeCapabilityHandshakeE2ETest \
  -Dpangrova.e2e.ai.image-id="${PANGROVA_E2E_AI_IMAGE_ID:?set the verified local model image digest}" \
  -Dpangrova.e2e.native-inventory-output="$PWD/pangrova-protocol/src/test/resources/capability-contracts/actual-native-capability-inventory.bin" \
  test
```

The separate cache-disabled fixture is still produced by the matching
`pangrova_function_export --catalogue`. Record the two actual hashes and counts,
then run the affected protocol contracts; an export is not a test result.
