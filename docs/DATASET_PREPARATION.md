# Automatic dataset preparation

`register-dataset FILE` registers a logical dataset ID, version, expected identity SHA256,
metadata filenames and per-node preparation/verification recipes. `dataset-status` shows
the resulting CPU jobs and verified bindings. See the CSSA integration in the research
repository for a complete example.

Each replica specifies its destination `path`, `cwd`, `prepare_argv`, `verify_argv`, optional
`env`, immutable `input_files`, CPU/RAM `resources` and `max_attempts`. No GPU reservation
is allowed for preparation. Dataset-level `max_parallel_prepares` defaults to 2.
Recipes must be idempotent and perform their domain-specific file/hash/split checks.
They may copy or generate metadata only as explicitly specified by the operator.

When a queued consumer lacks a registered replica on an eligible node, the controller
creates one ordinary CPU preparation job per dataset/node. Existing matching markers
skip preparation but still run verification. An existing wrong marker is never overwritten.
After command success, identity and metadata hashes are recorded in `DATASET_READY.json`.
Only a verified success receipt installs `node.datasets[id]` and the optional `asset_name`
binding. Resource snapshots are refreshed before the downstream workload can start.

Other verified nodes may run consumers while a new node is still preparing. Failed preparation
has bounded ordinary-job retries and does not mark a replica ready. Preparation jobs use the
catalog's `project` for campaign reporting. Already registered dataset/asset mappings retain
their existing behavior. Changed contracts require a new dataset ID. A failed preparation
can be retried using `retry-failed JOB_ID`; existing successful mappings are not silently
repaired or changed on identity mismatch.

The catalog explicitly lists approved roots and commands. The scheduler never guesses
equivalence from directory names, scans unrelated filesystem roots, changes dataset versions,
or invents source/transfer commands. Existing user-defined transfer policies still apply.
