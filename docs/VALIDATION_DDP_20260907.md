# Dependency-aware placement and registered DDP alternatives

The scheduler suite passes 103 tests. Five added tests cover DDP4→DDP2 fallback on
two-device servers, the larger per-device VRAM requirement, preference for the default
configuration when it fits, inheritance of downstream priority, immutable attempt
resource selection, rejection of unregistered alternatives and preservation of exact
requests for jobs that do not opt in.

Alternatives are operator-validated execution configurations, not an automatic claim
of scientific equivalence. The SIX and PRECHECK integration supports DDP2 (batch 12
per GPU) and DDP4 (batch 6 per GPU), both with global batch 24. Six project-level
registration/execution-contract tests pass. Running attempts are not resized or restarted.
