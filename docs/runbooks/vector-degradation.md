# Vector degradation

Vector retrieval is an approximate candidate generator, never a source of final ordering. Every returned ANN pool is reranked with the pinned quantized vectors and integer distance implementation before fusion.

If vector latency, malformed output, model revision mismatch, or index health breaches its deadline, record the channel degradation and continue only when all required deterministic channels completed. Confirm responses show `approximate_candidates=false` when the vector channel did not complete.

Quarantine a generation when its embedding manifest, dimension, distance function, normalization, table name, index name, or content hash differs from the activated manifest. Build a new immutable generation, measure recall against an exact-distance sample, and activate it through a new serving configuration. Never rebuild an active physical index in place.

Page if a supposedly optional vector failure causes required-channel failure, changes an eligibility decision, or prevents no-vector retrieval. Warn on sustained degradation, recall loss, or stale generation lag. A rollback moves the serving head to the prior generation and does not rewrite stored runs.
