# Vector degradation

Vector retrieval is an approximate candidate generator, never a source of final ordering. Every returned ANN pool is reranked with the pinned quantized vectors and integer distance implementation before fusion.

## Runtime configuration and activation

Vector retrieval is enabled only when all three settings are present:

- `MEMJEV_RETRIEVAL_VECTOR_CONFIG_FILE` points to a read-only `retrieval-vector-config.v1` document such as `configs/vector.example.json`.
- `MEMJEV_EMBEDDING_PROVIDER_ENDPOINT` is a pinned HTTPS embedding-gateway endpoint. Plain HTTP is accepted only in the development environment.
- `MEMJEV_EMBEDDING_PROVIDER_TOKEN_FILE` points to a read-only, single-line bearer token. The token is never accepted inside the JSON configuration or logged.

Startup fails on a partial configuration, unknown JSON fields, aliases such as `latest`, invalid tuning, an oversized file, a missing token, or an unsafe endpoint. The process derives the embedding manifest ID, physical table name, DiskANN index name, and index manifest ID from canonical configuration; operators do not provide SQL identifiers.

Installing the channel does not activate it for every tenant. A tenant executes vector retrieval only when its immutable serving configuration contains the exact derived vector index manifest. Tenants whose serving snapshots contain only exact, lexical, facet, and graph indexes continue on those four channels. Before activation, provision the generation, finish the tenant's embedding backfill through the captured projection epoch, measure ANN recall against the exact-distance oracle, publish the vector retrieval-index manifest, and atomically move the tenant serving head. Never point a serving head at a partially populated generation.

The provider contract is a POST JSON request containing `provider`, `model`, `model_revision`, and canonical input JSON strings in `inputs`. The response must echo the same pinned identity and return exactly one vector per input. Redirects, unknown response fields, trailing content, identity substitution, invalid vectors, and responses larger than 64 MiB fail the vector channel closed.

If vector latency, malformed output, model revision mismatch, or index health breaches its deadline, record the channel degradation and continue only when all required deterministic channels completed. Confirm responses show `approximate_candidates=false` when the vector channel did not complete.

Quarantine a generation when its embedding manifest, dimension, distance function, normalization, table name, index name, or content hash differs from the activated manifest. Build a new immutable generation, measure recall against an exact-distance sample, and activate it through a new serving configuration. Never rebuild an active physical index in place.

Page if a supposedly optional vector failure causes required-channel failure, changes an eligibility decision, or prevents no-vector retrieval. Warn on sustained degradation, recall loss, or stale generation lag. A rollback moves the serving head to the prior generation and does not rewrite stored runs.
