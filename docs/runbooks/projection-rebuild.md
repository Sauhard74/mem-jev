# Projection rebuild

SurrealDB procedure records are rebuildable projections. The canonical encrypted trace archive, immutable outcome evidence, pinned tool-contract version content, policy version, and synthesis manifest are the sources of truth.

## Preconditions

- Freeze publication for the tenant or rebuild into an isolated namespace/database.
- Record the current `projection_checkpoint`, manifest count, version count, and SHA-256 of every `procedure_version.canonical_projection`.
- Verify archive metadata, encryption mode, content hash, and canonical schema before replay.
- Use exactly the sanitizer, registry snapshot, evidence policy, graph builder, and synthesizer versions recorded in each manifest. A current default is not a substitute for a recorded version.

## Rebuild and compare

Replay manifests in stable `(tenant_id, created_at, synthesis_manifest_id)` order through the same deterministic projection builder. Write into the isolated target, then compare:

1. manifest IDs and content hashes;
2. procedure family and version IDs;
3. byte-for-byte `canonical_projection` values;
4. step and typed-edge IDs/content hashes;
5. negative-path IDs and all three compatibility-scope hashes;
6. evidence links and the final checkpoint manifest hash.

Promote the rebuilt projection only when every comparison is exact. The rebuild job must pass the stored and rebuilt bytes through `rebuild.CompareProjection`; it records `memjev.projection.rebuild_mismatches{component}` and fails closed on any difference. Quarantine the affected version, retain both datasets, and page the owner. Never normalize, reserialize, or "repair" bytes during comparison. Deploy the validated rules in `ops/alerts/evidence-pipeline.yaml` through the platform's Prometheus-compatible rule manager.

## Rollback

Serving reads switch by projection generation/checkpoint, not by mutating immutable versions. If post-cutover validation fails, restore the previous checkpoint/generation and keep the failed rebuild isolated for investigation.
