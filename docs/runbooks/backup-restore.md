# Backup and restore

Run `scripts/backup.sh` from a restricted operator host with SurrealDB authentication in `SURREAL_USER`/`SURREAL_PASS`, PostgreSQL authentication in standard `PG*` variables, a configured `mc` source alias, and an `age` recipient. The script exports SurrealDB, mirrors immutable canonical objects, dumps Temporal PostgreSQL, verifies every plaintext component, then writes only an encrypted bundle and checksum to the configured output directory.

Restore into newly created, empty SurrealDB, archive, and PostgreSQL targets. Set `MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION` to `empty:<namespace>:<database>:<mc-target>` exactly, run `scripts/restore.sh`, then run the public smoke test before routing traffic. The attestation is an operator safety interlock; infrastructure policy must ensure the targets are newly provisioned.

Schedule backups at least daily and retain one off-host copy. Perform a restore drill before launch and monthly thereafter. `mc mirror` captures current objects rather than version metadata; MemJev canonical object keys are immutable and conditional writes prohibit replacement.
