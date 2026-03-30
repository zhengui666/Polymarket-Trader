# Release Notes

## v0.1.0

Initial open source release of the Polymarket Trader workspace.

### Included

- Rust workspace covering node runtime, ops API, config, storage, rules, opportunity discovery, execution, settlement, and audit layers
- AGPLv3 licensing metadata aligned with the repository license file
- local development bootstrap via `.env.example`
- repository-level documentation for setup, contribution flow, and security reporting
- safer default posture for the ops API, including non-loopback bind protection unless explicitly overridden

### Validation

- `cargo check --workspace`
- `cargo test --workspace`

### Operational Notes

- Treat all real credentials and wallet material as external secrets
- Keep ops API access on loopback unless a stricter deployment boundary is intentionally configured
- Start from simulation or replay-oriented modes before enabling any execution path
