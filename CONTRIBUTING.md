# Contributing

## Development Baseline

1. Install the Rust toolchain declared in `rust-toolchain.toml`.
2. Copy `.env.example` to `.env`.
3. Keep `POLYMARKET_OPS_BIND_ADDR` on `127.0.0.1` unless you intentionally need a wider bind.
4. Prefer simulation or replay-oriented settings for local work.

## Before Opening A Pull Request

Run:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

If a test requires external credentials or exchange connectivity, document that clearly and keep it out of the default local path.

## Change Guidelines

- Do not commit credentials, tokens, wallet material, or local database files.
- Preserve conservative defaults for runtime safety.
- Keep documentation updated when adding environment variables, startup modes, or external integrations.
- Prefer making production-facing behavior opt-in rather than default-on.

## Security-Sensitive Changes

For changes involving authentication, execution, settlement, or secret loading:

- describe the new trust boundary
- document the rollback path
- state whether the change affects live-capital safety

If you discover a vulnerability, follow `SECURITY.md` instead of opening a public issue.
