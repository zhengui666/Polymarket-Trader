# Security Policy

## Scope

This repository contains trading infrastructure code. Treat any real exchange credentials, wallet material, operator tokens, and production databases as sensitive.

## Supported Usage

- Local development on loopback interfaces
- Simulation, replay, and test-oriented environments
- Explicitly approved execution environments with separately managed secrets

The default open source posture is conservative. Fresh checkouts should not be connected to production funds or publicly exposed control-plane endpoints.

## Reporting A Vulnerability

Do not open a public issue for credential exposure, authentication bypass, remote execution, unsafe execution enablement, or any bug that could impact live funds.

Report security issues privately to the repository maintainers. Include:

- affected files or components
- reproduction steps
- required configuration
- expected impact

If the issue involves accidentally committed secrets, rotate the secrets first, then report the exposure path and commit information.

## Operational Guidance

- Keep `POLYMARKET_OPS_BIND_ADDR` on a loopback address unless there is an explicit operational reason not to.
- The ops API refuses non-loopback binds unless `POLYMARKET_OPS_ALLOW_NON_LOOPBACK=true` is set intentionally.
- Never commit `.env`, credential files, or local SQLite databases.
- Use separate credentials for development, canary, and live environments.
