# Polymarket Trader

<p align="center">
  <strong>A Rust-first, operations-aware trading system for building, simulating, and operating Polymarket strategies.</strong>
</p>

<p align="center">
  Built for disciplined automation: safe local defaults, explicit runtime modes, auditable services, and an operator-facing control plane.
</p>

## Why This Project Exists

Most trading repos stop at “run a bot.” Real systems need more than that:

- a runtime that can be started, observed, and shut down cleanly
- explicit simulation and canary modes before anything production-adjacent
- durable storage, rules, opportunities, and audit trails as first-class modules
- an operations API for rollouts, alerts, visibility, and intervention

Polymarket Trader is structured as an actual system, not a single script. The repository is a Rust workspace that separates core trading logic, rules, opportunities, storage, audit, configuration, services, and message bus concerns into dedicated crates, with runnable apps for the trading node and the ops API.

## Highlights

- Rust workspace with clear boundaries across `core`, `services`, `storage`, `rules`, `audit`, `config`, and `msgbus`
- Safe-by-default local setup with `.env.example`, `Sim` runtime mode, and conservative development guidance
- Operator-facing HTTP control plane for rollout management, alerts, execution workflows, rules, and opportunity visibility
- Graceful startup and shutdown behavior in both main binaries
- Security-conscious contribution posture with private vulnerability reporting and strong secret-handling expectations
- AGPL-3.0 licensed from day one

## What Is In The Repo

### Runnable apps

- `polymarket-node`: starts the trading runtime, loads environment-driven config, initializes services, and runs until an explicit shutdown signal
- `polymarket-ops-api`: starts the operations API, initializes auth, storage, tracing, replay and monitoring workers, and exposes control endpoints

### Workspace crates

- `polymarket-core`: core trading logic
- `polymarket-services`: service layer
- `polymarket-storage`: storage management
- `polymarket-rules`: business rules engine
- `polymarket-opportunity`: opportunity handling
- `polymarket-audit`: audit functionality
- `polymarket-config`: configuration management
- `polymarket-msgbus`: message bus communication
- `polymarket-api-types`: API type definitions
- `polymarket-common`: shared utilities

## Architecture

```text
                     +----------------------+
                     | polymarket-ops-api   |
                     | alerts / rollout     |
                     | monitoring / control |
                     +----------+-----------+
                                |
                                v
+-------------------+    +------+-------+    +-------------------+
| config / common   |--->| services     |<---| rules / audit     |
+-------------------+    +------+-------+    +-------------------+
                                |
                                v
                     +----------+-----------+
                     | polymarket-core      |
                     | runtime / execution  |
                     +----------+-----------+
                                |
               +----------------+----------------+
               v                                 v
      +--------+--------+               +--------+--------+
      | opportunity     |               | storage /       |
      | discovery       |               | msgbus / api    |
      +-----------------+               +-----------------+
                                |
                                v
                     +----------+-----------+
                     | polymarket-node      |
                     | sim / canary runtime |
                     +----------------------+
```

The split is intentional: keep trading decisions, system operations, persistence, and auditability separable so the project can evolve like a serious open source system instead of collapsing into a monolith.

## Quick Start

### Requirements

- Rust 2021 toolchain
- Cargo
- A copied environment file: `.env.example` -> `.env`

### 1. Bootstrap

```bash
cp .env.example .env
cargo fmt --all
cargo check
```

### 2. Run the trading node

```bash
cargo run -p polymarket-node
```

By default, the example configuration points at:

- `POLYMARKET_ENV=dev`
- `POLYMARKET_NODE_RUNTIME_MODE=Sim`
- `POLYMARKET_NODE_DOMAIN=canary`
- `POLYMARKET_DATA_DIR=./var`

### 3. Run the operations API

```bash
cargo run -p polymarket-ops-api
```

Default bind target from the example environment:

- `POLYMARKET_OPS_BIND_ADDR=127.0.0.1:8080`

## Development Workflow

Before opening a PR:

```bash
cargo fmt --all
cargo check
cargo test
```

Project expectations are straightforward:

- never commit credentials or production secrets
- keep defaults conservative and local-first
- update docs when behavior changes
- document trust boundaries, rollback paths, and capital-safety impact for security-sensitive changes

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contributor workflow and [SECURITY.md](./SECURITY.md) for responsible disclosure.

## Configuration

The included `.env.example` exposes a clear separation of concerns:

- runtime settings: environment, data directory, log filter, heartbeat interval
- node settings: domain selection and runtime mode
- ops API settings: bind address, visible domains, target domain
- authentication: token-based access control for the operations surface

This makes the repo usable for local development and simulation without pretending deployment details are trivial.

## Project Status

Current release posture:

- `v0.1.0` initial open source release
- full Rust workspace with apps and subsystem crates
- local bootstrap and development conventions already present
- suitable today for development, simulation, architecture review, and extension work

If you are looking for a “clone and print money” bot, this is the wrong repository. If you want a composable base for a serious Polymarket trading system, this is the right shape of project.

## Roadmap

- Expand strategy examples and documented execution flows
- Add deployment recipes and environment-specific operating guides
- Harden observability, replay, and audit workflows
- Grow test coverage around rules, rollout policy, and failure handling
- Publish more protocol and dataflow documentation

## Why The README Looks Like This

Great open source READMEs usually do a few things very well:

- they explain the project in one sentence without jargon
- they show the shape of the system before diving into implementation
- they make the first successful run obvious
- they communicate project status and contribution expectations honestly

This README follows that pattern on purpose.

## Contributing

Issues and pull requests are welcome. The best contributions for an early-stage systems repo are:

- documentation that removes ambiguity
- tests that lock in expected behavior
- operational hardening
- improvements to rules, opportunity evaluation, storage, and auditability

Start with [CONTRIBUTING.md](./CONTRIBUTING.md).

## Security

Please do not disclose vulnerabilities publicly. Use the process in [SECURITY.md](./SECURITY.md), and rotate any exposed secrets immediately if credentials were involved.

## License

Licensed under [AGPL-3.0-only](./LICENSE).
