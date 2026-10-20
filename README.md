# microservice-dispatcher

The dispatcher: claims queued runs and publishes and creates one Kubernetes Job
for each.

This repository is one component of
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet). What it is
and how it fits with the others is stated by the documentation site, which
lives in the superrepo at `apps/docs/src/content/docs/`. Its Repositories page,
`apps/docs/src/content/docs/development/repositories.md`, draws the edges
between the repositories.

## The service

`crates/dispatcher` is `tcab-dispatcher`, the thin, long-running controller
that drains the backend's two queues: it claims each queued run and creates one
Kubernetes `Job` running the driver image for it, and claims each queued
publish and creates one `Job` running the publisher image. It keeps the claim
lease of every live run's `Job` renewed and reports a driver pod's death that
the driver could never report itself. It holds no state of its own, binds no
socket and takes no flags.

What the dispatcher must do is stated by the superrepo's
`apps/docs/src/content/docs/components/dispatcher/overview.md`. Its
configuration is entirely environment variables, documented in
`crates/dispatcher/src/config.rs` and listed with placeholder values in
`.env.dispatcher.example`. To run it on its own against the cluster your
Kubernetes client configuration names, copy that file to `.env.dispatcher`,
fill it in and run:

```sh
set -a; . ./.env.dispatcher; set +a
cargo run --bin tcab-dispatcher
```

The manifests that deploy it (its `Deployment`, the RBAC that lets it create
`Job`s, the environment patch naming the driver and publisher images) and the
deploy sets pinning its image live in the superrepo, under
`deployments/k8s/`. A deployment pins this repository's image at the commit the
superrepo's submodule pointer names.

## Getting started

Open the superrepo in its devcontainer, which carries every toolchain the
repositories need, and work from this repository's root.
A build here inherits the superrepo's Cargo configuration, which redirects
every dependency on another repository to the sibling checkout, so nothing is
fetched from a mirror.

One command runs every gate:

```sh
make gate
```

Each gate is a file under `ci/gates/` whose stem is the gate's id.
`uv run --quiet --project ci gate list` names them all, and
`azure-pipelines.yml` runs each one as a step of its own on every push, inside
the CI images the superrepo builds.

## The image

The `Dockerfile` at the root is the repository's image, built from the root
with `.dockerignore` as its allowlist. On every gated push to `master` and
`staging` the pipeline builds it natively for amd64 and arm64
(`scripts/ci/image.sh`) and fuses the two into the multi-arch
`testcabinet.azurecr.io/microservice-dispatcher:<commit>` a deployment pins
(`scripts/ci/manifest.sh`).
The `build-context` gate holds every `COPY` to the allowlist and the Rust pin
to `rust-toolchain.toml`. The kit wrote the `Dockerfile` once, and it is the
repository's to grow.

`cargo build-portable-dispatcher` builds a fully static `tcab-dispatcher`
through the musl target, for a host without the generic loader;
`.cargo/config.toml` lists what it needs.
