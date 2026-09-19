## What does this PR do?

<!-- One or two sentences. If it fixes an open issue, write "Fixes #123". -->

## Why?

<!-- The motivation — what problem exists today without this change. -->

## Type of change

- [ ] Bug fix
- [ ] New analyzer or framework support
- [ ] CLI / bindings change
- [ ] CI / packaging
- [ ] Documentation only
- [ ] Other (describe above)

## Testing

<!--
This project's tests are real and enforced in CI (see .github/workflows/ci.yml):
  cargo test -p pybeamguard-core       (Rust unit + integration tests)
  cargo fmt --all -- --check
  cargo clippy --workspace -- -D warnings
  maturin develop --release && pytest tests/   (Python bindings/CLI tests)

Describe what you actually ran locally, and paste real output/pass-fail —
do not just check boxes without running them.
-->

- [ ] `cargo test -p pybeamguard-core` passes
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace -- -D warnings` passes
- [ ] `pytest tests/` passes (if the Python bindings/CLI were touched)
- [ ] Added/updated tests covering the change (new analyzers need at least one fixture-based test)

## Notes for reviewers

<!-- Anything you're unsure about, or scope you deliberately left out. -->
