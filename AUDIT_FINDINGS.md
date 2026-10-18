# Audit findings — 2026-10-19

This file records the repository-specific findings from the portfolio audit
conducted on 2026-10-19. It is documentation only; no code or release artifacts
were changed in this pass unless explicitly noted under "Implementation status".

Sources: the repository's own README/source/workflows, GitHub metadata and
Actions runs, PyPI metadata, and official competitor documentation. Claims are
marked as verified, inferred, or open questions. "Absent" means not found in the
inspected repository/docs, not proof that it cannot exist.

## What it is

Static pre-deployment analysis for Apache Beam, Flink, and Spark pipelines: hot keys, shuffles, windowing/state, cost, reliability, and best practices.

## Verified strengths

- 10 Beam + 6 Flink/Spark analyzers; YAML rule engine; fail-on severity; data-profile estimates; JSON; PyPI + Rust binary.
- Useful multi-engine static heuristics.

## Verified gaps and user-facing risks

- Native analyzers are regex/line-based; `--ast` adds a real Python AST inventory and unbounded-source check, but the native analyzers are still regex-based.
- Cost figures are unvalidated heuristics; no Kafka/Ray support; macOS arm64 wheel only; no real pipeline corpus.
- No committed accuracy/cost validation.

## Direct competitors

- Beam/Dataflow native tooling, Flink/Spark, dbt tests.

## Ranked next actions

- P1: directory scan + Python AST parsing (M).
- P2: validate cost model against real Dataflow bills (M).

## Do not build

- Kafka Streams/Ray support; no demand evidence.

## Evidence gaps / validation needed

- No false-positive analysis; no benchmark against real jobs.

## Recommended disposition

Maintain; directory scan and AST parsing are the next credibility steps.

## Implementation status

No code changes in this pass.
