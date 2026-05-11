# `nico-doctor` perf benches

PRD-005 lands four criterion benches here in subsequent slices. This
file is a placeholder so the directory ships in Slice 0a.1 alongside
the fixture and dev-dep scaffolding. Slice 0a.2 fills in the bench
harness and pastes baseline numbers below.

## Running

```bash
# Wall-clock benches (system allocator)
cargo bench -p nico-doctor

# Heap-profiling pass (opt-in dhat allocator — slower, captures alloc count)
cargo bench -p nico-doctor --features dhat-heap
```

## Fixtures

Benches consume seed rows from `crates/nico-doctor/tests/fixtures/perf/`
via `nico_doctor::perf_fixtures::synthesize_*(n)`. Seeds are KB-scale
and checked in; the synthesizer multiplies them by N (1, 18, 250,
1000, 10000) at bench startup so no large fixtures live in git. To
refresh seeds against a live cluster:

```bash
./scripts/capture-fixtures.sh
```

## Baseline (TBD)

Slice 0a.2 will populate the table below from a single local run on
the maintainer's box. Treat the numbers as a regression guard, not
a tight budget.

| Bench | N | Wall-clock (median) | Allocs (dhat) |
| --- | --- | --- | --- |
| _placeholder_ | — | — | — |
