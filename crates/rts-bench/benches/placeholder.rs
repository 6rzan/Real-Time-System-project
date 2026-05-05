//! Placeholder bench so `cargo bench -p rts-bench` succeeds during P0.
//! Real harnesses (`sync_shootout`, `tail_latency`) land in P9 and P10.

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_noop(c: &mut Criterion) {
    c.bench_function("noop", |b| b.iter(|| 1u64.wrapping_add(1)));
}

criterion_group!(benches, bench_noop);
criterion_main!(benches);
