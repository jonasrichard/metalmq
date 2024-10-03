use criterion::*;

fn basic_deliver(_c: &mut Criterion) {
    //c.iter(|| {});
}

criterion_group!(benches, basic_deliver);
criterion_main!(benches);
