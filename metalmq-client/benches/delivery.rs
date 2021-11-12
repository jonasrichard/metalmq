use bencher::Bencher;

fn basic_deliver(bench: &mut Bencher) {
    bench.iter(|| {});
}

bencher::benchmark_group!(delivery, basic_deliver);
bencher::benchmark_main!(delivery);
