use bencher::Bencher;
use metalmq_client::Client;

fn basic_deliver(bench: &mut Bencher) {
    let cs = state::new();

    bench.iter(|| {});
}

bencher::benchmark_group!(delivery, basic_deliver);
bencher::benchmark_main!(delivery);
