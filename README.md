# MetalMQ

[![Crates.io][crates-badge]][crates-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/metalmq.svg
[crates-url]: https://crates.io/crates/metalmq
[actions-badge]: https://github.com/jonasrichard/metalmq/workflows/CI/badge.svg
[actions-url]: https://github.com/jonasrichard/metalmq/actions?query=workflow%3ACI

## Build and run

`metalmq` is under development, it is not feature complete but you can try and run with cargo run.

```bash
cargo run --bin metalmq
## or to enable logs
RUST_LOG=debug cargo run --bin metalmq
RUST_LOG=metalmq=trace cargo run --bin metalmq
```

## Run tests

To run tests it is recommended to install [nextest](https://nexte.st) runner because of the exclusions of integration tests requires MetalMQ to run.

```bash
cargo nextest run -E 'not binary_id(metalmq::it) and not binary_id(metalmq-client::it)'
```

Integration tests also can be run with normal test runner by

```bash
cargo test --package metalmq --test it
cargo test --package metalmq-client --test it
```

To have coverage of the server tests run `llvm-cov`

```bash
cargo install cargo-llvn-cov
cargo llvm-cov nextest --package metalmq --bins
cargo llvm-cov report --html
open target/llvm-cov/html/index.html
```

## Run examples

There are some examples in the `examples` directory, they implement simple scenarios of the
`metalmq-client` library. To run execute

```bash
RUST_LOG=metalmq_client=trace cargo run --example publish-consume
```

## AMQP compliance

For AMQP compliance we use `pika` Python library and `pytest` framework to be to validate
the comformance of metalmq server.

```
cd amqp-compliance
pytest
```

### Compilance matrix

|Method            |Field                          |
|------------------|-------------------------------|
|connection.       |                               |
|channel.          |                               |
|exchange.declare  |:white_check_mark: exchange    |
|                  |:x: type                       |
|                  |:white_check_mark: passive     |
|                  |:x: durable (no persistence)   |
|                  |:question: arguments           |
|queue.declare     |:white_check_mark: queue       |
|                  |:white_check_mark: passive     |
|                  |:question: durable             |
|                  |:white_check_mark: exclusive   |
|                  |:white_check_mark: auto-delete |
|basic.ack         |:white_check_mark:             |

## AMQP 0.9 client library

In `metalmq-client` there is a Rust async client which implements part of the AMQP 0.9.1 protocol.
You can try agains `metalmq` server or `rabbitmq`.

```bash
#docker run -p 5672:5672 -p 15672:15672 --rm rabbitmq:3-management
cargo test --lib metalmq-client
```

In order to validate AMQP packages we also need a stable AMQP client implementation which is
the `pika`. It runs on Python, so one need to install `pipenv` to run that.

```
cd amqp-compliance
python3 -m venv ~/.venv
source ~/.venv/bin/activate

~/.venv/bin/pytest
```

### Use tokio console

In the examples the publish-consume test has dependency on the tokio console.

```
RUST_BACKTRACE=1 RUSTFLAGS="--cfg tokio_unstable" cargo +nightly run --bin metalmq --features tracing
RUSTFLAGS="--cfg tokio_unstable" cargo run --example publish-consume
```
