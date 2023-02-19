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
cargo test --tests

## or to run integration tests
cargo test --package metalmq --test it
```

There are some examples in the `examples` directory, they implement simple scenarios of the
`metalmq-client` library. To run execute

```bash
RUST_LOG=metalmq_client=trace cargo run --example publish-consume
```

Running tests in `metalmq-client`

```bash
cargo test --package metalmq-client --test it
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
pytest
```

### Unit test coverage

Run unit test with coverage.

```
rustup component add llvm-tools-preview
cargo install grconv

mkdir -p target/coverage/data
make metalmq-cover
open target/coverage/html/index.html
```

### Use tokio console

In the examples the publish-consume test has dependency on the tokio console.

```
RUSTFLAGS="--cfg tokio_unstable" cargo run --example publish-consume
```
