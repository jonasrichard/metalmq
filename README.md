# MetalMQ

[![Crates.io][crates-badge]][crates-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/metalmq.svg
[crates-url]: https://crates.io/crates/metalmq
[actions-badge]: https://github.com/jonasrichard/metalmq/workflows/CI/badge.svg
[actions-url]: https://github.com/jonasrichard/metalmq/actions?query=workflow%3ACI

## Project status

I started this project to learn Rust async programming. I wanted something
fairly big because I didn't want to think about the requirement in order to grow
the project.

I downloaded the AMQP 0.9.1 spec and started to read and understand how the
protocol work, but it is quite hard since the spec is quite vague. I looked for
details in the internet to see how the wire protocol looked like, since I wanted
to implement everything from scratch. I found the RabbitMQ website quite useful.

I also needed to have a stable client implementation with which I can write the
acceptance tests. I saw that the Pika Python library is the thing I need, so I
started to crack on Python coding, too.

The more I went to details of the behaviour of the protocol, the less confident
I was if I want to finish this project fully. In memory most of the features
work, but when I saw inconsistencies in the protocol, like if an immediate
message are router to different queues but one of the queues doesn't have
consumer, but the other have, is it a success or not? If not, when the client
resend the message it duplicated the message to other consumers who got the
first instance.

So when I started to see that, I decided not to implement the file backend part.
Also I read that this protocol really invented by JP Morgan, and standardized by
a committee, where later IBM wanted to kill the initiative since it would have
killed their own message broker product.

I was thinking if I want to include `serde` in the metalmq codec part, probably
it would have been a good idea. Also about async programming I learned a lot. I
disappointed me that async functions aka futures really need to own their local
variables, killing the best features or Rust like references, mutability, etc.
Like if you want to pass a value to an async block, you need to renounce the
ownership (move semantics) or you need to clone it (your code will be full of
clones and unneeded memory allocation), or you need to wrap you big immutable
data with `Arc`, so again full of Arcs.

I know that compiler team constantly works on these issues, and there will be
changes in the future. Also I chose the CSP (Communicating Sequential Processes)
so there are a couple of places where messages or references of the messages are
going around. One might think that since there won't be that much threads in a
message broker, that multi-threaded model is also appropriate. Maybe yes, but
they would be another project since the goal was to understand the async in
Rust.

So I regard this project abandoned because of the inconsistencies of the
protocol and not these drawbacks I listed before.

## Build and run

`metalmq` is under development, it is not feature complete but you can try and
run with cargo run.

```bash
cargo run --bin metalmq
## or to enable logs
RUST_LOG=debug cargo run --bin metalmq
RUST_LOG=metalmq=trace cargo run --bin metalmq
```

## Run tests

To run tests it is recommended to install [nextest](https://nexte.st) runner
because of the exclusions of integration tests requires MetalMQ to run.

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

There are some examples in the `examples` directory, they implement simple
scenarios of the `metalmq-client` library. To run execute

```bash
RUST_LOG=metalmq_client=trace cargo run --example publish-consume
```

## AMQP compliance

For AMQP compliance we use `pika` Python library and `pytest` framework to be to
validate the comformance of metalmq server.

```
cd amqp-compliance
pytest
```

### Compilance matrix

| Method           | Field                          |
| ---------------- | ------------------------------ |
| connection.      |                                |
| channel.         |                                |
| exchange.declare | :white_check_mark: exchange    |
|                  | :x: type                       |
|                  | :white_check_mark: passive     |
|                  | :x: durable (no persistence)   |
|                  | :question: arguments           |
| queue.declare    | :white_check_mark: queue       |
|                  | :white_check_mark: passive     |
|                  | :question: durable             |
|                  | :white_check_mark: exclusive   |
|                  | :white_check_mark: auto-delete |
| basic.ack        | :white_check_mark:             |

## AMQP 0.9 client library

In `metalmq-client` there is a Rust async client which implements part of the
AMQP 0.9.1 protocol. You can try agains `metalmq` server or `rabbitmq`.

```bash
#docker run -p 5672:5672 -p 15672:15672 --rm rabbitmq:3-management
cargo test --lib metalmq-client
```

In order to validate AMQP packages we also need a stable AMQP client
implementation which is the `pika`. It runs on Python, so one need to install
`pipenv` to run that.

```
cd amqp-compliance
python3 -m venv ~/.venv
source ~/.venv/bin/activate

~/.venv/bin/pytest
```

### Use tokio console

In the examples the publish-consume test has dependency on the tokio console.

```bash
RUST_BACKTRACE=1 RUSTFLAGS="--cfg tokio_unstable" cargo +nightly run --bin metalmq --features tracing
RUSTFLAGS="--cfg tokio_unstable" cargo run --example publish-consume
```
