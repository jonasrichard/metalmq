FROM rust:1-buster as builder

WORKDIR /src
COPY . .

RUN cargo build --release --bin metalmq

FROM debian:buster

COPY --from=builder /src/target/release/metalmq /

CMD ["/metalmq"]
