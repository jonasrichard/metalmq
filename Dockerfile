FROM rust:1-buster as builder

WORKDIR /src
COPY . .

#COPY certs.pem /etc/ssl/certs/

#RUN SSL_CERT_FILE=/etc/ssl/certs/certs.pem cargo build --release --bin metalmq
RUN cargo build --release --bin metalmq

FROM debian:buster

COPY --from=builder /src/target/release/metalmq /
COPY --from=builder /src/metalmq-docker.toml /metalmq.toml

CMD ["/metalmq"]
