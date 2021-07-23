FROM rust:1-buster as builder

WORKDIR /src

COPY certs.pem /etc/ssl/certs/

# Speed up build, download and build deps if Cargo changes
COPY Cargo.* /src/
COPY metalmq/Cargo.* /src/metalmq/
RUN mkdir -p /src/metalmq/src/ && \
    echo "fn main() {}" > /src/metalmq/src/main.rs

COPY metalmq-codec/Cargo.* /src/metalmq-codec/
RUN mkdir -p /src/metalmq-codec/src && \
    echo "fn main() {}" > /src/metalmq-codec/src/lib.rs

COPY metalmq-client/Cargo.* /src/metalmq-client/
RUN mkdir -p /src/metalmq-client/src && \
    echo "fn main() {}" > /src/metalmq-client/src/lib.rs

COPY benches /src/benches/
COPY examples /src/examples/

RUN SSL_CERT_FILE=/etc/ssl/certs/certs.pem cargo fetch
RUN SSL_CERT_FILE=/etc/ssl/certs/certs.pem cargo build --release --bin metalmq && \
    rm -rf /src/src/main.rs target/release/metalmq
# End of speed up section

COPY . .

RUN SSL_CERT_FILE=/etc/ssl/certs/certs.pem cargo build --release --bin metalmq
#RUN cargo build --release --bin metalmq

FROM debian:buster

COPY --from=builder /src/target/release/metalmq /
COPY --from=builder /src/metalmq-docker.toml /metalmq.toml

CMD ["/metalmq"]
