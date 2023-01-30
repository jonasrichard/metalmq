
.PHONY: docker-metalmq metalmq-cover test

# TODO compile it with -C target-cpu=native and with opt level
# TODO cross compliation
# TODO docker containers

test:
	docker-compose up --abort-on-container-exit --exit-code-from amqp-test

docker-metalmq:
	docker build . -t metalmq:0.0.1

metalmq-cover:
	rm -f target/coverage/data/*
	CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='target/coverage/data/cargo-test-%p-%m.profraw' cargo test --bin metalmq
	mkdir -p target/coverage/html
	grcov . --binary-path ./target/debug/deps -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore '/*' -o target/coverage/html
