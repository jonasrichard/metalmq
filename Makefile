
.PHONY: test docker-metalmq

test:
	docker-compose up --abort-on-container-exit --exit-code-from amqp-test

docker-metalmq:
	docker build . -t metalmq:0.0.1
