.PHONY: install install-llm neo4j-up neo4j-down seed demo api test lint

install:
	python -m pip install -e '.[dev]'

install-llm:
	python -m pip install -e '.[dev,llm,graphrag]'

neo4j-up:
	docker compose up -d

neo4j-down:
	docker compose down

seed:
	kg-lab seed

demo:
	kg-lab ask "What systems depend on customer_events and why?"

api:
	uvicorn kg_lab.api:app --reload --port 8000

test:
	pytest -q

lint:
	ruff check src tests

.PHONY: infra-up infra-down stream-install demo-baseline demo-changed stream-consume impact integration registry-watch

infra-up:
	docker compose up -d --wait

infra-down:
	docker compose down

stream-install:
	python -m pip install -e '.[dev,stream]'

demo-baseline:
	kg-lab demo-publish --phase baseline

demo-changed:
	kg-lab demo-publish --phase changed

stream-consume:
	kg-lab stream-consume --max-messages 5

impact:
	kg-lab impact customer_events-value

registry-watch:
	kg-lab registry-watch --once

integration:
	RUN_INTEGRATION=1 pytest -q -m integration
