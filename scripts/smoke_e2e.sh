#!/usr/bin/env bash
set -euo pipefail
# Requires pip install -e '.[dev,stream]' and Docker Compose.
docker compose up -d --wait
kg-lab seed
kg-lab demo-publish --phase baseline
kg-lab demo-publish --phase changed
# Use a fresh consumer group for a deterministic replayable smoke test.
KAFKA_CONSUMER_GROUP="kg-smoke-$(date +%s)" kg-lab stream-consume --max-messages 5
kg-lab impact customer_events-value
RUN_INTEGRATION=1 pytest -q -m integration
