# ADR-001: Customer event streaming

The Customer360 platform uses `customer_events` as its primary event stream. Kafka was selected to decouple web and mobile producers from downstream analytical consumers, support replay, and preserve ordered events within customer partitions.

The `CustomerStreaming` job validates `CustomerEventV4` records and writes accepted events to `bronze.customer`. The schema subject must remain BACKWARD compatible because downstream consumers can lag during rolling deployments.
