# Customer360 runbook

The `customer_360` dbt model reads `bronze.customer` and publishes `gold.customer_360`. The `Customer Executive Dashboard` is a downstream Power BI consumer. Before deprecating `customer_events`, the Customer Platform, Customer Intelligence, and BI Analytics teams must review the migration plan.

If schema validation failures increase after a producer release, first compare the active producer schema with `CustomerEventV4`, then inspect the CustomerStreaming consumer logs and the bronze ingestion lag.
