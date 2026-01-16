# demo-iot

An IoT demonstration project featuring FastAPI, Kafka integration, and AI driven data exploration.

## Overview

`demo-iot` serves as a reference implementation of a full stack IoT application. It combines real time data streaming via Kafka with a FastAPI backend and uses Databricks Genie for natural language data querying.

## Features

### FastAPI Backend (`app.py`, `apis.py`)

Modern web API for IoT data:

* **Generated Router**: Uses code generation to ensure API contract consistency.
* **SSE Streaming**: Server Sent Events (SSE) endpoint for real time Kafka record streaming.
* **AI Chat**: Integration with Databricks Genie for conversational data analysis.

### Kafka Integration (`kafka_consumer.py`)

Reliable real time data consumption:

* **Lazy Consumer**: Efficient Kafka consumer initialization.
* **Offset Management**: Utilities for querying and seeking to specific topic offsets.

### AI Driven Exploration

* **Genie Integration**: Translates natural language queries into SQL using Databricks Genie.
* **Query Caching**: Disk based caching of Genie responses to improve performance.

## Usage

To run the IoT demo application:

```bash
uv run --project demo-iot python -m demo_iot.app
```

Note: Requires access to a Kafka cluster and Databricks workspace configuration.

## Dependencies

* `dbx-tools` (workspace dependency)
* `dbx-concurio` (workspace dependency)
* `uvicorn`
* `fastapi`
* `kafka-python`
* `humanize`
* `aioreactive`
* `websockets`

