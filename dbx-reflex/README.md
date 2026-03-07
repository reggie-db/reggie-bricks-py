# dbx-reflex

Custom components and event handling utilities for applications built with the Reflex framework.

## Overview

`dbx_reflex` extends the Reflex framework with additional components and state management helpers. It focuses on providing reusable UI patterns and smoother integration for common web application needs.

## Features

### Web Application (`app.py`)

The main entry point for the Reflex application:

* **Page Definitions**: Defines the index page and routing for the application.
* **Interactive UI**: Implements auto bound search and sort controls using Reflex state management.

### Components (`components.py`)

Advanced UI components:

* **IntersectionObserver**: A Reflex component wrapper for the browser Intersection Observer API, enabling scroll based triggers and lazy loading.

### State Management (`states.py`)

Enhanced state utilities:

* **Query Param Handling**: Helpers for automatically binding and synchronizing state variables with URL query parameters.

### Event Handling (`events.py`)

Streamlined event triggers:

* **Keyboard Events**: Simplified handling of specific key presses (e.g., Enter) for form submissions and interactive elements.

### Reflex Single-Port Runner (`run.py`)

Run Reflex directly in single-port mode so local behavior matches Databricks Apps
without an external proxy.

Key behavior:

* **Single-Port Mode**: Uses Reflex native single-port serving.
* **Databricks Port Alignment**: Reads `DATABRICKS_APP_PORT` or `--app-port`.
* **Single Reflex Process**: Runs one `reflex run` process directly.
* **Clean Shutdown**: On SIGINT/SIGTERM, terminates the Reflex process.
* **Reflex-like Invocation**: Supports forwarding args to `reflex run`.

## Usage

### Run Reflex

```bash
uv run --directory dbx-reflex dbx-reflex-run
```

Forward custom `reflex run` args:

```bash
uv run --directory dbx-reflex dbx-reflex-run -- --env dev --backend-only false
```

Set explicit app port:

```bash
uv run --directory dbx-reflex dbx-reflex-run --app-port 8000
```

### Query Parameter Binding

```python
import reflex as rx
from dbx_reflex import states

class SearchState(rx.State):
    @rx.var
    def q(self) -> str:
        return self.router.page.params.get("q", "")

    def set_q(self, value: str):
        return states.set_query_param(self, "q", value)
```

### Intersection Observer

```python
from dbx_reflex.components import IntersectionObserver

def index():
    return IntersectionObserver.create(
        rx.text("I am visible!"),
        on_intersect=lambda: print("Observed visibility change"),
    )
```

## Dependencies

* `reflex`

