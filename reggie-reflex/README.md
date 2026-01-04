# reggie-reflex

Custom components and event handling utilities for applications built with the Reflex framework.

## Overview

`reggie_reflex` extends the Reflex framework with additional components and state management helpers. It focuses on providing reusable UI patterns and smoother integration for common web application needs.

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

## Usage

### Query Parameter Binding

```python
import reflex as rx
from reggie_reflex import states

class SearchState(rx.State):
    @rx.var
    def q(self) -> str:
        return self.router.page.params.get("q", "")

    def set_q(self, value: str):
        return states.set_query_param(self, "q", value)
```

### Intersection Observer

```python
from reggie_reflex.components import IntersectionObserver

def index():
    return IntersectionObserver.create(
        rx.text("I am visible!"),
        on_intersect=lambda: print("Observed visibility change"),
    )
```

## Dependencies

* `reflex`

