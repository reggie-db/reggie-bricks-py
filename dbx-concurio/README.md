# dbx-concurio

Disk backed caching utilities and executable installation helpers with interprocess
locking support. This module provides thread safe caching and resource management
for concurrent execution scenarios.

## Overview

`dbx-concurio` provides disk backed caching built on `diskcache` with interprocess
locking via `fasteners`. It also includes utilities for downloading and installing
executables with caching support. The module is designed for scenarios where multiple
processes need to coordinate access to shared cached resources.

## Features

### Disk Caching (`caches.py`)

Thread safe disk backed cache with expiration and locking:

* **DiskCache**: Extended `diskcache.Cache` with additional features:
    * **On demand loading**: `get_or_load()` method for compute or load semantics
    * **Expiration support**: Optional time based cache expiration
    * **Interprocess locking**: Per key file locks to prevent concurrent writes
    * **Automatic directory management**: Smart cache directory resolution with
      fallbacks

* **DiskCacheValue**: Typed wrapper for cached values with load timestamps

Key features:

* Fast path reads avoid locking when cache is valid
* Process wide locks prevent concurrent loading of the same key
* Automatic staleness detection based on expiration time
* Safe directory resolution with temp fallback

### Executable Installation (`execs.py`)

Cached executable download and installation:

* **executable()**: Download and cache executables from URLs or local paths
* **InstallPath**: Extended `Path` class with completion callbacks for cleanup
* **Automatic caching**: Executables are cached by identifier hash to avoid
  redundant downloads
* **Permission handling**: Automatically sets executable permissions on downloaded
  files

## Usage

### Caching

```python
from dbx_concurio import caches
from datetime import timedelta

# Create a cache with automatic directory resolution
cache = caches.DiskCache(name="my_cache")

# Get or load a value with expiration
result = cache.get_or_load(
    key="expensive_computation",
    loader=lambda: expensive_function(),
    expire=3600  # 1 hour expiration
)

print(f"Value: {result.value}")
print(f"Loaded at: {result.load_timestamp}")
```

### Executable Installation

```python
from dbx_concurio import execs

# Download and cache an executable from URL
executable_path = execs.executable(
    source="https://example.com/tool",
    identifier="my-tool-v1.0"
)

# Use the executable
subprocess.run([str(executable_path), "--help"])

# Cleanup (if on_complete callback was provided)
executable_path.complete()
```

## Dependencies

* `reggie-core` (workspace dependency)
* `diskcache`
* `fasteners`

## Cache Directory Resolution

The cache directory is resolved in the following order:

1. If a file path is provided, uses temp directory + hash of file path
2. If a directory is provided, uses that directory
3. If `None`, uses temp directory + module name
4. Appends optional `name` parameter to the resolved path

All directories are created automatically if they don't exist.

## Thread Safety

* **Read operations**: Lock free when cache is valid (fast path)
* **Write operations**: Protected by per key file locks using `fasteners.InterProcessLock`
* **Concurrent access**: Multiple processes can safely access the same cache
* **Lock files**: Stored in `.key-locks` subdirectory of cache directory

## Use Cases

* Caching expensive computations across process restarts
* Sharing cached data between multiple processes
* Downloading and caching external tools or executables
* Coordinating resource access in distributed environments

