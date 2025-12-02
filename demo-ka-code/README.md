# demo-ka-code

A Databricks asset bundle for generating a knowledge assistant for source code in notebooks and Python scripts. This tool exports workspace files and converts them to text format suitable for ingestion by knowledge assistant systems.

## Overview

`demo-ka-code` provides functionality to export and process Databricks workspace files (notebooks and Python scripts) for use with knowledge assistant systems. It recursively traverses workspace directories, exports files in source format, and converts them to plain text files that can be easily consumed by AI-powered knowledge assistants.

## Features

- **Recursive Workspace Export**: Export all notebooks and Python scripts from a specified workspace path
- **Source Format Conversion**: Convert exported files to plain text format for knowledge assistant ingestion
- **Flexible Filtering**: Filter exported files based on custom criteria using callable filters
- **Base64 Decoding**: Automatically handle base64-encoded export responses
- **Error Handling**: Gracefully skip files that cannot be exported (e.g., directories, unsupported formats)

## Installation

This project uses `uv` for dependency management. Install dependencies from the workspace:

```bash
uv sync
```

## Dependencies

- `reggie-tools` - Databricks workspace client utilities (workspace dependency)
- `reggie-core` - Core utilities for path management (transitive dependency)
- `databricks-sdk` - Databricks SDK for workspace API access (transitive dependency)

## Usage

### Basic Export

Export all files from a workspace path:

```python
from demo_ka_code import export

# Export all files from a workspace directory
for workspace_export in export("/Workspace/Users/username@databricks.com"):
    content = workspace_export.content_str()
    if content:
        print(f"Exported: {workspace_export.filename()}")
        print(content)
```

### Filtered Export

Export files matching specific criteria:

```python
from demo_ka_code import export

# Export only files containing "race" in their path
for workspace_export in export(
    "/Workspace/Users/username@databricks.com",
    filter=lambda obj: "race" in obj.path.lower()
):
    content = workspace_export.content_str()
    if content:
        print(f"Exported: {workspace_export.filename()}")
```

### Save Exports to Files

Export workspace files and save them as text files:

```python
import os
import time
from pathlib import Path
from demo_ka_code import export
from reggie_core import paths

# Create output directory
output_dir = paths.temp_dir() / "workspace_export" / str(int(time.time() * 1000))
output_dir.mkdir(parents=True, exist_ok=True)

# Export and save files
for workspace_export in export("/Workspace/Users/username@databricks.com"):
    content = workspace_export.content_str()
    if content:
        file_path = output_dir / f"{workspace_export.filename()}.txt"
        file_path.write_text(content)
        print(f"Saved: {file_path}")
```

## API Reference

### `export(path: str = "/", filter: Callable[[ObjectInfo], bool] | None = None) -> Iterator[WorkspaceExport]`

Exports workspace files from the specified path.

**Parameters:**
- `path` (str): The workspace path to export from. Defaults to `"/"` (root).
- `filter` (Callable[[ObjectInfo], bool] | None): Optional filter function to include/exclude files based on `ObjectInfo` properties. If provided, only files where the filter returns `True` will be exported.

**Returns:**
- `Iterator[WorkspaceExport]`: An iterator yielding `WorkspaceExport` objects for each successfully exported file.

**Example:**
```python
# Export all Python files
for export in export("/Workspace/Users/me@databricks.com", 
                     filter=lambda obj: obj.path.endswith(".py")):
    print(export.filename())
```

### `WorkspaceExport`

A dataclass representing an exported workspace file.

#### Attributes

- `info` (ObjectInfo): Databricks workspace object information
- `response` (ExportResponse): The export response containing file content

#### Methods

##### `content_str() -> str | None`

Decodes the base64-encoded export content to a UTF-8 string.

**Returns:**
- `str | None`: The decoded file content as a string, or `None` if decoding fails (e.g., binary files).

**Example:**
```python
export = next(export("/Workspace/Users/me@databricks.com"))
content = export.content_str()
if content:
    print(content)
```

##### `filename() -> str`

Extracts the filename from the workspace object path.

**Returns:**
- `str`: The basename of the workspace file path.

**Example:**
```python
export = next(export("/Workspace/Users/me@databricks.com"))
print(export.filename())  # e.g., "my_notebook"
```

## Configuration

The module uses `reggie-tools` for Databricks workspace client configuration. Set up your Databricks authentication using one of the following methods:

1. **Databricks CLI Profile**: Configure a profile using `databricks configure`
2. **Environment Variables**: Set `DATABRICKS_CONFIG_PROFILE` to specify the profile name
3. **Direct Configuration**: The workspace client will use default configuration resolution

**Example:**
```python
import os
os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
```

## Examples

### Export All Notebooks for Knowledge Assistant

```python
from demo_ka_code import export
from reggie_core import paths
import time

output_dir = paths.temp_dir() / "ka_export" / str(int(time.time() * 1000))
output_dir.mkdir(parents=True, exist_ok=True)

# Export all files from a workspace directory
for workspace_export in export("/Workspace/Users/username@databricks.com"):
    content = workspace_export.content_str()
    if content:
        # Save as .txt for knowledge assistant ingestion
        file_path = output_dir / f"{workspace_export.filename()}.txt"
        file_path.write_text(content)
        print(f"Exported: {file_path}")
```

### Filter by File Type

```python
from demo_ka_code import export

# Export only Python scripts
python_files = export(
    "/Workspace/Users/username@databricks.com",
    filter=lambda obj: obj.path.endswith(".py")
)

for export in python_files:
    content = export.content_str()
    if content:
        print(f"Python file: {export.filename()}")
```

### Filter by Path Pattern

```python
from demo_ka_code import export

# Export files from specific directories
filtered_exports = export(
    "/Workspace/Users/username@databricks.com",
    filter=lambda obj: "notebooks" in obj.path.lower() or "scripts" in obj.path.lower()
)

for export in filtered_exports:
    content = export.content_str()
    if content:
        print(f"Found: {export.filename()}")
```

## Error Handling

The export function gracefully handles errors:

- **BadRequest exceptions**: Files that cannot be exported (e.g., directories, unsupported formats) are silently skipped
- **UnicodeDecodeError**: Binary files or files with encoding issues return `None` from `content_str()`
- **Missing files**: Non-existent paths are handled by the Databricks SDK

## Integration with Knowledge Assistants

The exported text files can be used with various knowledge assistant systems:

1. **Databricks Knowledge Assistant**: Upload exported `.txt` files to a knowledge assistant space
2. **Vector Databases**: Process exported files for embedding and storage in vector databases
3. **LLM Context**: Use exported files as context for large language model queries
4. **Documentation Systems**: Generate documentation from exported source code

## Development

### Running Tests

```bash
# Run tests (when implemented)
pytest tests/
```

### Project Structure

```
demo-ka-code/
├── pyproject.toml          # Project configuration and dependencies
├── README.md               # This file
└── src/
    └── demo_ka_code/
        ├── __init__.py     # Package initialization
        └── workspace_service.py  # Main export functionality
```

## License

See the LICENSE file in the repository root.

## Contributing

This is a Databricks asset bundle project. When contributing:

1. Ensure all exports work correctly with the Databricks workspace API
2. Maintain compatibility with `reggie-tools` and `reggie-core` dependencies
3. Add appropriate error handling for edge cases
4. Document any new filtering or export options

