# Repository Guidelines

## Project Structure and Module Organization

The repository is a uv workspace containing multiple Python projects. Each project is located in its own directory at the root and follows the src layout. Shared utilities live in `reggie-core`, while Databricks specific tools are in `dbx-tools`. Build artifacts and wheels are stored in the `dist` directory.

## Build, Test, and Development Commands

Use `uv sync --workspace` to install all dependencies. Individual projects can be run using `uv run --project <project-name>`. Tests are executed using `pytest` within each project directory or across the workspace.

## Coding Style and Naming Conventions

Follow standard Python style with four space indentation. Use type hints for function arguments and return values. Module names use snake_case while classes use PascalCase. Private functions and variables should be prefixed with an underscore. Place script descriptions after imports but before constants and classes.

## Testing Guidelines

Add tests under the `tests` directory within each project, mirroring the source structure. Use `pytest` for all test suites. Ensure that new functionality includes corresponding tests.

## Commit and Pull Request Guidelines

Write concise and imperative commit messages. Summarize changes in pull requests and note which modules are affected. Include logs or screenshots for UI changes.
