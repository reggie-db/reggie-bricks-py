#!/bin/sh
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <script_path> [args...]"
  exit 1
fi

SCRIPT_PATH="$1"
shift
BUILD_SCRIPTS_PATH="build-scripts"

case "$SCRIPT_PATH" in
  *.py) ;; 
  *) SCRIPT_PATH="${SCRIPT_PATH}.py" ;;
esac

env -u VIRTUAL_ENV uv run --project "$BUILD_SCRIPTS_PATH" --script "$BUILD_SCRIPTS_PATH/$SCRIPT_PATH" "$@"