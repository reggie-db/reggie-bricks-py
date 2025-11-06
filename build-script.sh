#!/bin/sh
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <script|command> [args...]"
  exit 1
fi

BUILD_SCRIPTS_PATH="build-scripts"

CMD="$1"
shift

case "$CMD" in
  *.py) SCRIPT="$CMD" ;;
  *) SCRIPT="$CMD.py" ;;
esac

if [ -f "$BUILD_SCRIPTS_PATH/$SCRIPT" ]; then
  set -- run --script "$BUILD_SCRIPTS_PATH/$SCRIPT" "$@"
else
  set -- "$CMD" "$@"
fi

env -u VIRTUAL_ENV uv --project "$BUILD_SCRIPTS_PATH" "$@"