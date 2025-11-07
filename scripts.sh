#!/bin/sh
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <script> [args...]"
  exit 1
fi

CMD="$1"
shift

case "$CMD" in
  *.py) SCRIPT="${CMD%.py}" ;;
  *) SCRIPT="$CMD" ;;
esac

uv run --package scripts -m "scripts.${SCRIPT}" "$@"