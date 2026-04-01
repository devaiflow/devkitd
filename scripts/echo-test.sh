#!/usr/bin/env bash
set -euo pipefail

# echo-test.sh — Simple test script for devkitd integration testing
# Usage: echo-test.sh --id <id> [--message <msg>]
#
# Echoes arguments and exits with code 0.
# Useful for verifying script execution, argument mapping, and output capture.

ID="unknown"
MSG="Hello from devkitd"

while [[ $# -gt 0 ]]; do
  case "$1" in
  --id)
    ID="$2"
    shift 2
    ;;
  --message)
    MSG="$2"
    shift 2
    ;;
  *)
    shift
    ;;
  esac
done

echo "echo-test: id=$ID, message=$MSG"
exit 0
