#!/usr/bin/env bash
# Print the number of lines in each file under the given directory (default: backtester).
set -euo pipefail

dir="${1:-backtester}"
find "$dir" -type f -print0 | sort -z | xargs -0 wc -l
