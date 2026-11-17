#!/usr/bin/env bash
# Re-render the architecture diagram from its Graphviz source.
# Requires Graphviz (e.g. `sudo apt-get install -y graphviz`).
set -euo pipefail
cd "$(dirname "$0")"
dot -Tpng -Gdpi=110 architecture.dot -o architecture.png
dot -Tsvg architecture.dot -o architecture.svg
