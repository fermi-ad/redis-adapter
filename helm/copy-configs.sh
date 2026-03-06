#!/bin/bash
# Copy system-configs into the Helm chart so .Files.Glob can find them.
# Run this before 'helm install' or 'helm template'.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHART_DIR="$SCRIPT_DIR/redis-adapter"
CONFIGS_SRC="$SCRIPT_DIR/../adapters/system-configs"

if [ ! -d "$CONFIGS_SRC" ]; then
    echo "Error: adapters/system-configs/ not found. Run adapters/generate_system.py first."
    exit 1
fi

mkdir -p "$CHART_DIR/configs"

for type in bpm-twin bpm blm-twin blm bcm-twin bcm; do
    if [ -d "$CONFIGS_SRC/$type" ]; then
        cp -r "$CONFIGS_SRC/$type" "$CHART_DIR/configs/"
        echo "Copied $type configs"
    else
        echo "Warning: $CONFIGS_SRC/$type not found, skipping"
    fi
done

echo "Done. Configs are in $CHART_DIR/configs/"
