#!/usr/bin/env bash
# Creates the initial GitHub Release with all current data CSVs.
# Run this ONCE after installing gh CLI and pushing the .gitignore changes.
#
# Prerequisites:
#   brew install gh
#   gh auth login
#
set -euo pipefail

DATA_TAG="latest-data"
DATA_DIR="data"

echo "=== Creating initial data release ==="

# Package all CSVs into a tar.gz
echo "Packaging data..."
tar czf /tmp/data.tar.gz -C "$DATA_DIR" .
SIZE=$(du -h /tmp/data.tar.gz | cut -f1)
ROW_COUNT=$(cat "$DATA_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
echo "  Archive: ${SIZE} (${ROW_COUNT} total rows)"

# Create the release
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Creating release '${DATA_TAG}'..."
if gh release view "$DATA_TAG" &>/dev/null; then
  echo "  Release exists — updating asset..."
  gh release upload "$DATA_TAG" /tmp/data.tar.gz --clobber
  gh release edit "$DATA_TAG" --notes "Last updated: ${DATE} — ${ROW_COUNT} rows"
else
  gh release create "$DATA_TAG" /tmp/data.tar.gz \
    --title "Latest GPU Pricing Data" \
    --notes "Last updated: ${DATE} — ${ROW_COUNT} rows"
fi

echo "=== Done! Release '${DATA_TAG}' is live ==="
echo "  View at: $(gh repo view --json url -q .url)/releases/tag/${DATA_TAG}"
