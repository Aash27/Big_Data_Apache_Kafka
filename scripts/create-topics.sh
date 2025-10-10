#!/usr/bin/env bash
set -euo pipefail

create_topic () {
  local topic=$1
  docker exec kafka kafka-topics.sh \
    --create \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    || true
}

echo "Creating topics..."
create_topic news.raw
create_topic news.cleaned
create_topic news.predictions
echo "Done."
