#!/usr/bin/env bash
set -e
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic mastodon_stream --replication-factor 1 --partitions 3
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic mastodon_errors --replication-factor 1 --partitions 1
