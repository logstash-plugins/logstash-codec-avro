#!/bin/bash
set -ex

echo "Starting SchemaRegistry"
build/confluent_platform/bin/schema-registry-start build/confluent_platform/etc/schema-registry/schema-registry-mutual.properties > /dev/null 2>&1 &
