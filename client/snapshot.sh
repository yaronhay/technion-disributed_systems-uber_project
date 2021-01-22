#!/bin/bash

serverList="$1"
random_server=$(sed '/^\s*$/d' "$serverList" | gshuf -n 1)

method="GET"
resource="snapshot"

headers=$(mktemp)

curl \
  --header "Content-Type: application/json" \
  --request $method \
  --dump-header "$headers" \
  --output "log/snapshot$(date +"%Y.%m.%d-%T").json" \
  --write-out "@client/curl-format.txt" \
  "$random_server/$resource"

rm "$headers"
