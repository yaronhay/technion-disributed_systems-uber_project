#!/bin/bash

req_body="$2"
serverList="$1"
random_server=$(sed '/^\s*$/d' "$serverList" | gshuf -n 1)

method="POST"
resource="path"
header="Content-Type: application/json"

headers=$(mktemp)
body=$(mktemp)

curl \
  --header "$header" \
  --request $method \
  --data "$req_body" \
  --dump-header "$headers" \
  --output "$body" \
  --write-out "@client/curl-format.txt" \
  "$random_server/$resource"  > /dev/null 2> /dev/null

cat "$body"
rm "$headers" "$body"
