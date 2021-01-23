#!/bin/bash

req_body="$2"
serverList="$1"


method="POST"
resource="path"
header="Content-Type: application/json"

headers=$(mktemp)
body=$(mktemp)

while ! curl \
  --header "$header" \
  --request $method \
  --data "$req_body" \
  --dump-header "$headers" \
  --output "$body" \
  --write-out "@client/curl-format.txt" \
  "$(sed '/^\s*$/d' "$serverList" | gshuf -n 1)/$resource"  > /dev/null 2> /dev/null; do
  :
done

cat "$body"
rm "$headers" "$body"
