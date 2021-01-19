#!/bin/bash


host="localhost"
port="6003"

method="Get"
resource="snapshot"

curl --header "Content-Type: application/json" \
  --request $method \
  --data "$req_body" \
  "$host:$port/$resource"
