#!/bin/bash

read -r -d '' req_body <<-EOF
{
  "day" :         13,
  "month":        1,
  "year" :        2021,
  "firstname" :   "Yaron",
  "lastname":     "Hay",
  "phonenumber":  "0123456789",
  "cities":       [
                    "city1",
                    "city5",
                    "city3"
                  ]
}
EOF

host="localhost"
port="6003"

method="POST"
resource="path"

curl --header "Content-Type: application/json" \
  --request $method \
  --data "$req_body" \
  "$host:$port/$resource"
