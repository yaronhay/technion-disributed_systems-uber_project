#!/bin/bash

read -r -d '' req_body <<-EOF
{
  "day" :                 13,
  "month":                1,
  "year" :                2021,
  "source":               "city4",
  "destination":          "city3",
  "firstname" :           "Yaron",
  "lastname":             "Hay",
  "phonenumber":          "0123456789",
  "permitted-deviation":  1,
  "vacancies":            1
}
EOF

host="localhost"
port="6000"

method="PUT"
resource="rides"

curl --header "Content-Type: application/json" \
  --request $method \
  --data "$req_body" \
  "$host:$port/$resource"