#!/bin/bash


# Clear down gaz indexes
curl -XDELETE 'http://localhost:9200/gaz'

curl -X PUT "localhost:9200/gaz" -d '{
  "settings" : {}
}'
