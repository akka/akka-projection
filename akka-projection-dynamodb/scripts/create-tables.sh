#!/usr/bin/env bash

#create-timestamp-offset-table
aws dynamodb create-table \
  --table-name timestamp_offset \
  --attribute-definitions \
      AttributeName=name_slice,AttributeType=S \
      AttributeName=pid,AttributeType=S \
  --key-schema \
      AttributeName=name_slice,KeyType=HASH \
      AttributeName=pid,KeyType=RANGE \
  --provisioned-throughput \
      ReadCapacityUnits=5,WriteCapacityUnits=5
#create-timestamp-offset-table
