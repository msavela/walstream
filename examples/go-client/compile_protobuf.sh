#!/bin/bash

protoc \
  --proto_path=../../proto \
  --go_out=. \
  --go_opt=Mplugin.proto=generated/plugin \
  --go-grpc_out=. \
  --go-grpc_opt=Mplugin.proto=generated/plugin \
  ../../proto/plugin.proto