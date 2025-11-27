#!/bin/bash

protoc \
    --plugin=./node_modules/.bin/protoc-gen-ts_proto \
    --ts_proto_out=./generated \
    --ts_proto_opt=esModuleInterop=true,outputServices=grpc-js \
    -I ../../proto ../../proto/plugin.proto