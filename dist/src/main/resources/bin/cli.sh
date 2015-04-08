#!/bin/bash

DIR=$(cd $(dirname "$0") && pwd)

java -cp ${DIR}/../lib/\* org.vertexium.cli.VertexiumShell "$@"
