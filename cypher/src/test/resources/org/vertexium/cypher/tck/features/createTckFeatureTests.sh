#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST_DIR=${DIR}/../../../../../../java/org/vertexium/cypher/tck
TEMPLATE=${DIR}/TckFeatureTest.template

for f in $(find . | grep \.feature$); do
  filename=$(basename $f)
  fname=${filename%.*}

  sed -e s/\${FNAME}/${fname}/g ${TEMPLATE} > ${TEST_DIR}/${fname}Test.java
done
