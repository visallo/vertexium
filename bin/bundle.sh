#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"


(
  cd ${DIR}/..
  git pull
  git tag | grep -q last-bundle
  [ $? -eq 0 ] && ref='last-bundle..master' || ref='master'
  bundle_filename="${DIR}/../../vertexium.$(date '+%Y%m%dT%H%M').bundle"
  git bundle create ${bundle_filename} ${ref}
  git tag -f last-bundle master
  git push origin :last-bundle
  git push origin last-bundle
  echo "created ${bundle_filename}"
)
