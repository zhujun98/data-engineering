#!/bin/bash

set -e

build() {
    NAME=$1
    IMAGE=zhujun/spark-local-$NAME
    cd $([ -z "$2" ] && echo "./$NAME" || echo "$2")
    echo '--------------------------' building $IMAGE in $(pwd)
    docker build -t $IMAGE .
    cd -
}

build base
build master
build worker
build submit
# build jupyterlab
build python-template template/python
# build java-template template/java
# build scala-template template/scala