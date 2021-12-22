#!/usr/bin/env bash

DIR=$(dirname $0)
WORK_DIR=$(realpath ${DIR}/..)

DIST_NAME=${DIST_NAME:-swish-utilities}
docker run -v ${WORK_DIR}:/workdir --workdir=/workdir \
    -e DIST_NAME=${DIST_NAME} \
    centos:centos7.9.2009 el7-build/build-el7.sh