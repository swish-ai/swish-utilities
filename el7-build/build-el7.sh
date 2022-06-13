#!/usr/bin/env bash

DIR=$(dirname $0)

yum install -y python3 
pip3 install -r ${DIR}/requirements-el7.txt
pip3 install wheel dataclasses pyinstaller==3.6

export LANG=en_US.utf8
WORK_DIR=$(realpath ${DIR}/..)
cd "$WORK_DIR"

DIST_NAME=${DIST_NAME:-swish-utilities}
pyinstaller --clean --onefile \
  --exclude-module matplotlib --exclude-module tkinter --exclude-module qt5 \
  --exclude-module python-dateutil --exclude-module pyinstaller --exclude-module tests \
  --hidden-import cli_util --hidden-import requests --hidden-import logging --hidden-import logging --hidden-import logging.handlers  --hidden-import flashtext \
  run.py -n ${DIST_NAME}

