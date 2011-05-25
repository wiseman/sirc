#!/bin/bash

PYTHONPATH=. env/bin/python tools/index.py $*

if [ $? -ne 0 ]; then
    exit 255
fi
