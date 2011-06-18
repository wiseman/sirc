#!/bin/bash

SOLR_URL=$1

(
    cd ~/src/irc-logs
    env/bin/python tools/nefget.py
)

time (
    cd ~/src/sirc
    PYTHONPATH=.:../irc-logs/tools env/bin/python tools/index.py $SOLR_URL ~/src/irc-logs/logs/*/*/*/*[0-9]
    PYTHONPATH=.:../irc-logs/tools env/bin/python tools/upload.py ~/src/irc-logs/logs/*/*/*/*[0-9]
)
