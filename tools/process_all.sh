#!/bin/bash

SOLR_URL=$1

(
    cd ~/src/irc-logs
    time env/bin/python tools/nefget.py
)

(
    cd ~/src/sirc
    PYTHONPATH=.:../irc-logs/tools time env/bin/python tools/index.py $SOLR_URL ~/src/irc-logs/logs/*/*/*/*[0-9]
    PYTHONPATH=.:../irc-logs/tools time env/bin/python tools/upload.py ~/src/irc-logs/logs/*/*/*/*[0-9]
    PYTHONPATH=.:../irc-logs/tools time env/bin/python tools/post_log_stats.py ~/src/irc-logs/logs
)
