#!/bin/bash
set -e

NUM_THREADS=5
SOLR_URL=$1
shift

echo $* | xargs -P $NUM_THREADS -n 30 ./index_files.sh $SOLR_URL
