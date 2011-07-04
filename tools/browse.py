#!/usr/bin/env python

import datetime
import pprint
import sys

import sirc.solr
import sirc.util.solr


def all_indexed(solr_url):
    query = 'id:day/*'
    conn = sirc.util.solr.get_solr_connection(solr_url)
    response = conn.query(q=query, score=False, rows=1000)
    num_results = len(response.results)
    while num_results > 0:
        for result in response.results:
            yield result
        response = response.next_batch()
        num_results = len(response.results)


def get_date(record):
    pieces = record[u'id'].split('/')
    year = int(pieces[3])
    month = int(pieces[4])
    day = int(pieces[5])
    return datetime.date(year, month, day)

if __name__ == '__main__':
    channels = {}
    for r in all_indexed(sys.argv[1]):
        c = '%s/%s' % (r[u'channel'], get_date(r).year)
        if c in channels:
            channels[c] += 1
        else:
            channels[c] = 1
    pprint.pprint(channels)
