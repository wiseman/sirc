#!/usr/bin/env python

import datetime
import pprint
import sys
import threading

import sirc.solr


def all_indexed(solr_url):
    query = 'id:day/*'
    conn = get_solr_connection(solr_url)
    response = conn.query(q=query, score=False, rows=1000)
    num_results = len(response.results)
    while num_results > 0:
        for result in response.results:
            yield result
        response = response.next_batch()
        num_results = len(response.results)


g_solr_connections = {}

def get_solr_connection(solr_url):
  assert solr_url.startswith('http')
  key = (threading.current_thread(), solr_url)
  if key in g_solr_connections:
    return g_solr_connections[key]
  connection = sirc.solr.SolrConnection(url=solr_url)
  g_solr_connections[key] = connection
  return connection


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
