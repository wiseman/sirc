import threading

import sirc.solr


g_solr_connections = {}

g_lock = threading.Condition()


def get_solr_connection(solr_url):
  global g_solr_connections, g_lock
  assert solr_url.startswith('http')
  key = (threading.current_thread(), solr_url)
  with g_lock:
    if key in g_solr_connections:
      return g_solr_connections[key]
    connection = sirc.solr.SolrConnection(url=solr_url)
    g_solr_connections[key] = connection
    return connection
