import re
import logging

from google.appengine.ext import db

import index


# Parse single term queries and phrase queries ("word another word").


class QueryPipe:
  def __init__(self, term, query):
    self.term = term
    self.query = query
    self.iterator = None
    self.record = None

  def advance(self):
    if not self.iterator:
      self.iterator = self.query.__iter__()
    logging.debug('Advancing %r' % (self,))
    self.record = self.iterator.next()
    logging.debug('Advanced %r' % (self,))

  def timestamp(self):
    return self.record.timestamp

  def __repr__(self):
    s = '<Query term=%s' % (self.term,)
    if self.record:
      s += ' timestamp=%s' % (self.timestamp(),)
    s += '>'
    return s

  
def term_query_pipe(term):
  query = db.Query(index.LogLineIndex)
  query.filter('terms =', term)
  query.order('-timestamp')
  return QueryPipe(term, query)


class MultiTermQueryPipe:
  def __init__(self, query_pipes):
    self.query_pipes = query_pipes
    self.started = False

  def advance_subqueries(self):
    if not self.started:
      logging.info('Starting %r' % (self,))
      for p in self.query_pipes:
        p.advance()
      self.started = True
    else:
        self.query_pipes[0].advance()

    # Reorder
    self.query_pipes = sorted(self.query_pipes,
                              key=QueryPipe.timestamp,
                              reverse=True)

    
  def advance(self):
    found_match = False
    num_records = 0
    while not found_match:
      #logging.info('Looking for match in %r' % (self,))
      self.advance_subqueries()
      num_records += 1
      if at_same_record(self.query_pipes):
        found_match = True

    logging.info('*** Found match in %s advances in %r' % (num_records, self,))
    return self.query_pipes[0].record

  def __repr__(self):
    return '<MTQP %s>' % ([pipe.term for pipe in self.query_pipes],)

  def fetch(self, n):
    records = []
    try:
      for i in xrange(n):
        logging.info('-------------------- FETCHING %s of %s' % (i, n))
        records.append(self.advance())
    except StopIteration:
      pass
    return records

def at_same_record(pipes):
  records = [p.record for p in pipes]
  keys = [r.key() for r in records]
  logging.debug('keys=%s' % (keys,))
  num_unique_keys = len(set(keys))
  return num_unique_keys == 1
  

def make_multi_term_query(terms):
  pipes = [term_query_pipe(term) for term in terms]
  return MultiTermQueryPipe(pipes)

  


## def s_ident(scanner, token): return token
## def s_operator(scanner, token): return "op%s" % token
## def s_float(scanner, token): return float(token)
## def s_int(scanner, token): return int(token)

## scanner = re.Scanner([
##   (r"[a-zA-Z_]\w*", s_ident),
##   (r"\d+\.\d*", s_float),
##   (r"\d+", s_int),
##   (r"=|\+|-|\*|/", s_operator),
##   (r"\s+", None),
##   ])

## # sanity check
## #print scanner.scan("sum = 3*foo + 312.50 + bar")



## def s_phrase_query(scanner, token): return ['PHRASE', token[1:-1]]
## def s_term_query(scanner, token): return ['TERM', token]

## scanner = re.Scanner([
##   (r'"(\s|[^_\W"])+"', s_phrase_query),
##   (r'[^_\W"]+', s_term_query),
##   (r'[\s_]+', None),
##   ])

## print scanner.scan('foo')
## print scanner.scan('foo bar')
## print scanner.scan('foo    bar')
## print scanner.scan('foo_bar')
## print scanner.scan('"foo bar"')

