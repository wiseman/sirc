#!/usr/bin/env python
from __future__ import with_statement
import sys
import re
import cgi
import datetime
import logging
import optparse
import Queue
import StringIO
import threading
import time
import unicodedata

import boto
import ircloglib

import sirc.util.s3
import sirc.log
import sirc.solr


class IndexingError:
  pass


class UTC(datetime.tzinfo):
  def utcoffset(self, dt):
    return datetime.timedelta(0)

  def tzname(self, dt):
    return "UTC"

  def dst(self, dt):
    return datetime.timedelta(0)


g_utc = UTC()


def _error(msg):
  sys.stdout.flush()
  sys.stderr.write('error: %s\n' % (msg,))


def _usage():
  sys.stdout.flush()
  sys.stderr.write('Usage: %s <solr url> <logpath> [<logpath>...]\n' % \
                   (sys.argv[0],))
  sys.stderr.write('Where <logpath> is a local file path to a log file ' +
                   'or an s3:// url.\n')


class Worker(threading.Thread):
  """Thread executing tasks from a given tasks queue"""
  def __init__(self, tasks):
    threading.Thread.__init__(self)
    self.tasks = tasks
    self.daemon = True
    self.start()
    
  def run(self):
    while True:
      func, args, kargs = self.tasks.get()
      try: func(*args, **kargs)
      except Exception, e: print e
      self.tasks.task_done()


class ThreadPool:
  """Pool of threads consuming tasks from a queue"""
  def __init__(self, num_threads):
    self.tasks = Queue.Queue(num_threads)
    for _ in range(num_threads):
      Worker(self.tasks)

  def add_task(self, func, *args, **kargs):
    """Add a task to the queue"""
    self.tasks.put((func, args, kargs))

  def wait_completion(self):
    """Wait for completion of all the tasks in the queue"""
    self.tasks.join()


# ----------------------------------------
# Multi-threaded

# def grouper(n, iterable, fillvalue=None):
#   "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
#   args = [itertools.iter(iterable)] * n
#   return itertools.izip_longest(fillvalue=fillvalue, *args)


# def index_files(solr_url, paths, thread_pool, force=False, ignore_errors=False):
#   for path_group in grouper(INDEX_BATCH_SIZE, paths):
#     log_datas = [sirc.log.parse_log_path(path) for path in path_group]
#     thread_pool.add_task(index_file_group, log_datas)


# def index_file_group(solr_url, log_datas, force=False):
#   index_times = get_index_times(solr_url, log_datas)
#   for log_data in log_datas:
#     if force or \
#           (not log_data in index_times) or \
#           index_times[log_data] <= file_mtime(log_data.path):
#       print 'Indexing %s' % (log_data.path,)


# def file_mtime(path):
#   mtime = os.stat(path).st_mtime
#   mtime = datetime.datetime.fromtimestamp(mtime, tz=pytz.utc)
#   return mtime


# ----------------------------------------
# Single-threaded.

def index_documents(solr_url, doc_paths, force=False, ignore_errors=False):
  for path in doc_paths:
    log_data = sirc.log.parse_log_path(path)
    if force or not is_already_indexed(solr_url, log_data):
      doc = get_document(path)
      index_document(solr_url, doc)
    else:
      #print 'Skipping %s' % (path,)
      pass
  quartiles()
  print 'Optimizing...'
  get_solr_connection(solr_url).optimize()


class Document:
  def __init__(self, log_data, file_object):
    self.file = file_object
    self.log_data = log_data


def get_document(doc_path):
  if is_s3_path(doc_path):
    return get_s3_document(doc_path)
  else:
    return get_fs_document(doc_path)


def is_s3_path(doc_path):
  return doc_path.startswith('s3://')


def get_fs_document(doc_path):
  fp = open(doc_path, 'rb')
  log_data = ircloglib.parse_header(fp.readline())
  fp.seek(0)
  return Document(log_data, fp)


def get_s3_document(doc_path):
  bucket, s3_path = sirc.util.s3.parse_s3_url(doc_path)
  bucket = sirc.util.s3.get_s3_bucket(bucket)
  key = boto.s3.key.Key(bucket)
  key.key = s3_path
  log_data = sirc.log.metadata_from_s3path(s3_path)
  log_contents = key.get_contents_as_string()
  log_fp = StringIO.StringIO(log_contents)
  return Document(log_data, log_fp)
  

def is_already_indexed(solr_url, log_data):
  id = sirc.log.encode_id(log_data) + '*'
  conn = get_solr_connection(solr_url)
  query = 'id:%s' % (id,)
  response = conn.query(q=query,
                        fields='id',
                        score=False)
  return len(response) > 0


def index_document(solr_url, doc):
  records = list(index_records_for_document(doc))
  if len(records) > 0:
    print records[0]
  post_records(solr_url, records)


def index_records_for_document(doc):
  print 'Indexing %s' % (doc.log_data,)
  first_line = doc.file.readline()
  log_data = ircloglib.parse_header(first_line)
  r = index_record_for_day(
    log_data,
    datetime.datetime.utcnow().replace(tzinfo=g_utc))
  yield r
  position = doc.file.tell()
  line_num = 0
  line = doc.file.readline()
  while line != '':
    xformed = index_record_for_line(log_data, line, line_num, position)
    position = doc.file.tell()
    line_num += 1
    line = doc.file.readline()
    if xformed:
      yield xformed


g_solr_connection = None
g_solr_url = None


def get_solr_connection(solr_url):
  assert solr_url.startswith('http')
  global g_solr_url, g_solr_connection
  if not (g_solr_url == solr_url and g_solr_connection):
    g_solr_url = solr_url
    g_solr_connection = sirc.solr.SolrConnection(url=solr_url)
  return g_solr_connection


def post_records(solr_url, index_records):
  if len(index_records) == 0:
    return
  start_time = time.time()
  conn = get_solr_connection(solr_url)
  #  for i in index_records:
  #    print i
  #    conn.add(i)
  conn.add_many(index_records)
  commit_start_time = time.time()
  conn.commit()
  commit_end_time = time.time()
  total_ms = int((commit_end_time - start_time) * 1000)
  commit_ms = int((commit_end_time - commit_start_time) * 1000)

  record_measurement('total', total_ms)
  record_measurement('commit', commit_ms)
  logging.info('Posted %s records in %s ms (%s ms commit)',
               len(index_records),
               total_ms, commit_ms)


def index_record_for_line(log_data, line, line_num, position):
  result = ircloglib.parse_line(line)
  kind, timestamp = result[0:2]
  if not kind in (ircloglib.MSG, ircloglib.ACTION):
    return None

  offset_seconds = \
      int(timestamp[0:2]) * 3600 + \
      int(timestamp[3:5]) * 60 + \
      int(timestamp[7:9])
  time_offset = datetime.timedelta(seconds=offset_seconds)
  line_timestamp = log_data.start_time + time_offset
  record = {
    'id': get_log_id(log_data, line_num),
    'server': log_data.server,
    'channel': log_data.channel,
    'timestamp': line_timestamp,
    'user': recode(result[2]),
    'text': recode(result[3])
    }
  return record


def index_record_for_day(log_data, index_time):
  record = {
    'id': sirc.log.encode_id(log_data),
    'server': log_data.server,
    'channel': log_data.channel,
    'index_timestamp': index_time
    }
  return record


def get_log_id(log_data, line_num):
  return sirc.log.encode_id(log_data, suffix='%05d' % (line_num,))


def is_ctrl_char(c):
  return unicodedata.category(c) == 'Cc'


def recode(text):
  try:
    recoded_text = unicode(text, 'cp1252', 'replace')
  except UnicodeDecodeError, e:
    print 'error unicoding %r: %s' % (text, e)
    raise

  recoded_text = ''.join([c for c in recoded_text if not is_ctrl_char(c)])
  return recoded_text


g_measurements = {}


def record_measurement(label, n):
  global g_measurements
  if not label in g_measurements:
    g_measurements[label] = []
  g_measurements[label].append(n)


def quartiles():
  global g_measurements
  for key in g_measurements:
    quartile(key, g_measurements[key])


def quartile(label, measurements):
  global g_measurements
  measurements = sorted(measurements)
  n = len(measurements)
  for x in [n * 0.1, n * 0.25, n * 0.5, n * 0.75, n * 0.9]:
    i = int(x)
    print '%s   %4s %%: %s' % (label, int((x / n) * 100), measurements[i])


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main(args):
  parser = optparse.OptionParser(
    usage='usage: %prog [options] <solr url> <logpath> [<logpath>...]')
  parser.add_option(
    '-f',
    '--force',
    dest='force',
    action='store_true',
    default=False,
    help='Indexes the file even if it has already been indexed ' + \
    '(default is %default).')
  parser.add_option(
    '-i',
    '--ignore-log-parse-errors',
    dest='ignore_log_parse_errors',
    action='store_true',
    default=False,
    help='Ignore log parsing errors (default is %default).')
  (options, args) = parser.parse_args()

  logging.basicConfig(level=logging.INFO)
  if len(args) < 2:
    _error('Too few arguments.')
    parser.print_usage()
    return 1
  solr_url = args[0]
  files = args[1:]
  index_documents(solr_url, files, force=options.force,
                  ignore_errors=options.ignore_log_parse_errors)


if __name__ == '__main__':
  sys.exit(main(sys.argv))
