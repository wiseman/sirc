#!/usr/bin/env python
from __future__ import with_statement
import sys
import re
import cgi
import logging
import StringIO
import time
import unicodedata
import optparse

import boto
import ircloglib

import sirc.util.s3
import sirc.log
import sirc.solr


def _error(msg):
  sys.stdout.flush()
  sys.stderr.write('%s\n' % (msg,))


def _usage():
  sys.stdout.flush()
  sys.stderr.write('Usage: %s <solr url> <logpath> [<logpath>...]\n' % \
                   (sys.argv[0],))
  sys.stderr.write('Where <logpath> is a local file path to a log file ' +
                   'or an s3:// url.\n')


def is_already_indexed(solr_url, log_data):
  id = sirc.log.encode_id(log_data) + '*'
  conn = get_solr_connection(solr_url)
  query = 'id:%s' % (id,)
  response = conn.query(q=query,
                        fields='id',
                        score=False)
  return len(response) > 0


def index_files(solr_url, paths, force=False, ignore_errors=False):
  for path in paths:
    try:
      log_data = sirc.log.parse_log_path(path)
      if force or not is_already_indexed(solr_url, log_data):
        index_file(solr_url, path)
      else:
        #print 'Skipping %s' % (path,)
        pass
    except LogParseException, e:
      if not ignore_errors:
        raise
      else:
        logging.error(e)
  quartiles()
  print 'Optimizing...'
  get_solr_connection(solr_url).optimize()


def index_file(solr_url, path):
  if path.startswith('s3://'):
    index_s3_file(solr_url, path)
  else:
    index_local_file(solr_url, path)


def index_s3_file(solr_url, path):
  bucket, s3_path = sirc.util.s3.parse_s3_url(path)
  bucket = sirc.util.s3.get_s3_bucket(bucket)
  key = boto.s3.key.Key(bucket)
  key.key = s3_path
  log_data = sirc.log.metadata_from_s3path(s3_path)
  log_contents = key.get_contents_as_string()
  log_fp = StringIO.StringIO(log_contents)
  return index_fp(solr_url, log_data, log_fp)


def index_local_file(solr_url, path):
  with open(path, 'rb') as f:
    log_data = ircloglib.parse_header(f.readline())
    f.seek(0)
    index_fp(solr_url, log_data, f)


def index_fp(solr_url, log_data, fp):
  records = list(index_records_for_fp(log_data, fp))
  post_records(solr_url, records)


def index_records_for_fp(log_data, fp):
  print 'Indexing %s' % (log_data,)
  first_line = fp.readline()
  log_data = ircloglib.parse_header(first_line)
  position = fp.tell()
  line_num = 0
  line = fp.readline()
  while line != '':
    
    xformed = index_record_for_line(log_data, line, line_num, position, timestamp)
    position = fp.tell()
    line_num += 1
    line = fp.readline()
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


LINE_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+)(.*)', re.UNICODE)

def index_record_for_line(log_data, line, line_num, position):
  result = parse_log_line(line)
  if result:
    timestamp, who, message = result
    who = cgi.escape(recode(who))
    message = recode(message)
    message = cgi.escape(message)
    date_str = '%s-%02d-%02d' % (log_data.start_time.year,
                                 log_data.start_time.month,
                                 log_data.start_time.day)
    timestamp = '%sT%sZ' % (date_str, timestamp,)
    id = get_log_id(log_data, line_num)
    return {
      'id': id,
      'channel': log_data.channel,
      'timestamp': timestamp,
      'user': who,
      'text': message,
      'position': position
      }


def index_record_for_day(log_data, index_time):
  id = sirc.log.encode_id(log_data)
  date_str = '%s-%02d-%02d' % (index_time.year,
                               index_time.month,
                               log_data.start_time.day)
  


def get_log_id(log_data, line_num):
  return sirc.log.encode_id(log_data, suffix='%05d' % (line_num,))


def parse_log_line(line):
  match = LOG_LINE_RE.match(line)
  if match:
    # timestamp, who, text
    return match.groups()


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
    usage='usage: %prog [options] [<log source>...]')
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
    parser.print_usage()
    return 1
  solr_url = args[0]
  files = args[1:]
  index_files(solr_url, files, force=options.force,
              ignore_errors=options.ignore_log_parse_errors)


if __name__ == '__main__':
  sys.exit(main(sys.argv))
