#!/usr/bin/env python

import sys
import re
from string import Template
import cgi
import logging
import StringIO

import boto

import sirc.util.s3
import sirc.log
import sirc.solr


LOG_TIMESTAMP_HEADER_RE = re.compile(r'.*log: started (.+)/([0-9\.]+)')

LOG_LINE_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+) <(\w+)> ?(.*)', re.UNICODE)


def _error(msg):
  sys.stdout.flush()
  sys.stderr.write('%s\n' % (msg,))

def _usage():
  sys.stdout.flush()
  sys.stderr.write('Usage: %s <solr url> <logpath> [<logpath>...]\n' % (sys.argv[0],))
  sys.stderr.write('Where <logpath> is a local file path to a log file or an s3:// url.\n')


def index_files(solr_url, paths):
  for path in paths:
    index_file(solr_url, path)


def index_file(solr_url, path):
  if path.startswith('s3://'):
    index_s3_file(solr_url, path)
  else:
    index_local_file(solr_url, path)


def index_s3_file(solr_url, path):
  bucket, s3_path = parse_s3_url(path)
  bucket = get_s3_bucket(bucket)
  key = boto.s3.key.Key(bucket)
  key.key = s3_path
  log_sink = StringIO.StringIO()
  log_data = sirc.log.metadata_from_s3path(s3_path)
  log_contents = key.get_contents_as_string()
  log_fp = StringIO.StringIO(log_contents)
  return index_fp(solr_url, log_data, log_fp)


def index_local_file(solr_url, path):
  with open(path, 'rb') as f:
    log_data = sirc.log.metadata_from_logpath(path)
    index_fp(solr_url, log_data, f)


def index_fp(solr_url, log_data, fp):
  post_records(solr_url, list(index_records_for_fp(log_data, fp)))


def index_records_for_fp(log_data, fp):
  logging.info('Indexing %s' % (log_data,))
  first_line = fp.readline()
  match = LOG_TIMESTAMP_HEADER_RE.match(first_line)
  if not match:
    raise Exception('Unable to parse log header %s: %r' % (log_data.path, first_line))
  assert log_data.channel == match.groups()[0]
  
  position = fp.tell()
  line_num = 0
  line = fp.readline()
  while line != '':
    xformed = index_record_for_line(log_data, line, line_num, position)
    position = fp.tell()
    line_num += 1
    line = fp.readline()
    if xformed:
      yield xformed


def post_records(solr_url, index_records):
  conn = sirc.solr.SolrConnection(url=solr_url)
  conn.add_many(index_records)
  conn.commit()

def index_record_for_line(log_data, line, line_num, position):
  result = parse_log_line(line)
  if result:
    timestamp, who, message = result
    who = cgi.escape(recode(who))
    message = recode(message)
    message = cgi.escape(message)
    date_str = '%s-%02d-%02d' % (log_data.date.year,
                                 log_data.date.month,
                                 log_data.date.day)
    timestamp = '%sT%sZ' % (date_str, timestamp,)

    return {
      'id': id,
      'channel': log_data.channel,
      'timestamp': timestamp,
      'user': who,
      'text': message,
      'position': position
      }


def parse_log_line(line):
  match = LOG_LINE_RE.match(line)
  if match:
    # timestamp, who, text
    return match.groups()


import unicodedata

def is_ctrl_char(c):
  return unicodedata.category(c) == 'Cc'


def recode(text):
  recoded_text = unicode(text, 'cp1252', 'strict')
  recoded_text = ''.join([c for c in recoded_text if not is_ctrl_char(c)])
  return recoded_text




# ------------------------------------------------------------
# S3 utils
# ------------------------------------------------------------

def parse_s3_url(url):
  pieces = [p for p in url.split('/') if len(p) > 0]
  return pieces[1], '/'.join(pieces[2:])


g_connection = None

def get_s3_connection():
  global g_connection
  if not g_connection:
    credentials = sirc.util.s3.get_credentials()
    g_connection = boto.connect_s3(credentials.access_key, credentials.secret, debug=1)
  return g_connection
  

g_buckets = {}

def get_s3_bucket(bucket_name):
  global g_buckets
  if not (bucket_name in g_buckets):
    conn = get_s3_connection()
    bucket = conn.create_bucket(bucket_name)
    g_buckets[bucket_name] = bucket
  else:
    bucket = g_buckets[bucket]
  return bucket


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main(argv):
  logging.basicConfig(level=logging.INFO)
  args = argv[1:]
  if len(args) < 2:
    _usage()
    sys.exit(1)
  solr_url = args[0]
  files = args[1:]
  index_files(solr_url, files)

  
if __name__ == '__main__':
  main(sys.argv)


