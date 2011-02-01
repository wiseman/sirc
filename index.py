import sys
import re
import logging
import datetime
from google.appengine.ext import webapp

from google.appengine.ext import db
from google.appengine.ext import blobstore

import mapreduce.operation
import mapreduce.control
import mapreduce.context

import tokenz


class DayLog(db.Model):
  channel = db.StringProperty(required=True)
  date = db.DateProperty(required=True)
  blob = blobstore.BlobReferenceProperty(required=True)
  indexing_state = db.StringProperty(required=True,
                                     choices=set(['unindexed',
                                                  'indexed',
                                                  'in-progress']))
  indexing_job_id = db.StringProperty()

class LogLineIndex(db.Model):
  log = db.ReferenceProperty(DayLog)
  position = db.IntegerProperty()
  channel = db.StringProperty(required=True)
  timestamp = db.DateTimeProperty(required=True)
  terms = db.StringListProperty(required=True)
  text = db.StringProperty()
  user = db.StringProperty()


def delete_logs():
  for b in blobstore.BlobInfo.all():
    b.delete()

def delete_indices():
  for i in LogLineIndex.all():
    i.delete()
  for i in Indices.all():
    i.delete()



NUM_INDEXER_SHARDS = 1

def start_indexing_log(blob_info):
  blob_reader = blob_info.open()
  # The first line of the log should look something like this:
  # 00:00:00 --- log: started lisp/04.01.01
  log_header_re = re.compile(r'.*log: started (.+)/([0-9\.]+)')
  first_line = blob_reader.readline()
  match = log_header_re.match(first_line)
  if not match:
    raise Exception('Unable to parse log header %s: "%s"' % (blob_reader.key(), first_line))
  #logging.info('%s' % (match.groups(),))
  channel = match.groups()[0]
  date_str = match.groups()[1]
  #logging.info('date_str=%s, %s' % (date_str, date_str[0:2]))
  year = 2000 + int(date_str[0:2])
  month = int(date_str[3:5])
  day = int(date_str[6:8])
  log_date = datetime.date(year, month, day)

  log = DayLog(channel=channel, date=log_date, blob=blob_info.key(),
               indexing_state='in-progress')
  log.put()
  job_id = mapreduce.control.start_map('Index log %s' % (blob_info.key()),
                                       handler_spec='index.index_log_line',
                                       reader_spec='mapreduce.input_readers.BlobstoreLineInputReader',
                                       shard_count=NUM_INDEXER_SHARDS,
                                       reader_parameters={'blob_keys': str(blob_info.key()),
                                                          'log_key': str(log.key())},
                                       mapreduce_parameters={'done_callback': '/indexing_did_finish'})
  

LINE_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+) <(\w+)> ?(.*)', re.UNICODE)

def parse_log_line(line):
  match = LINE_RE.match(line)
  if match:
    # timestamp, who, text
    return match.groups()




def index_log_line(entity):
  context = mapreduce.context.get()
  params = context.mapreduce_spec.mapper.params
  #logging.info('params: %s' % (params,))
  #logging.info('Got entity: %s' % (entity,))
  log_key = params['log_key']
  position, line = entity

  parsed_line = parse_log_line(line)
  if parsed_line:
    timestamp_str, user, text = parsed_line

    log = DayLog.get(params['log_key'])
    msg_hour = int(timestamp_str[0:2])
    msg_minute = int(timestamp_str[3:5])
    msg_second = int(timestamp_str[6:8])
    timestamp = datetime.datetime(log.date.year,
                                  log.date.month,
                                  log.date.day,
                                  msg_hour,
                                  msg_minute,
                                  msg_second)
    
    encoded = False
    try:
      text_u = tokenz.recode(text)
      terms_u = tokenz.extract_text_tokens(text_u)
      user_u = tokenz.recode(user)
      encoded = True
    except tokenz.EncodingError, e:
      logging.error('Unable to encode line %r: %s' % (text, e))

    if encoded:
      index = LogLineIndex(log=log,
                           position=position,
                           channel=log.channel,
                           timestamp=timestamp,
                           terms=terms_u,
                           user=user_u,
                           text=text_u)
      yield mapreduce.operation.db.Put(index)


class IndexingFinished(webapp.RequestHandler):
  def post(self):
    pass


def parse_query_string(query_string):
  logging.info('%r' % (query_string,))
  words = tokenz.extract_text_tokens(query_string)
  return words

def make_db_query_from_parsed_query(parsed_query):
  db_query = LogLineIndex.all()
  for word in parsed_query:
    db_query.filter('terms =', word)
  #db_query.order('-timestamp')
  return db_query

  
def get_query_results(query_string):
  parsed_query = parse_query_string(query_string)
  db_query = make_db_query_from_parsed_query(parsed_query)
  records = db_query.fetch(200)
  #for r in records:
  #  r.line_text = get_log_line_from_position(blob_reader, r.position)
  return records


def get_log_line_from_position(blob_reader, position):
  blob_reader.seek(position)
  return blob_reader.readline()


if __name__ == '__main__':
  for line in sys.stdin:
    parsed_line = parse_log_line(line)
    if parsed_line:
      timestamp, who, text = parsed_line
      words = extract_text_tokens(text)
      print words

    
