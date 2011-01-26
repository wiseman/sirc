import sys
import re
import logging

from mapreduce import operation as op
from google.appengine.ext import db
from google.appengine.ext import blobstore


class LogLineIndex(db.Model):
  position = db.IntegerProperty()
  timestamp = db.StringProperty(required=True)
  who = db.StringProperty()
  words = db.StringListProperty()
  text = db.ByteStringProperty()

class Indices(db.Model):
  name = db.StringProperty(required=True)
  blob_key = blobstore.BlobReferenceProperty(required=True)
  indexing_state = db.StringProperty(required=True)
  indexing_job_id = db.StringProperty()


def delete_logs():
  for b in blobstore.BlobInfo.all():
    b.delete()

def delete_indices():
  for i in LogLineIndex.all():
    i.delete()
  for i in Indices.all():
    i.delete()




LINE_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+) <(\w+)> ?(.*)')

def parse_log_line(line):
  match = LINE_RE.match(line)
  if match:
    # timestamp, who, text
    return match.groups()


TOKEN_RE = re.compile('\W+')

def extract_text_tokens(text):
  words = TOKEN_RE.split(text)
  words = [w.lower() for w in set(words) if len(w) > 0]
  return words
  


def index_log_line(entity):
  position, line = entity
  parsed_line = parse_log_line(line)
  if parsed_line:
    timestamp, who, text = parsed_line
    words = extract_text_tokens(text)
    index = LogLineIndex(position=position,
                         timestamp=timestamp,
                         who=who,
                         words=words,
#                         text=str(line)
                         )
    yield op.db.Put(index)


def parse_query_string(query_string):
  words = extract_text_tokens(query_string)
  return words

def make_db_query_from_parsed_query(parsed_query):
  db_query = LogLineIndex.all()
  for word in parsed_query:
    db_query.filter('words =', word)
  return db_query

  
def get_query_results(query_string):
  parsed_query = parse_query_string(query_string)
  db_query = make_db_query_from_parsed_query(parsed_query)
  records = db_query.fetch(10)
  blob_reader = blobstore.BlobInfo.all().fetch(1)[0].open()
  for r in records:
    r.line_text = get_log_line_from_position(blob_reader, r.position)
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

    
