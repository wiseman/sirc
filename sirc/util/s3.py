from __future__ import with_statement
import re
import fnmatch

import iso8601

import boto.s3.bucketlistresultset
import boto.s3.key


class Key(boto.s3.key.Key):
  def __init__(self, *args, **kwargs):
    boto.s3.key.Key.__init__(self, *args, **kwargs)

  def set_contents_from_filename(*args, **kwargs):
    self = args[0]
    self.key = translate_to_s3_path(self.path)
    print 'Setting content for key=%r' % (self.key,)
    print boto.s3.key.Key.set_contents_from_file(*args, **kwargs)


def parse_s3_url(url):
  pieces = [p for p in url.split('/') if len(p) > 0]
  return pieces[1], '/'.join(pieces[2:])


def translate_to_s3_path(logical_path):
  return logical_path
#  return logical_path.replace('/', ',folder/')


def translate_from_s3_path(s3_path):
  return s3_path
#  return s3_path.replace(',folder/', '/')


def split_s3_path(s3_path):
  path = translate_from_s3_path(s3_path)
  return [p for p in path.split('/') if len(p) > 0]


PATTERN_CHARS_RE = re.compile(r'\[\|\?|\*')


def get_all_keys(bucket):
  return [k for k in boto.s3.bucketlistresultset.bucket_lister(bucket)]


def glob_s3_path(bucket, s3_path, remote_keys=None):
  if not remote_keys:
    remote_keys = get_all_keys(bucket)
  regex = fnmatch.translate(s3_path)
  #print regex
  regex = re.compile(regex)
  #print [k.name for k in keys]
  matches = [k.name for k in remote_keys if regex.match(k.name)]
  #print matches
  return matches


g_remote_keys_cache = {}


def cached_glob_s3_path(bucket, s3_path):
  global g_remote_keys_cache
  if not bucket in g_remote_keys_cache:
    g_remote_keys_cache[bucket] = get_all_keys(bucket)
  x = glob_s3_path(bucket, s3_path, remote_keys=g_remote_keys_cache[bucket])
  return x


def key_exists(bucket, s3_path):
  return len(cached_glob_s3_path(bucket, s3_path)) > 0


def cached_get_mtime(bucket, s3_path):
  global g_remote_keys_cache
  if not bucket in g_remote_keys_cache:
    g_remote_keys_cache[bucket] = get_all_keys(bucket)
  x = get_mtime(bucket, s3_path, remote_keys=g_remote_keys_cache[bucket])
  return x


def get_mtime(bucket, s3_path, remote_keys=None):
  if not remote_keys:
    remote_keys = get_all_keys(bucket)
  # Linear search duh whatever.
  for k in remote_keys:
    if k.name == s3_path:
      mtime_str = k.last_modified
      mtime = iso8601.parse_date(mtime_str)
      return mtime
  # Hacky way to raise a KeyError.
  {}[s3_path]


def mkdir(bucket, logical_path):
  pieces = [p for p in logical_path.split('/') if len(p) > 0]
  prefix = []
  for piece in pieces:
    k = boto.s3.key.Key(bucket)
    prefix += [piece]
    path = '/'.join(prefix)
    k.key = translate_to_s3_path(path + '/')
    print 'mkdir on key=%r' % (k.key,)
    print k.set_contents_from_string('')


class Credentials:
  def __init__(self, access_key, secret):
    self.access_key = access_key
    self.secret = secret


def get_credentials():
  with open('creds.txt', 'rb') as f:
    aws_access_key_id = f.readline()[:-1]
    aws_secret_access_key = f.readline()[:-1]
  return Credentials(aws_access_key_id, aws_secret_access_key)


g_connection = None


def get_s3_connection():
  global g_connection
  if not g_connection:
    credentials = get_credentials()
    g_connection = boto.connect_s3(credentials.access_key,
                                   credentials.secret,
                                   debug=1)
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
