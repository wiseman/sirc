import zlib

from google.appengine.api import memcache

import boto

import sirc.log
import sirc.logrender
from sirc.util import s3


def fetch_from_key(key):
  (log_data, suffix) = sirc.log.decode_id(key)
  s3path = 'rawlogs/%s/%s/%02d.%02d' % (log_data.channel,
                                        log_data.start_time.year,
                                        log_data.start_time.month,
                                        log_data.start_time.day)
  credentials = s3.get_credentials()
  conn = boto.connect_s3(credentials.access_key, credentials.secret,
                         debug=0)
  bucket_name = 'sirc'
  bucket = conn.create_bucket(bucket_name)
  s3key = boto.s3.key.Key(bucket)
  s3key.key = s3path
  data = s3key.get_contents_as_string()
  return data


def render_from_key(key):
  cached_compressed_html = memcache.get(key)
  if cached_compressed_html:
    return zlib.decompress(cached_compressed_html)

  data = fetch_from_key(key)
  html = sirc.logrender.render_log(sirc.log.decode_id(key)[0], data)

  # Cache for up to 5 minutes.  We probably update the newest logs
  # every 10 minutes...
  memcache.set(key, zlib.compress(html), time=300)
  return html
