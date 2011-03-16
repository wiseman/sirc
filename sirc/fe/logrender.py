import string
import cgi

from google.appengine.api import memcache

import boto

from sirc import log
from sirc.util import s3


LINE_TEMPLATE = '<tr>' + \
                '<td class="nick"><span class="brack">&lt;</span><span class="source">$user</span><span class="brack">&gt;</span></td>' + \
                '<td class="msg">$message</td>' + \
                '<td class="time">$time</td>' + \
                '</tr>\n'


def render_line(timestamp, user, message):
  time_str = '%02d:%02d:%02d' % (timestamp.hour, timestamp.minute, timestamp.second)
  user_str = cgi.escape(user)
  message_str = cgi.escape(message)
  template = string.Template(LINE_TEMPLATE)
  template.substitute(user=user_str, message=message_str, time=time_str)
  
def render_from_key(key):
  cached_data = memcache.get(key)
  if cached_data:
    return cached_data

  (log_data, suffix) = log.decode_id(key)
  s3path = 'rawlogs/%s/%s/%02d.%02d' % (log_data.channel,
                                        log_data.date.year,
                                        log_data.date.month,
                                        log_data.date.day)
  credentials = s3.get_credentials()
  conn = boto.connect_s3(credentials.access_key, credentials.secret,
                         debug=0)
  bucket_name = 'sirc'
  bucket = conn.create_bucket(bucket_name)
  s3key = boto.s3.key.Key(bucket)
  s3key.key = s3path
  data = s3key.get_contents_as_string()
  memcache.set(key, data, time=60)
  return data
