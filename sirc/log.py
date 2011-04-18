from __future__ import with_statement
import datetime
import StringIO

import boto.s3
import ircloglib

import sirc.util.s3


def open_log(path, mode):
  if path.startswith('s3://'):
    return open_log_from_s3(path, mode)
  else:
    return open(path, mode)


def open_log_from_s3(path, mode):
  if 'r' in mode:
    return open_read_only_log_from_s3(path)
  else:
    return open_writeable_log_from_s3(path)


def open_read_only_log_from_s3(path):
  bucket, s3_path = sirc.util.s3.parse_s3_url(path)
  bucket = sirc.util.s3.get_s3_bucket(bucket)
  key = boto.s3.key.Key(bucket)
  key.key = s3_path
  log_data = sirc.log.metadata_from_s3path(s3_path)
  log_contents = key.get_contents_as_string()
  log_file = StringIO.StringIO(log_contents)
  return log_file


## def metadata_from_logpath(logpath):
##   path_pieces = [p for p in logpath.split('/') if len(p) > 0]
##   server_str = path_pieces[-4]
##   channel_str = path_pieces[-3]
##   year_str = path_pieces[-2]
##   date_piece = path_pieces[-1]
##   month_str = date_piece[3:5]
##   day_str = date_piece[6:8]
##   date = datetime.date(int(year_str), int(month_str), int(day_str))
##   return Metadata(path=logpath, server=server_str, channel=channel_str,
##                   date=date)


def metadata_from_s3path(s3path):
  path_pieces = [p for p in s3path.split('/') if len(p) > 0]
  server_str = path_pieces[-4]
  channel_str = path_pieces[-3]
  year_str = path_pieces[-2]
  date_piece = path_pieces[-1]
  month_str = date_piece[0:2]
  day_str = date_piece[3:5]
  date = datetime.date(int(year_str), int(month_str), int(day_str))
  return Metadata(path=s3path, server=server_str, channel=channel_str,
                  date=date)


S3_PATH_PREFIX = 's3://'


def parse_log_path(path):
  if path.startswith(S3_PATH_PREFIX):
    return metadata_from_s3path(path)
  else:
    with open(path, 'rb') as f:
      return ircloglib.parse_header(f.readline())


def encode_id(log_data, suffix=None):
  assert suffix is None or not ('/' in suffix)
  if isinstance(log_data, ircloglib.Metadata):
    key = '/'.join([log_data.server,
                    log_data.channel,
                    '%s' % (log_data.start_time.year,),
                    '%02d' % (log_data.start_time.month,),
                    '%02d' % (log_data.start_time.day,)])
    if not (suffix is None):
      key += ':' + suffix
    return key
  else:
    return encode_id(parse_log_path(log_data), suffix=suffix)


def decode_id(id):
  suffix = None
  pieces = id.split('/')
  if ':' in pieces[-1]:
    suffix_pieces = pieces[-1].split(':')
    assert len(suffix_pieces) == 2
    pieces[-1] = suffix_pieces[0]
    suffix = suffix_pieces[1]
  date = datetime.date(year=int(pieces[2]),
                       month=int(pieces[3]),
                       day=int(pieces[4]))
  m = Metadata(path=None, server=pieces[0], channel=pieces[1], date=date)
  return (m, suffix)


def browse_url_for_key(key):
  (m, suffix) = decode_id(key)
  if suffix:
    suffix = '#%s' % (suffix,)
  else:
    suffix = ''
  return '/browse/%s/%s/%02d/%02d%s' % (m.channel,
                                         m.date.year,
                                         m.date.month,
                                         m.date.day,
                                         suffix)
