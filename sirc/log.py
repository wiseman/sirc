import datetime


class Metadata():
  def __init__(self, path, server, channel, date):
    self.path = path
    self.server = server
    self.channel = channel
    self.date = date

  def __str__(self):
    return '<log.Metadata %s %s %r %s>' % (self.path,
                                           self.server,
                                           self.channel,
                                           self.date)


def metadata_from_logpath(logpath):
  path_pieces = [p for p in logpath.split('/') if len(p) > 0]
  server_str = path_pieces[-4]
  channel_str = path_pieces[-3]
  year_str = path_pieces[-2]
  date_piece = path_pieces[-1]
  century_str = date_piece[0:2]
  month_str = date_piece[3:5]
  day_str = date_piece[6:8]
  date = datetime.date(int(year_str), int(month_str), int(day_str))
  return Metadata(path=logpath, server=server_str, channel=channel_str,
                  date=date)


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
    return metadata_from_logpath(path)
  

def encode_id(log_data, suffix=None):
  if suffix is None:
    suffix = ''
  if isinstance(log_data, Metadata):
    return '/'.join([log_data.server,
                     log_data.channel,
                     '%s' % (log_data.date.year,),
                     '%02d' % (log_data.date.month,),
                     '%02d' % (log_data.date.day,),
                     suffix])
  else:
    return encode_id(parse_log_path(log_data), suffix=suffix)


def decode_id(id):
  pieces = id.split('/')
  date = datetime.date(year=int(pieces[2]),
                       month=int(pieces[3]),
                       day=int(pieces[4]))
  return Metadata(path=None, server=pieces[0], channel=pieces[1],
                  date=date)

