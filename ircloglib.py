from __future__ import with_statement
import datetime
import glob
import os
import os.path
import simplejson as json

import iso8601


class Metadata():
  def __init__(self, server=None, channel=None, start_time=None, path=''):
    self.path = path
    self.server = server
    self.channel = channel
    self.start_time = start_time

  def __str__(self):
    return '<ircloglib.Metadata path=%s server=%s channel=%r start_time=%s>' %  \
        (self.path,
         self.server,
         self.channel,
         self.start_time)


def filename_from_metadata(m):
  return filename_from_date(m.start_time)


def filename_from_date(date):
  return '%04d-%02d-%02d' % (date.year,
                             date.month,
                             date.day)


def path_from_metadata(m):
  return os.path.join(m.server,
                      m.channel,
                      str(m.start_time.year),
                      filename_from_metadata(m))


def date_from_path(path):
  filename = os.path.basename(path)
  pieces = filename.split('-')
  year_str = pieces[0]
  month_str = pieces[1]
  day_str = pieces[2]
  date = datetime.date(year=int(year_str),
                       month=int(month_str),
                       day=int(day_str))
  return date


def format_header(metadata):
  time_str = metadata.start_time.isoformat()
  d = {'server': metadata.server,
       'channel': metadata.channel,
       'start_time': time_str}
  return '00:00:00 --- log: %s' % (json.dumps(d),)


def parse_header(header):
  json_m = header.find('{')
  dict = json.loads(header[json_m:])
  start_time_str = dict['start_time']
  start_time = iso8601.parse_date(start_time_str)
  m = Metadata(server=dict['server'],
               channel=dict['channel'],
               start_time=start_time)
  return m


INFO = 'INFO'
ACTION = 'ACTION'
MSG = 'MSG'


class ParsingError(Exception):
  pass


def parse_line(line):
  #t_str = line[0:8]
  offset = line[0:8] #int(t_str[0:2]) * 3600 +  int(t_str[3:5]) * 60 + int(t_str[6:8])
  rest = line[9:]
  if len(rest) == 0:
    raise ParsingError('Unable to parse empty line: %s' % (line,))
  if rest[0] == '<':
    user_end = rest.find('>', 1)
    return (MSG, offset, rest[1:user_end], rest[user_end + 1:])
  elif rest[0] == '-':
    return (INFO, offset, rest)
  elif rest[0] == '*':
    user_end = rest.find(' ', 1)
    if user_end == -1:
      return [ACTION, offset, rest[1:], '']
    else:
      return [ACTION, offset, rest[1:user_end], rest[user_end+1:]]
  else:
    raise ParsingError('Unable to parse this line: "%s" (%s)' % (line, rest[1]))


def all_logs(dir):
  for server_dir in sorted(glob.glob(os.path.join(dir, '*'))):
    for channel_dir in sorted(glob.glob(os.path.join(server_dir, '*'))):
      for year_dir in sorted(glob.glob(os.path.join(channel_dir, '[0-9]*'))):
        for path in sorted(glob.glob(os.path.join(year_dir, '[0-9]*'))):
          with open(path, 'rb') as f:
            header = f.readline()
          metadata = parse_header(header)
          metadata.path = path
          yield metadata


def missing_logs(dir):
  one_day = datetime.timedelta(days=1)
  today = datetime.date.today()
  for server_dir in sorted(glob.glob(os.path.join(dir, '*'))):
    for channel_dir in sorted(glob.glob(os.path.join(server_dir, '*'))):
      for year_dir in sorted(glob.glob(os.path.join(channel_dir, '[0-9]*'))):
        year = int(os.path.basename(year_dir))
        date = datetime.date(year=year, month=1, day=1)
        while (date.year == year and date <= today):
          filename = filename_from_date(date)
          path = os.path.join(year_dir, filename)
          if not os.path.exists(path):
            m = Metadata(server=os.path.basename(server_dir),
                         channel=os.path.basename(channel_dir),
                         start_time=date,
                         path=path)
            yield m
          date += one_day
