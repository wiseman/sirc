import datetime


class Metadata():
  def __init__(self, channel, date):
    self.channel = channel
    self.date = date
               
def metadata_from_logpath(logpath):
  path_pieces = [p for p in logpath.split('/') if len(p) > 0]
  channel_str = path_pieces[-3]
  year_str = path_pieces[-2]
  date_piece = path_pieces[-1]
  century_str = date_piece[0:2]
  month_str = date_piece[3:5]
  day_str = date_piece[6:8]
  
  date = datetime.date(int(year_str), int(month_str), int(day_str))
  return Metadata(channel=channel_str, date=date)

def metadata_from_s3path(logpath):
  path_pieces = [p for p in s3.split('/') if len(p) > 0]
  channel_str = path_pieces[-3]
  year_str = path_pieces[-2]
  date_piece = path_pieces[-1]
  century_str = date_piece[0:2]
  month_str = date_piece[3:5]
  day_str = date_piece[6:8]
  
  date = datetime.date(int(year_str), int(month_str), int(day_str))
  return Metadata(channel=channel_str, date=date)

