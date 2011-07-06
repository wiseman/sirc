import StringIO
import sys

import ircloglib



NUM_DAYS_BY_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


# { (server, channel):
#    { year:
#        {month #: ((day, # msgs), ...)}}}

def get_channels_stats(log_dir):
  stats = {}
  for log_data in ircloglib.all_logs(log_dir):
    #print log_data.path
    channel_stats = stats.get((log_data.server, log_data.channel), {})
    year_stats = channel_stats.get(log_data.start_time.year, {})
    month_stats = year_stats.get(log_data.start_time.month, {})
    activity_count = get_activity_count(log_data.path)
    month_stats[log_data.start_time.day] = activity_count
    year_stats[log_data.start_time.month] = month_stats
    channel_stats[log_data.start_time.year] = year_stats
    stats[(log_data.server, log_data.channel)] = channel_stats
  return stats

def create_channel_page(server, channel, channel_stats):
  out = StringIO.StringIO()
  out.write('<h1>%s/%s</h1>\n' % (server, channel))
  for year in sorted(channel_stats.keys()):
    out.write('<h2>%s</h2>\n' % (year,))
    out.write('<table>\n')
    for month in sorted(channel_stats[year].keys()):
      out.write('<tr><td><big>%s<big></td>' % (month,))
      for day in range(NUM_DAYS_BY_MONTH[month]):
        day += 1
        if day in channel_stats[year][month]:
          out.write('<td align="center">%s</td>' % (day,))
        else:
          out.write('<td align="center">--</td>')
      out.write('</tr>\n')
    out.write('</table>\n')
  return out.getvalue()

def get_activity_count(path):
  with open(path, 'rb') as f:
    return len(f.readlines())

if __name__ == '__main__':
  stats = get_channels_stats(sys.argv[1])
  for server, channel in sorted(stats.keys()):
    print create_channel_page(server, channel, stats[(server, channel)])
