import calendar
import StringIO
import sys

import ircloglib

import sirc.browse


# {(server, channel):
#    {year:
#        {month #: {day: # msgs}}}}

def get_channels_stats(log_dir):
  activity_counts = []
  stats = {}
  for log_data in ircloglib.all_logs(log_dir):
    #print log_data.path
    channel_stats = stats.get((log_data.server, log_data.channel), {})
    year_stats = channel_stats.get(log_data.start_time.year, {})
    month_stats = year_stats.get(log_data.start_time.month, {})
    activity_count = get_activity_count(log_data.path)
    month_stats[log_data.start_time.day] = activity_count
    activity_counts.append(activity_count)
    year_stats[log_data.start_time.month] = month_stats
    channel_stats[log_data.start_time.year] = year_stats
    stats[(log_data.server, log_data.channel)] = channel_stats
  return stats, sorted(activity_counts)




def get_activity_count(path):
  with open(path, 'rb') as f:
    return len(f.readlines())


if __name__ == '__main__':
  print """
<html>
<head>
<style type="text/css">
  .activity-xlow   { background-color: rgb(  0,   0, 255); }
  .activity-low    { background-color: rgb(  0, 102, 255); }
  .activity-medium { background-color: rgb(  0, 255, 255); }
  .activity-high   { background-color: rgb(255, 255,   0); }
  .activity-xhigh  { background-color: rgb(255, 101,   0); }
</style>
<title>Browsing</title>
</head>
<body>
"""
  stats, activity_counts = get_channels_stats(sys.argv[1])
  print '<!-- %s -->' % (activity_counts,)
  for server, channel in sorted(stats.keys()):
    print sirc.browse.get_channel_browse_html(server, channel, stats[(server, channel)], activity_counts)
  print "</body></html>"
