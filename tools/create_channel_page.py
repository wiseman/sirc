import sys

import ircloglib


# { (server, channel):
#    { year:
#        {month #: ((day, # msgs), ...)}}}

def get_channels_stats(log_dir):
  stats = {}
  for log_data in ircloglib.all_logs(log_dir):
    #print log_data.path
    channel_stats = stats.get((log_data.server, log_data.channel), {})
    year_stats = channel_stats.get(log_data.start_time.year, {})
    month_stats = year_stats.get(log_data.start_time.month, [])
    activity_count = get_activity_count(log_data.path)
    day_stats = (log_data.start_time.day, activity_count)
    month_stats.append(day_stats)
    year_stats[log_data.start_time.month] = month_stats
    channel_stats[log_data.start_time.year] = year_stats
    stats[(log_data.server, log_data.channel)] = channel_stats
  return stats


def get_activity_count(path):
  with open(path, 'rb') as f:
    return len(f.readlines())

if __name__ == '__main__':
  get_channels_stats(sys.argv[1])
