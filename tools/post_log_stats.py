import httplib
import json
import sys
import urllib

import ircloglib


# {(server, channel):
#    {year:
#        {month #: {day: # msgs}}}}

def get_channels_stats(log_dir):
  activity_counts = []
  stats = {}
  for log_data in ircloglib.all_logs(log_dir):
    sc_key = '%s:%s' % (log_data.server, log_data.channel)
    channel_stats = stats.get(sc_key, {})
    year_stats = channel_stats.get(str(log_data.start_time.year), {})
    month_stats = year_stats.get(str(log_data.start_time.month), {})
    activity_count = get_activity_count(log_data.path)
    month_stats[str(log_data.start_time.day)] = activity_count
    activity_counts.append(activity_count)
    year_stats[str(log_data.start_time.month)] = month_stats
    channel_stats[str(log_data.start_time.year)] = year_stats
    stats[sc_key] = channel_stats
  return {'channel_stats': stats,
          'activity_counts': sorted(activity_counts)}


def get_activity_count(path):
  with open(path, 'rb') as f:
    return len(f.readlines())


def main(argv):
  log_dir = argv[1]
  stats = get_channels_stats(log_dir)
  stats_json = json.dumps(stats)
  params = urllib.urlencode({'stats': stats_json})
  headers = {'Content-type': 'application/x-www-form-urlencoded',
             'Accept': 'text/plain'}
  conn = httplib.HTTPConnection('heavymetalab.appspot.com')
  conn.request('POST', '/postactivitystats', params, headers)
  response = conn.getresponse()
  print response.status, response.reason
  data = response.read()
  print data
  conn.close()


if __name__ == '__main__':
  main(sys.argv)

