import calendar
import logging
import StringIO

from google.appengine.ext import db

from django.utils import simplejson


class ChannelStats(db.Model):
  stats_json = db.TextProperty()


def set_statistics(stats):
  logging.info('%s' % (stats.keys(),))
  assert 'channel_stats' in stats
  assert 'activity_counts' in stats
  record = ChannelStats(key_name='channel_activity_stats',
                        stats_json=simplejson.dumps(stats))
  record.put()


def get_statistics():
  key = db.Key.from_path('ChannelStats', 'channel_activity_stats')
  record = db.get(key)
  return simplejson.loads(record.stats_json)


NUM_DAYS_IN_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def num_days_in_month(year, month):
  if calendar.isleap(year) and month == 2:
    return NUM_DAYS_IN_MONTH[month] + 1
  else:
    return NUM_DAYS_IN_MONTH[month]


def get_all_channels_statistics():
  stats = get_statistics()
  server_channels = []
  for key in stats['channel_stats']:
    server, channel = key.split(':')
    server_channels.append({'server': server,
                            'channel': channel})
  return server_channels


def get_channel_browse_html(server, channel, stats):
  channel_stats = stats['channel_stats']['%s:%s' % (server, channel)]
  activity_counts = stats['activity_counts']
  out = StringIO.StringIO()
  out.write('<h1>%s/%s</h1>\n' % (server, channel))
  years = [int(y) for y in channel_stats.keys()]
  for year in sorted(years, reverse=True):
    year_str = str(year)
    out.write('<h2><a name="%s">%s</a></h2>\n' % (year_str, year_str))
    out.write('<table>\n')
    months = [int(m) for m in channel_stats[year_str].keys()]
    for month in sorted(months, reverse=True):
      month_str = str(month)
      out.write('<tr><td><big><a name="%s-%s">%s</a></big></td>' %
                (year_str, month_str, calendar.month_name[month],))
      for day in range(num_days_in_month(year, month)):
        day += 1
        day_str = str(day)
        if day_str in channel_stats[year_str][month_str]:
          count = channel_stats[year_str][month_str][day_str]
          css_class = activity_css_class(count, activity_counts)
          # e.g. /browse/haskell/2011/07/22
          target_url = '/browse/%s/%s/%s/%s' % (
            channel, year_str, month_str, day_str)
          out.write('<td><a href="%s">' % (target_url,))
          out.write('<span class="act %s">%02d</span></a></td>' % (
              css_class, day))
        else:
          out.write('<td align="center">--</td>')
      out.write('</tr>\n')
    out.write('</table>\n')
  return out.getvalue()


# We assign one of five CSS classes to a channel-day of activity based
# on what percentile of overall activity it reaches.  The minimum
# percentile for each class:
#
#  Extra low:   0th percentile
#  Low:        10th percentile
#  Medium:     25th percentile
#  High:       75th percentile
#  Extra high: 90th percentile

ACTIVITY_CLASSES = (('xlo', 0.0),
                    ('lo', 0.1),
                    ('med', 0.25),
                    ('hi', 0.75),
                    ('xhi', 0.9))


def activity_css_class(count, distribution):
  activity_css = ACTIVITY_CLASSES[0][0]
  for i in range(len(ACTIVITY_CLASSES)):
    break_val = distribution[int(ACTIVITY_CLASSES[i][1] * len(distribution))]
    if count > break_val:
      activity_css = ACTIVITY_CLASSES[i][0]
    else:
      break
  return activity_css
