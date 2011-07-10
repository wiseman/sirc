import calendar
import StringIO


NUM_DAYS_BY_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def num_days_in_month(year, month):
  if calendar.isleap(year) and month == 2:
    return NUM_DAYS_IN_MONTH[month] + 1
  else:
    return NUM_DAYS_IN_MONTH[month]

def get_channel_browse_html(server, channel, channel_stats, activity_counts):
  out = StringIO.StringIO()
  out.write('<h1>%s/%s</h1>\n' % (server, channel))
  for year in sorted(channel_stats.keys(), reverse=True):
    out.write('<h2>%s</h2>\n' % (year,))
    out.write('<table>\n')
    for month in sorted(channel_stats[year].keys(), reverse=True):
      out.write('<tr><td><big>%s</big></td>' % (calendar.month_name[month],))
      for day in range(NUM_DAYS_BY_MONTH[month]):
        day += 1
        if day in channel_stats[year][month]:
          count = channel_stats[year][month][day]
          css_class = activity_css_class(count, activity_counts)
          out.write('<td><span class="act %s">%02d</span></td>' % (css_class, day))
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
                    ('lo',  0.1),
                    ('med', 0.25),
                    ('hi',  0.75),
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
  
