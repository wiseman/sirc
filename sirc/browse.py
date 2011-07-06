import calendar
import StringIO


NUM_DAYS_BY_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


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
          out.write('<td><span class="%s">%02d</span></td>' % (css_class, day))
        else:
          out.write('<td align="center">--</td>')
      out.write('</tr>\n')
    out.write('</table>\n')
  return out.getvalue()



ACTIVITY_CLASSES = (('activity-xlow', 0.0),
                    ('activity-low', 0.1),
                    ('activity-medium', 0.25),
                    ('activity-high', 0.75),
                    ('activity-xhigh', 0.9))


def activity_css_class(count, distribution):
  #print '<!-- *** -->'
  activity_css = ACTIVITY_CLASSES[0][0]
  for i in range(len(ACTIVITY_CLASSES)):
    break_val = distribution[int(ACTIVITY_CLASSES[i][1] * len(distribution))]
    #print '<!-- %s, %s -->' % (count, break_val)
    if count > break_val:
      activity_css = ACTIVITY_CLASSES[i][0]
    else:
      break
  return activity_css
  
