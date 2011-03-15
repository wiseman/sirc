import string
import cgi

from sirc import log


LINE_TEMPLATE = '<tr>' + \
                '<td class="nick"><span class="brack">&lt;</span><span class="source">$user</span><span class="brack">&gt;</span></td>' + \
                '<td class="msg">$message</td>' + \
                '<td class="time">$time</td>' + \
                '</tr>\n'


def render_line(timestamp, user, message):
  time_str = '%02d:%02d:%02d' % (timestamp.hour, timestamp.minute, timestamp.second)
  user_str = cgi.escape(user)
  message_str = cgi.escape(message)
  template = string.Template(LINE_TEMPLATE)
  template.substitute(user=user_str, message=message_str, time=time_str)
  
def render_from_key(key):
  log_data = log.decode_id(key)
  s3path = 'rawlogs/%s/%s/%02d.%02d' % (log_data.channel,
                                        log_data.date.year,
                                        log_data.date.month,
                                        log_data.date.day)
  return s3path
