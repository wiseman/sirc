import string
import cgi
import StringIO
import re
import logging

import sirc.log
import sirc.util.urlfinder


MSG_LINE_TEMPLATE = string.Template(
  '<tr class="serprow">' +
  '<td class="serptime"><a id="$line_num">$time</a></td>' +
  '<td class="serpuser">' +
  '<span class="brack">&lt;</span>' +
  '<span>$user</span>' +
  '<span class="brack">&gt;</span>' +
  '</td>' + \
  '<td class="serptext">$message</td>' + \
  '</tr>\n')

UNKNOWN_LINE_TEMPLATE = string.Template(
  '<tr class="serprow">' + \
  '<td class="serptime"><a id="$line_num">$time</a></td>' + \
  '<td colspan="2" class="serptext">$line</td>' + \
  '</tr>\n')

g_line_renderers = {}


def register_line_renderer(regex, function):
  global g_line_renderers
  g_line_renderers[regex] = function


def render_line(line_num, line):
  global g_line_renderers
  for regex in g_line_renderers:
    #logging.info('Checking %s against %s', regex, line[0:20])
    match = regex.match(line)
    if match:
      logging.info(match.groups()[1])
      return g_line_renderers[regex](line_num, *match.groups())
  #logging.info('no match')
  return render_default_line(line_num, line)


DEFAULT_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+)(.*)', re.UNICODE)


def render_default_line(line_num, line):
  match = DEFAULT_RE.match(line)
  if not match:
    return '<tr><td></td><td colspan="2">Unknown line: %s</td></tr>\n' % \
           (cgi.escape(line),)
  else:
    time_str = match.groups()[0]
    remaining_str = match.groups()[1]
    return UNKNOWN_LINE_TEMPLATE.substitute(line_num='%05d' % (line_num,),
                                            time=time_str,
                                            line=cgi.escape(remaining_str))


def render_msg_line(line_num, timestamp, user, message):
  #logging.info('BINGO')
  time_str = cgi.escape(timestamp)
  user_str = cgi.escape(user)
  message_str = sirc.util.urlfinder.markup_urls(message)
  return MSG_LINE_TEMPLATE.substitute(line_num='%05d' % (line_num,),
                                      user=user_str,
                                      message=message_str,
                                      time=time_str)


register_line_renderer(
  regex=re.compile(r'([0-9]+:[0-9]+:[0-9]+) <([^>]+)> ?(.*)', re.UNICODE),
  function=render_msg_line)


LOG_PROLOGUE_TEMPLATE = """<!DOCTYPE HTML>
<html>
<head>
<meta http-equiv="Content-type" content="text/html;charset=UTF-8">
<title>$title</title>
<link href="/static/mainq.css" rel="stylesheet" type="text/css">
<script type="text/javascript">

  var _gaq = _gaq || [];
  _gaq.push(['_setAccount', 'UA-137136-5']);
  _gaq.push(['_trackPageview']);

  (function() {
    var ga = document.createElement('script');
    ga.type = 'text/javascript';
    ga.async = true;
    ga.src = ('https:' == document.location.protocol ? \
      'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
    var s = document.getElementsByTagName('script')[0];
    s.parentNode.insertBefore(ga, s);
  })();

</script>
</head>
<body>
<h1>$heading</h1>
<table>
"""


def render_log(log_data, text):
  date_str = '%02d-%02d-%02d' % (log_data.date.year,
                                 log_data.date.month,
                                 log_data.date.day)
  title = 'SIRC > %s > %s' % (log_data.channel, date_str)
  heading = '%s > %s' % (log_data.channel, date_str)
  in_buffer = StringIO.StringIO(text)
  out_buffer = StringIO.StringIO()
  prologue_template = string.Template(LOG_PROLOGUE_TEMPLATE)
  prologue = prologue_template.substitute({
      'title': cgi.escape(title),
      'heading': cgi.escape(heading)
      })
  out_buffer.write(prologue)
  for line_num, line in enumerate(in_buffer):
    out_buffer.write(render_line(line_num, line[0:-1]))
  out_buffer.write('</table>\n')
  out_buffer.write('</body>\n')
  out_buffer.write('</html>\n')
  return out_buffer.getvalue()
