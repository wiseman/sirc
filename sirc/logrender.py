import string
import cgi
import StringIO
import re

import sirc.log


MSG_LINE_TEMPLATE = string.Template(
  '<tr>' +
  '<td class="time"><a name="$line_num">$time</a></td>' +
  '<td class="nick">' +
  '<span class="brack">&lt;</span>' +
  '<span class="source">$user</span>' +
  '<span class="brack">&gt;</span>' +
  '</td>' + \
  '<td class="msg">$message</td>' + \
  '</tr>\n')

UNKNOWN_LINE_TEMPLATE = string.Template(
  '<tr>' + \
  '<td class="time"><a name="$line_num">$time</a></td>' + \
  '<td colspan="2" class="unknown">$line</td>' + \
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
  message_str = cgi.escape(message)
  return MSG_LINE_TEMPLATE.substitute(line_num='%05d' % (line_num,),
                                      user=user_str,
                                      message=message_str,
                                      time=time_str)


register_line_renderer(
  regex=re.compile(r'([0-9]+:[0-9]+:[0-9]+) <(\w+)> ?(.*)', re.UNICODE),
  function=render_msg_line)


def render_log(text):
  in_buffer = StringIO.StringIO(text)
  out_buffer = StringIO.StringIO()
  out_buffer.write('<html>\n')
  out_buffer.write('<head>\n')
  out_buffer.write('<title>woo</title>\n')
  out_buffer.write('</head>\n')
  out_buffer.write('<body>\n')
  out_buffer.write('<table>\n')
  for line_num, line in enumerate(in_buffer):
    out_buffer.write(render_line(line_num, line[0:-1]))
  out_buffer.write('</table>\n')
  out_buffer.write('</body>\n')
  out_buffer.write('</html>\n')
  return out_buffer.getvalue()
