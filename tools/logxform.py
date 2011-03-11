import sys
import re
from string import Template
import cgi


TEMPLATE = Template('''
<doc>
  <field name="id">$id</field>
  <field name="channel">$channel</field>
  <field name="timestamp">$timestamp</field>
  <field name="user">$user</field>
  <field name="text">$message</field>
  <field name="position">$position</field>
</doc>
''')


LOG_TIMESTAMP_HEADER_RE = re.compile(r'.*log: started (.+)/([0-9\.]+)')
LOG_LINE_RE = re.compile(r'([0-9]+:[0-9]+:[0-9]+) <(\w+)> ?(.*)', re.UNICODE)


def parse_log_line(line):
  match = LOG_LINE_RE.match(line)
  if match:
    # timestamp, who, text
    return match.groups()


g_id_prefix = ''
g_id_counter = 0

def next_id():
  global g_id_counter, g_id_prefix
  g_id_counter += 1
  return '%s:%s' % (g_id_prefix, g_id_counter)

g_channel = None
g_date_str = None

 
def xform_line(line, position):
    global g_channel, g_date_str
    result = parse_log_line(line)
    if result:
        timestamp, who, message = result
        who = cgi.escape(recode(who))
        message = recode(message)
        if not has_ctrl(message):
          message = cgi.escape(message)
          timestamp = '%sT%sZ' % (g_date_str, timestamp,)
          return TEMPLATE.substitute(id=next_id(),
                                     channel=g_channel,
                                     timestamp=timestamp,
                                     user=who,
                                     message=message,
                                     position=position)


def check_high_bit(path, text):
  for char in text:
    if ord(char) == 24:
      raise Exception('Got char 24 in %s: %r' % (path, text))

def has_ctrl(text):
  for char in text:
    if ord(char) < 32:
      return True
  return False


def xform_file(path):
  global g_channel, g_date_str, g_id_prefix
  with open(path, 'rb') as f:
    # Extract the channel name and date from the first line of the
    # log.
    first_line = f.readline()
    match = LOG_TIMESTAMP_HEADER_RE.match(first_line)
    if not match:
      raise Exception('Unable to parse log header %s: %r' % (path, first_line))
    g_channel = match.groups()[0]
    date_str = match.groups()[1]
    g_id_prefix = '%s/%s' % (g_channel, date_str)
    year = 2000 + int(date_str[0:2])
    month = int(date_str[3:5])
    day = int(date_str[6:8])
    g_date_str = '%s-%02d-%02d' % (year, month, day)

    position = f.tell()
    line = f.readline()
    while line != '':
      xformed = xform_line(line, position)
      position = f.tell()
      line = f.readline()
      if xformed:
        print xformed

  
  

def recode(text):
  recoded_text = unicode(text, 'cp1252', 'replace')
  recoded_text = recoded_text.encode('ascii', 'xmlcharrefreplace')
  return recoded_text


if __name__ == '__main__':
    print '<add>'
    for file in sys.argv[1:]:
      xform_file(file)
    print '</add>'


