import string

g_url_prefixes = ["http", "ftp", "https", "telnet", "gopher", "file"]


def find_url_start(text, start):
  """Returns the start index of the first URL found in the specified
  string (starting at the specified index, which defaults to 0).  If
  no URL is found, this function returns -1.
  """
  for prefix in g_url_prefixes:
    extra_prefix = "%s://" % (prefix,)
    url_start = string.find(text, extra_prefix, start)
    if url_start > -1:
      return url_start
  return -1


def find_url_end(text, start):
  """Given a string and the starting position of a URL, returns the
  index of the first non-URL character.
  """
  for i in range(start, len(text)):
    if text[i] == ">" or text[i] in string.whitespace:
      return i
  return len(text)


def find_url(text, start=0):
  url_start = find_url_start(text, start)
  #print "start: %s" % (URLStart,)
  if url_start > -1:
    url_end = find_url_end(text, url_start)
    #print "end: %s" % (URLEnd,)
    if (url_end > -1):
      return (url_start, url_end)
  return None


def find_urls(text, start=0):
  url_indices = []
  indices = find_url(text, start)
  while (indices != None and start < len(text)):
    url_indices.append(indices)
    start = indices[1]
    indices = find_url(text, start)
  return url_indices


def run_tests():
  import pprint
  s = """11:11:10 I have just read about google art project ( """ + \
      """http://www.googleartproject.com/ ) and the first thing that """ + \
      """ has come to my mind is Paul Graham's failed startup called """ + \
      """Artix ( http://www.paulgraham.com/bronze.html ) :)"""
  url_spans = find_urls(s)
  pprint.pprint(find_urls(s))
  for span in url_spans:
    print '%r' % (s[span[0]:span[1]],)


if __name__ == '__main__':
    run_tests()
