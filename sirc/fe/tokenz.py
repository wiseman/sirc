from __future__ import with_statement
import sys
import re
import chardet


TOKEN_RE = re.compile('_|\W+', re.UNICODE)


class EncodingError(Exception):
  pass


def extract_text_tokens(text):
  words = TOKEN_RE.split(text)
  words = [w.lower() for w in set(words) if len(w) > 0]
  return words


def recode(text):
  recoded_text = None
  try:
    recoded_text = unicode(text, 'cp1252')
  except UnicodeDecodeError:
    pass
  if not recoded_text:
    guess = chardet.detect(text)
    if guess and 'encoding' in guess and guess['encoding']:
      try:
        recoded_text = unicode(text, guess['encoding'])
      except UnicodeDecodeError, e:
        raise EncodingError('Unable to encode text %r as cp1252 or %s: %s' % \
                            (text, guess, e))
    else:
      raise EncodingError('Unable to encode text %r as cp1252, ' +
                          'unable to guess encoding.' % (text,))
  return recoded_text


def main(argv):
  print sys.getdefaultencoding()
  for filename in argv:
    with open(filename, 'rb') as f:
      for line in f.xreadlines():
        high_char = False
        for char in line:
          if ord(char) > 127:
            high_char = True
        recoded_text = recode(line)
        terms = extract_text_tokens(recoded_text)
        if '_' in line:
          print line
          print terms
#        if high_char:
#          print '\n' + line
#          for term in terms:
#            sys.stdout.write('%s ' % (term,))


if __name__ == '__main__':
  main(sys.argv)
