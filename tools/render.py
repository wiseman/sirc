#!/usr/bin/env python
from __future__ import with_statement
import sys
import time
import logging

import sirc.logrender


def main(args):
  for path in args[1:]:
    with open(path, 'rb') as file:
      log_text = file.read()
      time_start = time.time()
      html = sirc.logrender.render_log(log_text)
      time_end = time.time()
      print html
      logging.info('Rendered %s in %s ms',
                   path,
                   int((time_end - time_start) * 1000))


if __name__ == '__main__':
  main(sys.argv)
