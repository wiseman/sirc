#!/usr/bin/env python
from __future__ import with_statement
import optparse
import os
import sys
import time

import boto

import sirc.util.s3
import sirc.log


def _error(msg):
  sys.stdout.flush()
  sys.stderr.write('%s\n' % (msg,))


############################################################
#
# ./upload.py ~/src/irc-logs/freenode/lisp/2010/10.01.01
#
############################################################

def main(argv):
  parser = optparse.OptionParser(
    usage='usage: %prog [options] [<log source>...]')
  parser.add_option(
    '-f',
    '--force',
    dest='force',
    action='store_true',
    default=False,
    help='Uploads the file even if it already exists (default is %default).')
  (options, args) = parser.parse_args()
  if len(args) < 1:
    parser.print_usage()
    return 1

  credentials = sirc.util.s3.get_credentials()
  conn = boto.connect_s3(credentials.access_key, credentials.secret, debug=0)
  bucket_name = 'sirc'
  bucket = conn.create_bucket(bucket_name)

  for path in args:
      upload_log_file(bucket, path, force=options.force)


def upload_callback(bytes_sent, bytes_left):
  sys.stdout.write('.')
  sys.stdout.flush()


def upload_log_file(bucket, local_path, force=False):
  start_time = time.time()
  with open(local_path, 'rb') as f:
    log_data = sirc.log.metadata_from_logpath(local_path)
    remote_path = 'rawlogs/%s/%s/%02d.%02d' % (log_data.channel,
                                               log_data.date.year,
                                               log_data.date.month,
                                               log_data.date.day)
    if not (force or
            len(sirc.util.s3.cached_glob_s3_path(bucket, remote_path)) == 0):
      print 'Skipping %s' % (local_path,)
      return

    key = boto.s3.key.Key(bucket)
    key.key = remote_path
    sys.stdout.write('%s -> s3://%s/%s: ' % (local_path, bucket.name, key.key))
    sys.stdout.flush()
    key.set_contents_from_file(f, cb=upload_callback, num_cb=10)
  end_time = time.time()
  file_size = os.stat(local_path).st_size
  sys.stdout.write(' %.1f KB/s\n' % \
                   ((file_size / 1024) / (end_time - start_time),))


if __name__ == '__main__':
  main(sys.argv)
