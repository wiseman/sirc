#!/usr/bin/env python

import os
import sys
import time

import boto

import sirc.util.s3
import sirc.log


def _error(msg):
  sys.stdout.flush()
  sys.stderr.write('%s\n' % (msg,))


def _usage():
  _error('Usage: %s <logpath> [<logpath>...]' % (sys.argv[0],))


############################################################
#
# ./upload.py ~/src/irc-logs/freenode/lisp/2010/10.01.01
#
############################################################

def main(argv):
  args = argv[1:]
  if len(args) == 0:
    _usage()
    sys.exit(1)

  credentials = sirc.util.s3.get_credentials()
  conn = boto.connect_s3(credentials.access_key, credentials.secret, debug=0)
  bucket_name = 'sirc'
  bucket = conn.create_bucket(bucket_name)

  for path in args:
    upload_log_file(bucket, path)


def upload_callback(bytes_sent, bytes_left):
  sys.stdout.write('.')
  sys.stdout.flush()


def upload_log_file(bucket, local_path):
  start_time = time.time()
  with open(local_path, 'rb') as f:
    log_data = sirc.log.metadata_from_logpath(local_path)
    remote_path = 'rawlogs/%s/%s/' % (log_data.channel, log_data.date.year)
    key = boto.s3.key.Key(bucket)
    key.key = os.path.join(remote_path, '%02d.%02d' % (log_data.date.month,
                                                       log_data.date.day))
    sys.stdout.write('%s -> s3://%s/%s: ' % (local_path, bucket.name, key.key))
    sys.stdout.flush()
    key.set_contents_from_file(f, cb=upload_callback, num_cb=10)
  end_time = time.time()
  file_size = os.stat(local_path).st_size
  sys.stdout.write(' %.1f KB/s\n' % \
                   ((file_size / 1024) / (end_time - start_time),))


if __name__ == '__main__':
  main(sys.argv)
