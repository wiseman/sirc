#!/usr/bin/env python

import os
import sys

import boto
from boto.s3.connection import S3Connection

import sirc.util.s3
import sirc.log


############################################################
#
# ./upload.py ~/src/irc-logs/freenode/lisp/2010/10.01.01
#
############################################################

def main(argv):
  args = argv[1:]
  credentials = sirc.util.s3.get_credentials()
  conn = S3Connection(credentials.access_key, credentials.secret, debug=0)
  bucket_name = 'sirc'
  bucket = conn.create_bucket(bucket_name)

  for path in args:
    upload_log_file(bucket, path)


def upload_callback(bytes_sent, bytes_left):
  sys.stdout.write('.')
  sys.stdout.flush()

def upload_log_file(bucket, local_path):
  with open(local_path, 'rb') as f:
    log_data = sirc.log.metadata_from_logpath(local_path)
    remote_path='rawlogs/%s/%s/' % (log_data.channel, log_data.date.year)
    key = boto.s3.key.Key(bucket)
    key.key = os.path.join(remote_path, '%02d.%02d' % (log_data.date.month, log_data.date.day))
    sys.stdout.write('%s -> s3://%s/%s: ' % (local_path, bucket.name, key.key))
    sys.stdout.flush()
    key.set_contents_from_file(f, cb=upload_callback, num_cb=10)
  sys.stdout.write('\n')

    
if __name__ == '__main__':
  main(sys.argv)
