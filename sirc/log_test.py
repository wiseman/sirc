import unittest
import datetime

import log


class LogTest(unittest.TestCase):
  def test_metadata(self):
    path = '/~mrshrimp/freenode/lisp/2011/11.01.01'
    metadata = log.metadata_from_logpath(path)
    self.assertEqual(metadata.path, '/~mrshrimp/freenode/lisp/2011/11.01.01')
    self.assertEqual(metadata.server, 'freenode')
    self.assertEqual(metadata.channel, 'lisp')
    self.assertEqual(metadata.date, datetime.date(2011, 1, 1))

    path = 's3://freenode/lisp/2011/02.24'
    metadata = log.metadata_from_s3path(path)
    self.assertEqual(metadata.path, 's3://freenode/lisp/2011/02.24')
    self.assertEqual(metadata.server, 'freenode')
    self.assertEqual(metadata.channel, 'lisp')
    self.assertEqual(metadata.date, datetime.date(2011, 2, 24))

  def test_ids(self):
    path = '/~mrshrimp/freenode/lisp/2011/11.01.01'
    metadata = log.metadata_from_logpath(path)
    id1 = log.encode_id(path)
    id2 = log.encode_id(metadata)
    self.assertEqual(id1, 'freenode/lisp/2011/01/01/')
    self.assertEqual(id2, 'freenode/lisp/2011/01/01/')
    m1 = log.decode_id(id1)
    self.assertEqual(m1.server, metadata.server)
    self.assertEqual(m1.channel, metadata.channel)
    self.assertEqual(m1.date, metadata.date)
    
    
if __name__ == '__main__':
  unittest.main()

