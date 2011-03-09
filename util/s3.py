import boto.s3.key

class Key(boto.s3.key.Key):
  def __init__(self, *args, **kwargs):
    boto.s3.key.Key.__init__(self, *args, **kwargs)

  def set_contents_from_filename(*args, **kwargs):
    self = args[0]
    self.key = translate_to_s3_path(self.path)
    print 'Setting content for key=%r' % (self.key,)
    print boto.s3.key.Key.set_contents_from_file(*args, **kwargs)



def translate_to_s3_path(logical_path):
  return logical_path
#  return logical_path.replace('/', ',folder/')

def translate_from_s3_path(s3_path):
  return s3_path
#  return s3_path.replace(',folder/', '/')

def split_s3_path(s3_path):
  path = translate_from_s3_path(s3_path)
  return [p for p in path.split('/') if len(p) > 0]


def mkdir(bucket, logical_path):
  pieces = [p for p in logical_path.split('/') if len(p) > 0]
  prefix = []
  for piece in pieces:
    k = boto.s3.key.Key(bucket)
    prefix += [piece,]
    path = '/'.join(prefix)
    k.key = translate_to_s3_path(path + '/')
    print 'mkdir on key=%r' % (k.key,)
    print k.set_contents_from_string('')


AWS_ACCESS_KEY_ID = '1P7JHG3GWH0R2596ZK82'
AWS_SECRET_ACCESS_KEY = '4u6iCa6UFc1Q2WfK3zIEhRdTV0ah94RSFJY9dASS'



class Credentials:
  def __init__(self, access_key, secret):
    self.access_key = access_key
    self.secret = secret
    
def get_credentials():
  with open('creds.txt', 'rb') as f:
    aws_access_key_id = f.readline()[:-1]
    aws_secret_access_key = f.readline()[:-1]
  return Credentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)


