from google.appengine.ext import db
from google.appengine.ext import blobstore


class DayLog(db.Model):
  blob = blobstore.BlobReferenceProperty(required=True)
  channel = db.StringProperty(required=True)
  date = db.DateProperty(required=True)
  md5 = db.StringProperty()
