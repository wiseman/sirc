# Copyright 2012 John Wiseman
# jjwiseman@gmail.com


# Install the Appstats event recorder to record performance stats (see
# https://developers.google.com/appengine/docs/python/tools/appstats)

def webapp_add_wsgi_middleware(app):
  from google.appengine.ext.appstats import recording
  app = recording.appstats_wsgi_middleware(app)
  return app

appstats_MAX_STACK = 100
