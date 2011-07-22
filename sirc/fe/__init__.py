from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app


__all__ = ['index', 'irclog', 'logrender', 'tokenz', 'urlfinder', 'views']


# ------------------------------------------------------------
# Application URL routing.
# ------------------------------------------------------------

def make_application():
  import sirc.fe.views
  app = webapp.WSGIApplication(
      [('/', sirc.fe.views.Search),
       # Browse URLs can look like this:
       # http://sirc.com/browse/haskell/2011/07/11
       ('/browse/(.*)/(.*)/(.*)/(.*)', sirc.fe.views.BrowseDay),
       # Or like this:
       # http://sirc.com/browse/haskell
       ('/browse/(.*)', sirc.fe.views.BrowseChannel),
       ('/postactivitystats', sirc.fe.views.PostActivityStats),
       ],
      #debug=True
      )
  return app


def run_application():
  app = make_application()
  run_wsgi_app(app)
