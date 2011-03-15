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
       ('/browse/(.*)/(.*)/(.*)/(.*)', sirc.fe.views.Browse),
       ],
      #debug=True
      )
  return app


def run_application():
  app = make_application()
  run_wsgi_app(app)
