# sirc
#
# Copyright 2011 John Wiseman <jjwiseman@gmail.com>

from __future__ import with_statement
import os.path
import logging

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
from django.utils import simplejson
from google.appengine.ext import db
from google.appengine.api import users as gaeusers
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers

import index


# ------------------------------------------------------------
# Keep templates in the 'templates' subdirectory.
# ------------------------------------------------------------

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'templates')

def render_template(name, values={}):
  return template.render(os.path.join(TEMPLATE_PATH, name), values)


class UploadLog(blobstore_handlers.BlobstoreUploadHandler):
  def get(self):
    upload_url = blobstore.create_upload_url('/u')
    values = {'upload_url': upload_url}
    self.response.out.write(render_template('upload.html', values))

  def post(self):
    upload_files = self.get_uploads('file')
    blob_info = upload_files[0]
    index.start_indexing_log(blob_info)
    self.redirect('/mapreduce')
        

class Admin(webapp.RequestHandler):
  def get(self):
    self.response.out.write(render_template('admin.html'))

  def post(self):
    if len(self.request.get('delete-indices')) > 0:
      index.delete_indices()
    if len(self.request.get('delete-logs')) > 0:
      index.delete_logs()
    self.redirect('/a')


class Search(webapp.RequestHandler):
  def get(self):
    values = {}
    query = self.request.get('q')
    values['has_results'] = False
    if len(query) > 0:
      results = index.get_query_results(query)
      values['results'] = results
      values['has_results'] = True
    values['query'] = query
    logging.info('values=%s' % (values,))
    self.response.out.write(render_template('search.html', values))
      


# ------------------------------------------------------------
# Application URL routing.
# ------------------------------------------------------------

application = webapp.WSGIApplication([('/', Search),
                                      ('/u', UploadLog),
                                      ('/a', Admin),
                                      ('/indexing_did_finish', index.IndexingFinished)
                                      ]
                                     #debug=True
                                     )


def real_main():
  run_wsgi_app(application)


def profile_main():
    # This is the main function for profiling
    # We've renamed our original main() above to real_main()
    import cProfile, pstats
    prof = cProfile.Profile()
    prof = prof.runctx("real_main()", globals(), locals())
    print "<pre>"
    stats = pstats.Stats(prof)
    stats.sort_stats("time")  # Or cumulative
    stats.print_stats(80)  # 80 = how many to print
    # The rest is optional.
    # stats.print_callees()
    # stats.print_callers()
    print "</pre>"


main = real_main
    
if __name__ == "__main__":
  main()
