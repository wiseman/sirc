# sirc
#
# Copyright 2011 John Wiseman <jjwiseman@gmail.com>

from __future__ import with_statement
import os.path
import logging
import collections
import hashlib
import urllib
import cgi
import string

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
from django.utils import simplejson
from google.appengine.ext import db
from google.appengine.api import users as gaeusers
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers

import sirc.fe.index
import sirc.fe.urlfinder

# ------------------------------------------------------------
# Keep templates in the 'templates' subdirectory.
# ------------------------------------------------------------

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'templates')

def render_template(name, values={}):
  return template.render(os.path.join(TEMPLATE_PATH, name), values)


class UploadLog(blobstore_handlers.BlobstoreUploadHandler):
  def get(self):
    upload_url = blobstore.create_upload_url('/uuuuu')
    values = {'upload_url': upload_url}
    self.response.out.write(render_template('upload.html', values))

  def post(self):
    upload_files = self.get_uploads('file')
    blob_info = upload_files[0]

    hash = blob_hash(blob_info)
    logging.info('hash=%s' % (hash,))
    previous_logs = sirc.fe.index.DayLog.all().filter('md5 = ',hash).fetch(5)
    if len(previous_logs) > 0:
      blob_info.delete()
      logging.error('md5 collision.')
      self.redirect('/a')
    else:
      logging.info('Starting indexing of %s' % (blob_info.key(),))
      sirc.fe.index.start_indexing_log(blob_info)
      self.redirect('/mapreduce')


class Admin(webapp.RequestHandler):
  def get(self):
    self.response.out.write(render_template('admin.html'))

  def post(self):
    if len(self.request.get('delete-indices')) > 0:
      sirc.fe.index.delete_indices()
    if len(self.request.get('delete-logs')) > 0:
      sirc.fe.index.delete_logs()
    self.redirect('/a')

class AddMD5(webapp.RequestHandler):
  def post(self):
    import hashlib
    logs = []
    for blob_info in blobstore.BlobInfo.all():
      log = sirc.fe.index.DayLog.all().filter('blob = ', blob_info.key()).fetch(1)[0]
      log.md5 = blob_hash(blob_info)
      logs.append(log)
      logging.info('Hashing log %s' % (log.key(),))
    logging.info('Committing %s.' % (len(logs),))
    db.put(logs)
    self.redirect('/a')

def blob_hash(blob_info):
  m = hashlib.md5()
  reader = blobstore.BlobReader(blob_info)
  try:
    m.update(reader.read())
    return m.hexdigest()
  finally:
    reader.close()



PAGE_SIZE = 20

class Search(webapp.RequestHandler):
  def get(self):
    values = {}
    query = self.request.get('q')
    start = self.request.get('start')

    if start and len(start) > 0:
      try:
        start = int(start)
      except:
        logging.error('Unable to parse start=%r' % (start,))
        start = 0
    else:
      start = 0
      
    values['css_file'] = 'main.css'
    values['has_results'] = False
    if len(query) > 0:
      values['query'] = query
      values['css_file'] = 'mainq.css'
      logging.error(sirc.__dict__)
      response = sirc.fe.index.get_query_results(query, start, PAGE_SIZE)
      records = response['docs']
      if len(records) > 0:
        results = prepare_results_for_display(records)
        paging_html = create_pagination_html(self.request.path, query, start, response['numFound'])
        result_html = render_template('serp.html', {'start': start + 1,
                                                    'end': start + len(results),
                                                    'total': response['numFound'],
                                                    'results': results,
                                                    'total_time': '%s' % (int(response['query_time'] * 1000),),
                                                    'query_time': '%s' % (response['QTime'],),
                                                    'pagination_html': paging_html})
        values['result_html'] = result_html
        values['has_results'] = True

    self.response.out.write(render_template('search.html', values))

def create_pagination_html(url, query, start, total):
  start = int(start / PAGE_SIZE) * PAGE_SIZE
  base_url = '%s?q=%s' % (url, urllib.quote_plus(query))
  def make_url(start):
    return '%s&start=%s' % (base_url, start)
  
  pages = []
  for offset in range(0, total, PAGE_SIZE):
    pages.append('<a href="%s">%s</a> ' % (make_url(offset), (offset / PAGE_SIZE) + 1))

  page_str = '<div class="pagination">'
  
  num_pages = total / PAGE_SIZE

  previous_next_nav_html = ''
  if num_pages > 1:
    if start >= PAGE_SIZE:
      previous_next_nav_html += '<a href="%s">Previous</a> ' % (make_url(start - PAGE_SIZE),)
    if start < total - PAGE_SIZE:
      previous_next_nav_html += '<a href="%s">Next</a> ' % (make_url(start + PAGE_SIZE),)
  if len(previous_next_nav_html) > 0:
    page_str += '<div>' + previous_next_nav_html + '</div>'

  if num_pages > 1:
    if start >= PAGE_SIZE:
      page_str += '<a href="%s">First</a> ' % (make_url(0),)
  
  window_width = min(total / PAGE_SIZE, 14)
  if window_width % 2 == 0:
    # Make it odd
    window_width -= 1
    
  current_idx = start / PAGE_SIZE
  wl = min(max(0, current_idx - window_width / 2), num_pages - window_width)
  wr = min(total / PAGE_SIZE, wl + window_width)
  logging.info('wl=%s, wr=%s' % (wl, wr))

  if wl > 0:
    page_str += '... '
  logging.info('wl=%s, wr=%s, current_idx=%s, len(pages)=%s' % \
               (wl, wr, current_idx, len(pages)))
  for i in range(wl, wr):
    if i != current_idx:
      page_str += pages[i]
    else:
      page_str += '%s ' % (i+1,)
  if wr < total / PAGE_SIZE:
    page_str += '... '

  if num_pages > 1:
    if start < total - PAGE_SIZE:
      page_str += '<a href="%s">Last</a> ' % (make_url((total - PAGE_SIZE) + 1),)

  page_str += '</div>'
  return page_str


def markup_urls(text):
  def escape(s):
    return cgi.escape(s, quote=True)

  url_spans = urlfinder.find_urls(text)
  if len(url_spans) == 0:
    return escape(text)
  
  import StringIO
  result = StringIO.StringIO()
  start = 0
  for url_start, url_end in urlfinder.find_urls(text):
    result.write(escape(text[start:url_start]))
    url = escape(text[url_start:url_end])
    result.write('<a href="%s">%s</a>' % (url, url))
    start = url_end
  result.write(escape(text[start:]))
  return result.getvalue()


def prepare_results_for_display(records):
  results = []

  for r in records:
    r['text'] = markup_urls(r['text'])
                 
  current_date = None
  previous_timestamp = None
  for r in records:
    if current_date is None or not is_same_day(previous_timestamp, r['timestamp']):
      previous_timestamp = r['timestamp']
      current_date = r['timestamp'].date()
    results.append({'date': current_date, 'record': r})
  return results

def is_same_day(t1, t2):
  v = not (t1.day != t2.day or t1.month != t2.month or t1.year != t2.year)
  #logging.info('%s = %s: %s' % (t1, t2, v))
  return v



# ------------------------------------------------------------
# Application URL routing.
# ------------------------------------------------------------

application = webapp.WSGIApplication([('/', Search),
                                      ('/uuuuu', UploadLog),
                                      ('/uuuuv', AddMD5),
                                      ('/a', Admin),
                                      #('/indexing_did_finish', sirc.fe.index.IndexingFinished)
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
