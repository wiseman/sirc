# sirc
#
# Copyright 2011 John Wiseman <jjwiseman@gmail.com>

from __future__ import with_statement
import cgi
import datetime
import logging
import os.path
import simplejson
import sys
import time
import traceback
import urllib

from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from django.utils import simplejson

import ircloglib
import pytz

import sirc.fe.browse
import sirc.fe.index
import sirc.util.urlfinder
import sirc.fe.logrender
import sirc.fe.pagination
import sirc.log


# ------------------------------------------------------------
# Keep templates in the 'templates' subdirectory.
# ------------------------------------------------------------

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'templates')


def render_template(name, values={}):
  return template.render(os.path.join(TEMPLATE_PATH, name), values)


class BrowseDay(webapp.RequestHandler):
  def get(self, channel_str, year_str, month_str, day_str):
    server_str = 'freenode'
    start_time = time.time()
    year = int(year_str)
    month = int(month_str)
    day = int(day_str)
    log_date = datetime.date(year=year, month=month, day=day)
    log_data = ircloglib.Metadata(server=server_str,
                                  channel=channel_str,
                                  start_time=log_date)
    key = sirc.log.encode_id(log_data)
    log = sirc.fe.logrender.render_from_key(key)
    fetch_time = time.time()
    self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
    self.response.out.write(log)
    end_time = time.time()
    logging.info('total=%s ms, fetch=%s ms',
                 int((end_time - start_time) * 1000),
                 int((fetch_time - start_time) * 1000))


class BrowseChannel(webapp.RequestHandler):
  def get(self, channel_str):
    server_str = 'freenode'
    stats = sirc.fe.browse.get_statistics()
    html = sirc.fe.browse.get_channel_browse_html(server_str, channel_str, stats)
    values = {'stats_html': html}
    self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
    self.response.out.write(render_template('browse.html', values))


class PostActivityStats(webapp.RequestHandler):
  def post(self):
    stats = simplejson.loads(self.request.get('stats'))
    sirc.fe.browse.set_statistics(stats)
    self.response.out.write('OK')


PAGE_SIZE = 20


class Search(webapp.RequestHandler):
  def get(self):
    values = {}
    query = self.request.get('q')
    page = self.request.get('page')

    if page and len(page) > 0:
      try:
        page = int(page)
      except:
        logging.error('Unable to parse page=%r' % (page,))
        page = 1
    else:
      page = 1

    values['css_file'] = 'main.css'
    values['has_results'] = False
    if len(query) > 0:
      values['query'] = query
      values['css_file'] = 'mainq.css'
      response = sirc.fe.index.get_query_results(query,
                                                 (page - 1) * PAGE_SIZE,
                                                 PAGE_SIZE)
      records = response['docs']
      if len(records) > 0:
        results = prepare_results_for_display(records)
        paging_html = sirc.fe.pagination.get_pagination(
          adjacents=5,
          limit=PAGE_SIZE,
          page=page,
          total_items=response['numFound'],
          script_name=self.request.path,
          extra='&q=%s' % (cgi.escape(query),))
        total_ms = int(response['query_time'] * 1000)
        result_html = render_template(
          'serp.html', {'start': (page - 1) * PAGE_SIZE + 1,
                        'end': (page - 1) * PAGE_SIZE + len(results),
                        'total': response['numFound'],
                        'results': results,
                        'total_time': '%s' % (total_ms,),
                        'query_time': '%s' % (response['QTime'],),
                        'pagination_html': paging_html})
        values['result_html'] = result_html
        values['has_results'] = True
    self.response.out.write(render_template('search.html', values))

  def handle_exception(self, exception, debug_mode):
    logging.info('WOO')
    from google.appengine.api import xmpp

    exception_name = sys.exc_info()[0].__name__
    exception_details = str(sys.exc_info()[1])
    exception_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
    logging.error(exception_traceback)
    self.error(500)

    user_gtalk = 'jjwiseman@gmail.com'
    if xmpp.get_presence(user_gtalk):
      logging.info('got it')
      msg = "%s: %s\n\n%s" % (exception_name,
                              exception_details,
                              exception_traceback)
      xmpp.send_message(user_gtalk, msg)
    logging.info('done')


def create_pagination_html(url, query, start, total):
  start = int(start / PAGE_SIZE) * PAGE_SIZE
  base_url = '%s?q=%s' % (url, urllib.quote_plus(query.encode('utf-8')))

  def make_url(start):
    return cgi.escape('%s&start=%s' % (base_url, start))

  pages = []
  for offset in range(0, total, PAGE_SIZE):
    pages.append('<a href="%s">%s</a> ' % (make_url(offset),
                                           (offset / PAGE_SIZE) + 1))

  page_str = '<div class="pagination">'

  num_pages = total / PAGE_SIZE

  previous_next_nav_html = ''
  if num_pages > 1:
    if start >= PAGE_SIZE:
      previous_next_nav_html += '<a href="%s">Previous</a> ' % \
                                (make_url(start - PAGE_SIZE),)
    if start < total - PAGE_SIZE:
      previous_next_nav_html += '<a href="%s">Next</a> ' % \
                                (make_url(start + PAGE_SIZE),)
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
      page_str += '%s ' % (i + 1,)
  if wr < total / PAGE_SIZE:
    page_str += '... '

  if num_pages > 1:
    if start < total - PAGE_SIZE:
      page_str += '<a href="%s">Last</a> ' % \
                  (make_url((total - PAGE_SIZE) + 1),)

  page_str += '</div>'
  return page_str


def prepare_results_for_display(records):
  results = []

  for r in records:
    r['text'] = sirc.util.urlfinder.markup_urls(r['text'])

  current_date = None
  previous_timestamp = None

  # Adjust timestamps to desired timezone for subsequent uses.
  pacific_tz = pytz.timezone('America/Los_Angeles')
  for r in records:
    ts = r['timestamp']
    ts = ts.replace(tzinfo=pytz.utc).astimezone(pacific_tz)
    r['timestamp'] = ts

  for r in records:
    if current_date is None or not is_same_day(previous_timestamp,
                                               r['timestamp']):
      previous_timestamp = r['timestamp']
      current_date = r['timestamp'].date()
    results.append({'date': current_date, 'record': r})

  for r in records:
    r['log_url'] = sirc.log.browse_url_for_key(r['id'])

  return results


def is_same_day(t1, t2):
  v = not (t1.day != t2.day or t1.month != t2.month or t1.year != t2.year)
  #logging.info('%s = %s: %s' % (t1, t2, v))
  return v


# class BaseRequestHandler(webapp.RequestHandler):
#   def handle_exception(self, exception, debug_mode):
#     exception_name = sys.exc_info()[0].__name__
#     exception_details = str(sys.exc_info()[1])
#     exception_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
#     logging.error(exception_traceback)
#     # expiration in seconds (max 1 mail per hour for a particular
#     # exception)
#     exception_expiration = 3600
#     mail_admin = "yourmail@yourdomain"  # must be admin for the application
#     sitename = "yourapplication"
#     throttle_name = 'exception-' + exception_name
#     throttle = memcache.get(throttle_name)
#     if throttle is None:
#       memcache.add(throttle_name, 1, exception_expiration)
#       subject = '[%s] exception [%s: %s]' % (sitename,
#                                              exception_name,
#                                              exception_details)
#       mail.send_mail_to_admins(sender=mail_admin,
#                                subject=subject,
#                                body=exception_traceback)
#     template_values = {}
#     if users.is_current_user_admin():
#       template_values['traceback'] = exception_traceback
#     self.response.out.write(template.render('error.html',
#                                             template_values))


def real_main():
  run_wsgi_app(application)


def profile_main():
    # This is the main function for profiling
    # We've renamed our original main() above to real_main()
    import cProfile
    import pstats
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
