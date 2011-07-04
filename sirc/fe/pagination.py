import math
import StringIO


def get_pagination(adjacents,
                   limit,
                   page,
                   total_items,
                   script_name,
                   extra):
  prev_page = page - 1
  next_page = page + 1
  last_page = int(math.ceil(float(total_items) / limit))
  lpm1 = last_page - 1
  p = StringIO.StringIO()

  def pw(s):
    p.write(s)

  counter = 0
  if last_page > 1:
    pw(u'<div class="pagination">')
    if page > 1:
      pw(u'<a href="%s?page=%s%s">\u00AB previous</a> ' % (script_name,
                                                           prev_page,
                                                           extra))
    else:
      pw(u'<span class="pagination_disabled"> \u00AB previous</span> ')

    if last_page < 7 + (adjacents * 2):
      for counter in range(1, last_page + 1):
        if counter == page:
          pw(u'<span class="pagination_current">%s</span> ' % (counter,))
        else:
          pw(u'<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                                 counter,
                                                 extra,
                                                 counter))
    elif last_page > 5 + (adjacents * 2):
      if page < 1 + (adjacents * 2):
        for counter in range(1, 4 + adjacents * 2):
          if counter == page:
            pw('<span class="pagination_current">%s</span> ' % (counter,))
          else:
            pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                                  counter,
                                                  extra,
                                                  counter))
        pw('...')
        pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                               lpm1,
                                               extra,
                                               lpm1))
        pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                               last_page,
                                               extra,
                                               last_page))
      elif last_page - (adjacents * 2) > page and page > (adjacents * 2):
        pw('<a href="%s?page=1%s">1</a> ' % (script_name, extra))
        pw('<a href="%s?page=2%s">2</a> ' % (script_name, extra))
        pw('...')
        for counter in range(page - adjacents, page + adjacents + 1):
          if counter == page:
            pw('<span class="pagination_current">%s</span> ' % (counter,))
          else:
            pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                                  counter,
                                                  extra,
                                                  counter))
        pw('...')
        pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                               lpm1,
                                               extra,
                                               lpm1))
        pw('<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                               last_page,
                                               extra,
                                               last_page))
      else:
        pw('<a href="%s?page=1%s">1</a> ' % (script_name, extra))
        pw('<a href="%s?page=2%s">2</a> ' % (script_name, extra))
        pw('...')
        for counter in range(last_page - (2 + (adjacents * 2)), last_page + 1):
          if counter == page:
            pw('<span class="pagination_current">%s</span> ' % (counter,))
          else:
            pw(u'<a href="%s?page=%s%s">%s</a> ' % (script_name,
                                                    counter,
                                                    extra,
                                                    counter))
  if page < last_page:
    pw(u'<a href="%s?page=%s%s">next \u00BB</a>' % (script_name,
                                                    next_page,
                                                    extra))
  else:
    pw(u'<span class="pagination_disabled">next \u00BB</span>')
  pw(u'</div>\n')
  return unicode(p.getvalue())
