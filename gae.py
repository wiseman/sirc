# Copyright 2012 John Wiseman
# jjwiseman@gmail.com
#
# This script is defined as the handler for everything in app.yaml.
#
# From https://developers.google.com/appengine/docs/python/runtime#App_Caching:
# 
#     You can tell App Engine to cache the CGI handler script itself,
#     in addition to imported modules. If the handler script defines a
#     function named main(), then the script and its global
#     environment will be cached like an imported module. The first
#     request for the script on a given web server evaluates the
#     script normally. For subsequent requests, App Engine calls the
#     main() function in the cached environment.
#
#     To cache a handler script, App Engine must be able to call
#     main() with no arguments. If the handler script does not define
#     a main() function, or the main() function requires arguments
#     (that don't have defaults), then App Engine loads and evaluates
#     the entire script for every request.
#
# So we define a main here.

import sirc.fe


def main():
  sirc.fe.run_application()


if __name__ == '__main__':
  main()
