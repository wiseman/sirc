SIRC - "Search IRC"
===================

SIRC is a search engine for IRC logs.  It has four big parts:

* The front end, which lets users do searches and shows the results (and lets users browse old logs).
* The back end, which indexes IRC logs and stores them in the log repository.
* The search index.
* The log repository.

The front end
-------------

The front end is written in Python and runs on Google's App Engine.
You can see it in action (try it out!) at
[http://heavymetalab.appspot.com/](http://heavymetalab.appspot.com/).

The home page:

![Screenshot of heavymetalab.appspot.com home page](https://github.com/wiseman/sirc/raw/master/screenshots/sirc-home.png "Home page")

The results of a search:

![Screenshot of heavymetalab.appspot.com serp page](https://github.com/wiseman/sirc/raw/master/screenshots/sirc-serp.png "SERP page")

Looking at the log for one channel for one day:

![Screenshot of heavymetalab.appspot.com home page](https://github.com/wiseman/sirc/raw/master/screenshots/sirc-log.png "Log page")

Browsing the entire history of a channel, with color-coded activity indicators:

![Screenshot of heavymetalab.appspot.com home page](https://github.com/wiseman/sirc/raw/master/screenshots/sirc-browse.png "Browsing page")

The front end queries the search index for searches, displays search
results, and renders logs for display.

The back end
------------

The back end collects IRC logs, adds them to the index, and stores them in the log repository.

To collect logs, I use code from my [irc-logs
project](http://code.google.com/p/irc-logs/) (I don't have a log bot,
so I use the logs that nef collects at
[http://tunes.org/~nef/logs/](http://tunes.org/~nef/logs/).

The search index
----------------

SIRC indexes each line of every IRC log using [Solr](http://lucene.apache.org/solr/).

The log repository
------------------

I store the logs in Amazon S3.  The front end gets them from S3, marks them up with HTML, and shows them to the user.
