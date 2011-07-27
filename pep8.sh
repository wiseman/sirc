#!/bin/bash

pep8 --repeat --ignore=E111 --exclude=solr.py tools sirc sirc/be sirc/fe sirc/util
