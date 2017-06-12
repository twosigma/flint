#!/usr/bin/env bash
set -e
#
# Run a set of scala tests from a file
# Ignore empty files
#
if test "$#" -lt 1; then 
  echo "Must provide filename suffix (i.e. aa) as parameter"; exit 1
fi
filename=scala_test_${1}
if test ! -f ${filename}; then
  echo "File ${filename} does not exist - nothing to do!"; exit 0
fi
# It is imperative that the arguments, starting with 'testOnly' are
# are passed to sbt as a single string, otherwise all tests are run!
sbt "testOnly $(cat ${filename})"
rm -f ${filename}
