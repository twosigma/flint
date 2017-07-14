#!/usr/bin/env bash


#
#  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
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
