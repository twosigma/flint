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
# Divide all scala tests into ${1} files
# Otherwise Travis will fail with a timeout, trying to run one
# long job.
#
if [ "$#" -lt 1 ]; then
  echo "Must provide number of files as parameter"; exit 1
fi
# Convert file path to class names
cd ./src/test/scala
find . -name '*Spec.scala' |
sed -e 's/\.scala//' -e 's,/,.,g' -e 's/^..//' >../../../all_tests
cd ../../..
# Calculate the number of lines per file
total_lines=$(wc -l < all_tests)
lines_per_file=$(echo "${total_lines}/${1}+1" | bc)
# Split all_tests into files of name like scala_test_aa, scala_test_ab, ...
rm -f scala_test_*
split -l ${lines_per_file} all_tests scala_test_
rm -f all_tests
