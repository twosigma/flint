#!/usr/bin/env bash
set -e
#
# Divide all scala tests into ${1} files
# Otherwise Travis will fail with a timeout, trying to run one
# long job.
#
if test "$#" -lt 1; then 
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
