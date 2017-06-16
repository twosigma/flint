#!/bin/bash

#
#  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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

set -v

cd ..
make dist
cd python

curl http://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz -o spark.tgz
tar -xzf spark.tgz
mv spark-2.1.1-bin-hadoop2.7 spark
# lower log level to WARN by default;
# otherwise travis output will be swamped by Spark reporting.
cp travis/spark_log4j.properties spark/conf/log4j.properties
# Add the flint assembly to spark test instance
sed "s,_FLINT_ROOT_,$(pwd)/..," <travis/spark-defaults.conf >spark/conf/spark-defaults.conf

conda create -n flint python=3.5 pandas notebook
