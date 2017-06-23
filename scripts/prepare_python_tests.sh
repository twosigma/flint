#!/bin/bash

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

if test "${SPARK_HOME}" == ""; then
    echo "Must set SPARK_HOME environment variable before running this script"
    exit 1
fi
if ! test -d python/travis; then
    echo "This script must be run from the root of the Flint distribution!"
    exit 1
fi
# lower log level to WARN by default;
# otherwise travis output will be swamped by Spark reporting.
cp python/travis/spark_log4j.properties ${SPARK_HOME}/conf/log4j.properties
# Add the flint assembly to spark test instance
sed "s,_FLINT_ROOT_,$(pwd)," <python/travis/spark-defaults.conf >${SPARK_HOME}/conf/spark-defaults.conf

# Create the Flint environment 
conda create -n flint python=3.5 --file python/requirements.txt
