<!--
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
-->
# Python tests

## Overview
This directory contains the code to test the Python code. It uses the `unittest` module.

## Prerequisites
The tests need a spark distribution installed locally to run. An easy way to do it is to go to the
[Apache Spark download page](https://spark.apache.org/downloads.html) and select version 2.1.1 (May 02 2017), Pre-built for Apache Hadoop 2.7 and later.

Extract the tarball in a local directory and set the following environment variable:
```
export SPARK_HOME=<local-spark-directory>
```
One time preparation for running the python tests can be setup by runinning the following
from the root Flint directory:
```
scripts/prepare_python_tests.sh
```

## Running tests
To run the tests issue the following command from the root Flint directory:
```
scripts/run_python_tests.sh
```


## Extending
If the test setup done in the default class, `SparkTestCase`, does not fit the needs of a particular environment, a new class can be written. The name of the new class, say `MyTestCase` is then exported in the `BASE_CLASS` variable before the tests are run:
```
export FLINT_BASE_TESTCASE=<Name of new class>
```
