# Flint: A Time Series Library for Apache Spark

The ability to analyze time series data at scale is critical for the success of finance and IoT applications based on Spark.
Flint is Two Sigma's implementation of highly optimized time series operations in Spark.
It performs truly parallel and rich analyses on time series data by taking advantage of the natural ordering in time series data to provide locality-based optimizations.

Flint is an open source library for Spark based around the `TimeSeriesRDD`, a time series aware data structure, and a collection of time series utility and analysis functions that use `TimeSeriesRDD`s.
Unlike `DataFrame` and `Dataset`, Flint's `TimeSeriesRDD`s can leverage the existing ordering properties of datasets at rest and the fact that almost all data manipulations and analysis over these datasets respect their temporal ordering properties.
It differs from other time series efforts in Spark in its ability to efficiently compute across panel data or on large scale high frequency data.

[![Documentation Status](https://readthedocs.org/projects/ts-flint/badge/?version=latest)](http://ts-flint.readthedocs.io/en/latest/?badge=latest)

## Requirements

| Dependency     | Version           |
| -------------- | ----------------- |
| Spark Version  |  2.3              |
| Scala Version  |  2.11.7 and above |
| Python Version |  3.5 and above    |


## How to install
Scala artifact is published in maven central:

https://mvnrepository.com/artifact/com.twosigma/flint

Python artifact is published in PyPi:

https://pypi.org/project/ts-flint

Note you will need both Scala and Python artifact to use Flint with PySpark.

## How to build
To build from source:

Scala (in top-level dir):

```bash
sbt assemblyNoTest
```

Python (in python subdir):
```bash
python setup.py install
```
or
```bash
pip install .
```

## Python bindings

The python bindings for Flint, including quickstart instructions, are documented at [python/README.md](python/README.md).
API documentation is available at http://ts-flint.readthedocs.io/en/latest/.

## Getting Started

### Starting Point: `TimeSeriesRDD` and `TimeSeriesDataFrame`
The entry point into all functionalities for time series analysis in Flint is `TimeSeriesRDD` (for Scala) and `TimeSeriesDataFrame` (for Python). In high level, a `TimeSeriesRDD` contains an `OrderedRDD` which could be used to represent a sequence of ordering key-value pairs. A `TimeSeriesRDD` uses `Long` to represent timestamps in nanoseconds since epoch as keys and `InternalRow`s as values for `OrderedRDD` to represent a time series data set.

### Create `TimeSeriesRDD`

Applications can create a `TimeSeriesRDD` from an existing `RDD`, from an `OrderedRDD`, from a `DataFrame`, or from a single csv file.

As an example, the following creates a `TimeSeriesRDD` from a gzipped CSV file with header and specific datetime format.

```scala
import com.twosigma.flint.timeseries.CSV
val tsRdd = CSV.from(
  sqlContext,
  "file://foo/bar/data.csv",
  header = true,
  dateFormat = "yyyyMMdd HH:mm:ss.SSS",
  codec = "gzip",
  sorted = true
)
```

To create a `TimeSeriesRDD` from a `DataFrame`, you have to make sure the `DataFrame` contains a column named "time" of type `LongType`.

```scala
import com.twosigma.flint.timeseries.TimeSeriesRDD
import scala.concurrent.duration._
val df = ... // A DataFrame whose rows have been sorted by their timestamps under "time" column
val tsRdd = TimeSeriesRDD.fromDF(dataFrame = df)(isSorted = true, timeUnit = MILLISECONDS)
```

One could also create a `TimeSeriesRDD` from a `RDD[Row]` or an `OrderedRDD[Long, Row]` by providing a schema, e.g.

```scala
import com.twosigma.flint.timeseries._
import scala.concurrent.duration._
val rdd = ... // An RDD whose rows have sorted by their timestamps
val tsRdd = TimeSeriesRDD.fromRDD(
  rdd,
  schema = Schema("time" -> LongType, "price" -> DoubleType)
)(isSorted = true,
  timeUnit = MILLISECONDS
)
```

It is also possible to create a `TimeSeriesRDD` from a dataset stored as parquet format file(s). The `TimeSeriesRDD.fromParquet()` function provides the option to specify which columns and/or the time range you are interested, e.g.

```scala
import com.twosigma.flint.timeseries._
import scala.concurrent.duration._
val tsRdd = TimeSeriesRDD.fromParquet(
  sqlContext,
  path = "hdfs://foo/bar/"
)(isSorted = true,
  timeUnit = MILLISECONDS,
  columns = Seq("time", "id", "price"),  // By default, null for all columns
  begin = "20100101",                    // By default, null for no boundary at begin
  end = "20150101"                       // By default, null for no boundary at end
)
```

### Group functions

A group function is to group rows with nearby (or exactly the same) timestamps.

- `groupByCycle` A function to group rows within a cycle, i.e. rows with exactly the same timestamps. For example,

```scala
val priceTSRdd = ...
// A TimeSeriesRDD with columns "time" and "price"
// time  price
// -----------
// 1000L 1.0
// 1000L 2.0
// 2000L 3.0
// 2000L 4.0
// 2000L 5.0

val results = priceTSRdd.groupByCycle()
// time  rows
// ------------------------------------------------
// 1000L [[1000L, 1.0], [1000L, 2.0]]
// 2000L [[2000L, 3.0], [2000L, 4.0], [2000L, 5.0]]
```

- `groupByInterval` A funcion to group rows whose timestamps falling into an interval. Intervals could be defined by another `TimeSeriesRDD`. Its timestamps will be used to defined intervals, i.e. two sequential timestamps define an interval. For example,

```scala
val priceTSRdd = ...
// A TimeSeriesRDD with columns "time" and "price"
// time  price
// -----------
// 1000L 1.0
// 1500L 2.0
// 2000L 3.0
// 2500L 4.0

val clockTSRdd = ...
// A TimeSeriesRDD with only column "time"
// time
// -----
// 1000L
// 2000L
// 3000L

val results = priceTSRdd.groupByInterval(clockTSRdd)
// time  rows
// ----------------------------------
// 1000L [[1000L, 1.0], [1500L, 2.0]]
// 2000L [[2000L, 3.0], [2500L, 4.0]]
```

- `addWindows` For each row, this function adds a new column whose value for a row is a list of rows within its `window`.

```scala
val priceTSRdd = ...
// A TimeSeriesRDD with columns "time" and "price"
// time  price
// -----------
// 1000L 1.0
// 1500L 2.0
// 2000L 3.0
// 2500L 4.0

val result = priceTSRdd.addWindows(Window.pastAbsoluteTime("1000ns"))
// time  price window_past_1000ns
// ------------------------------------------------------
// 1000L 1.0   [[1000L, 1.0]]
// 1500L 2.0   [[1000L, 1.0], [1500L, 2.0]]
// 2000L 3.0   [[1000L, 1.0], [1500L, 2.0], [2000L, 3.0]]
// 2500L 4.0   [[1500L, 2.0], [2000L, 3.0], [2500L, 4.0]]
```

### Temporal Join Functions

A temporal join function is a join function defined by a matching criteria over time. A `tolerance` in temporal join matching criteria specifies how much it should look past or look futue.

- `leftJoin` A function performs the temporal left-join to the right `TimeSeriesRDD`, i.e. left-join using inexact timestamp matches.  For each row in the left, append the most recent row from the right at or before the same time. An example to join two `TimeSeriesRDD`s is as follows.

```scala
val leftTSRdd = ...
val rightTSRdd = ...
val result = leftTSRdd.leftJoin(rightTSRdd, tolerance = "1day")
```

- `futureLeftJoin` A function performs the temporal future left-join to the right `TimeSeriesRDD`, i.e. left-join using inexact timestamp matches. For each row in the left, appends the closest future row from the right at or after the same time.

```scala
val result = leftTSRdd.futureLeftJoin(rightTSRdd, tolerance = "1day")
```

### Summarize Functions

Summarize functions are the functions to apply summarizer(s) to rows within a certain period, like cycle, interval, windows, etc.

- `summarizeCycles` A function computes aggregate statistics of rows that are within a cycle, i.e. rows share a timestamp.

```scala
val volTSRdd = ...
// A TimeSeriesRDD with columns "time", "id", and "volume"
// time  id volume
// ------------
// 1000L 1  100
// 1000L 2  200
// 2000L 1  300
// 2000L 2  400

val result = volTSRdd.summarizeCycles(Summary.sum("volume"))
// time  volume_sum
// ----------------
// 1000L 300
// 2000L 700
```

Similarly, we could summarize over intervals, windows, or the whole time series data set. See

- `summarizeIntervals`
- `summarizeWindows`
- `addSummaryColumns`

One could check `timeseries.summarize.summarizer` for different kinds of summarizer(s), like `ZScoreSummarizer`, `CorrelationSummarizer`, `NthCentralMomentSummarizer` etc.

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the `cla` folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Spark is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Flint, or this website in any manner.

© Two Sigma Open Source, LLC
