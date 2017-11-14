Partition Preserving Operations
===============================

What are partition preserving operations
----------------------------------------
**Definition 1: Partitioning**
```
The partitioning of DataFrame is a mapping from key space to
its partitions. The key range of a partition is a range that covers all keys
in the partition. here, key range doesn't need to be tight. For the ordered
key spaces we deal with here, partitioning includes the key
ranges of each partition and the key ordering within each partition
and we say

df1 and df2 have the same partitioning iff:
* df1 and df2 have the same number of partitions
* df1 and df2 have the same key ranges
* df1 and df2 have the same key ordering within each partition

```

**Notation 1: ->**
```
We use -> to represent a DataFrame operation. A DataFrame operation is
a function transform DataFrame from one to the other. For instance,
select("col") is a DataFrame operation.

For the rest of the document,
df1 -> df2 means df2 is the result by applying a DataFrame operation -> on df1.

```

**Definition 2: Partition Preserving Operations**
```
A partition preserving operation is a DataFrame operation that doesn't
change the partitioning of the input DataFrame, or equivalently:

Given a DataFrame operation df1 -> df2, -> is a partition preserving operation iff:
for any DataFrame df1, df2 has the same partitioning as df1.
```

Why partition preserving operations are important
-------------------------------------------------
`OrderedRDD` maintains metadata of each partition (i.e. dependencies
between `OrderedRDD` partitions and `DataFrame` partitions, key range
of each `OrderedRDD` partition). If a `DataFrame` operation is not
partition preserving, we will need to reconstruct the metadata when
creating a `OrderedRDD`, which can be as expensive as resorting the
entire DataFrame. On the other hand, if a `DataFrame` operation is
partition preserving, we can reuse old partition metadata and the new
`DataFrame` and create a new `OrderedRDD` with relatively low cost.

Catalyst's operation push down (predicates push down, column pruning)
can be problematic for Flint. For instance, one would think
`select("col")` is a partition preserving operation, however, this is
not true. For instance, `df.orderBy("col").select("col")` has
different partitioning as `df.orderBy("col")`. This is because
`df.orderBy("col").select("col")` is optimized by Catalyst to
`df.select("col").orderBy("col")`. `df.select("col").orderBy("col")`
and `df.orderBy("col")` have different partitioning because the hashes
used by `orderBy()` is different.

In fact, it's hard to prove any operation is a partition preserving
operation. However it's easy to prove certain operations are partition
preserving operations on a subset of DataFrames. For the rest of the
doc, we will study partition preserving operations on a particular
subset of `DataFrame`s.

Partition Preserving Physical Node
----------------------------------
**Notation 2: Physical Plan**
```
We use the following notation to describe the executed plan for a DataFrame

df.executedPlan = leaf :: node_1 :: node_2 :: ... :: node_n

This describes the physical plan for DataFrame df:

node_n
   |---node_n-1
         |----- ...
                 |--- node_1
                        |--- LeafNode

```

Catalyst rules might push a projection or a filter into a leaf
node. For instance, `ParquetRelation("time", "value") :: Projection("time")`
might be optimized to `ParquetRelation("time")`. Some types of leaf
node are partition preserving in the sense that their partitioning
won't be changed by such optimizations. Because of this property,
these nodes make it easy to reason about partition preserving of
`DataFrame` operations. To give a formal definition:

**Definition 3: PartitionPreservingLeafNode**
```
For any given DataFrame df1, where df1.executedPlan = leaf :: node_1 :: node_2 :: ... :: node_n
and a DataFrame operation df1 -> df2, where df2.executedPlan = leaf' :: node_1' :: node_2' :: ... :: node_m',
if leaf and leaf' always have the same partitioning, then leaf is a PartitionPreservingLeafNode.
```
Note 1: It's important that df2 is the result of *one* operation on
df1, because one can always break partitioning by `orderBy("col").cache()`

**Definition 4: PartitionPreservingUnaryNode**
```
Given a DataFrame df, df1.executedPlan = leaf, df2.executedPlan = leaf :: node, node is a PartitionPreservingUnaryNode iff:
df1 and df2 have the same partitioning
```

**Definition 5: PartitionPreservingDataFrame**
```
A DataFrame df is a PartitionPreservingDataFrame iff:
df.executedPlan = leaf :: node_1 :: node_2 :: .. :: node_n, where leaf is PartitionPreservingLeafNode and node_1 ... node_n are all PartitionPreservingUnaryNodes
```

Partition Info
--------------
`TimeSeriesRDD` consists of a `DataFrame` and an
`OrderedRDD`. `OrderedRDD` maintains partition metadata named
`PartitionInfo`. `PartitionInfo` consists of:
* Time ranges
* Partition dependencies between `OrderedRDD` and `DataFrame`

**Theorem 1**
```
Given a PartitionPreservingDataFrame df1 = leaf :: node_1 :: node_2 :: ... :: node_n, and a DataFrame operations df1 -> df2,
if df2 is also a PartitionPreservingDataFrame: df2 = leaf' :: node_1' :: node_2' :: ... node_m', then
df1 and df2 have the same partitioning

```

Proof:
```
leaf and leaf' are PartitionPreservingLeafNodes => leaf and leaf' have the same partitioning
node_1...node_n, node_1'...node_m' are PartitionPreservingUnaryNodes => df1 and df2 have the same partitioning

```

TimeSeriesRDD (Scala)
---------------------
**Invariant 1**
```
The DataFrame inside TimeSeriesRDD is always a PartitionPreservingDataFrame
```

**Theorem 2**
```
Given a TimeSeriesRDD tsrdd1, df1 = tsrdd1.dataFrame and a DataFrame operations df1 -> df2,
if df2 is a PartitionPreservingDataFrame, we can construct TimeSeriesRDD tsrdd2 by:

tsrdd2 = TimeSeriesRDD(df2, tsrdd1.partitionInfo)
```

Proof:
```
Invariant 1 => df1 is PartitionPreservingDataFrame
df2 is PartitionPreservingDataFrame, Theorem 1 => df1 and df2 have the same partitioning
df1 and df2 have the same partitioning -> we can reuse tsrdd1.partitionInfo
```

From Theorem 2, for a `DataFrame` operation df1 -> df2, we just need to
check if df2 is a `PartitionPreservingDataFrame` to decide if we can reuse
partition info or throws exception if we cannot.


TimeSeriesDataFrame (Python)
----------------------------
Unlike `TimeSeriesRDD`, a `TimeSeriesDataFrame` is not always sorted, nor
does it always have partitionInfo.

For a given operation df1 -> df2, we need to decide:
* If df1 has partitionInfo, whether df2 has the same partitioning as
  df1. Note that If df1 doesn't have partitionInfo, we don't care whether df2
  has the same partitioning as df1 as there is no partitionInfo to
  reuse.
* if df1 is sorted, whether or not df2 is sorted. Note that If df1 is not
  sorted, we don't care whether df2 is sorted - we treat df2 as it's
  not sorted. Note that df2 could be sorted if the operation is
  orderBy("time"), but it's beyond the scope of this doc.

### Partition Info
The first problem is similar to Scala. From Theorem 2, we know we can
reuse partitionInfo iff:
* df1 and df2 both are `PartitionPreservingDataFrame`

In Python, we ensure the following invariant:

**Invariant 2**
```
If TimeSeriesDataFrame has partitionInfo, the DataFrame inside TimeSeriesDataFrame is a PartitionPreservingDataFrame
```
We enforce this invariant because partitionInfo can only be reused if
the DataFrame is a `PartitionPreservingDataFrame`, therefore
there's no need to store partitionInfo if it's not.

From Theorem 1 and Invariant 2, we conclude that we can reuse
df1.partitionInfo iff df2 is a `PartitionPreservingDataFrame`.

### Ordering
#### Difference between Ordering and Partitioning
We consider partitioning to be a physical property of a DataFrame,
i.e, it's affected by the physical plan of a DataFrame. That's why the
analysis of partitioning preserving operation has been done on the
physical plan level.

Unlike partitioning, ordering is a logical property of a DataFrame -
how a DataFrame is ordered is not affected by the physical plan of a
DataFrame, rather, it should only be affected by the logical plan of a
DataFrame. If a DataFrame is ordered, it's always ordered no matter
how Catalyst decides to construct it's physical plan. Therefore, we
look at the logical plan to see whether or not the ordering is changed
by a DataFrame operation.

#### Analyzed Plan
There are four stages of logical plan transformations in QueryExecution,
each one of them is the output of some transformation of the previous one. To use ==> to
describe a transformation, we have:

logicalPlan ==> analyzedPlan ==> withCachedData ==> optimizedPlan

For an operation df1 -> df2, we *believe* analyzedPlan has the following properties:
* -> will only add logical node to df1.analyzedPlan, i.e, If
  `df1.analyzedPlan = node_1 :: node_2 :: ... :: node_n`, then
  `df2.analyzedPlan = node_1 :: node_2 :: ... ::node_n :: node_1' :: node_2' :: ... :: node_n'`
  (comparing to withCachedData, in which certain cached logical node can be
  transformed to InMemoryRelation, i.e, if df1 is cached, df2.withCachedData will end up being:
  `InMemoryRelation :: node_1' :: node_2' :: ... :: node_n'`)
  This property can be checked in code by comparing the analyzedPlan of df1 and df2.

* node_1' .. node_n' are "detailed" enough that we can decide whether
  or they are ordering preserving (comparing to logical plan, in which
  Project(sum("v")) has not yet transformed to Aggregate(sum("v")))

To give a few example, for `df1 = sqlContext.read.parquet("path").orderBy("time")`
and  `df1.analyzedPlan = Relation :: Sort`.

Order Preserving:
```
df2 = df1.select("time")
df2.analyzedPlan = Relation :: Sort :: Project

df2 = df1.filter(df1("time") > 0)
df2.analyzedPlan = Relation :: Sort :: Filter

df2 = df1.withColumn("time2", df1("time") * 2)
df2.analyzedPlan = Relation :: Sort :: Project

df2 = df1.withColumn("time2", udf({time: Long => time * 2})(df1("time")))
df2.analyzedPlan = Relation :: Sort :: EvalutePython :: Project :: Project
```

Not Order Preserving (non order preserving logical node is marked with *):
```
df2 = df1.select(sum("time"))
df2.analyzedPlan = Relation :: Sort :: *Aggregate*

df2 = df1.groupBy("time").agg(sum("value"))
df2.analyzedPlan = Relation :: Sort :: *Aggregate*

df2 = df1.withColumn("v2", percentRank().over(window))
df2.analyzedPlan = Relation :: Sort :: Project :: *Window* :: Project :: Project
```

**Theorem 3**
```
For an operation df1 -> df2, df1 and df2 have the same ordering if all
addition logical nodes by the operation in analyzedPlan are order
preserving logical node.
```
Use *Theorem 3*, we can decide whether to pass the is_sorted flag by
checking analyzedPlan of df1 and df2,

Known Partition Preserving Leaf Node
-----------------------------
(1.6.3 and 2.x)
### execution.PhysicalRDD (renamed to RDDScanExec in Spark 2.x)
PhysicalRDD is created from a LogicalRDD in the transformation from
optimized plan to spark plan. There are no rule for LogicalRDD and
PhysicalRDD. They are treated as black box in Catalyst.

### execution.InMemoryColumnarTableScan (renamed to InMemoryTableScanExec in Spark 2.x)
InMemoryColumnarTableScan is created in the transformation from
analyzed plan to spark plan. Catalyst replaces the cached subtree in
analyzed plan with InMemoryRelation. InMemoryRelation then is
transformed to InMemoryColumnarTableScan in physical planning. Rule
relates to InMemoryColumnarTableScan is InMemoryScan (in
SparkStrategies.scala). Project and filter can be pushed into a
InMemoryColumnarTableScan, but its partitioning is not changed.


Not all InMemoryColumnarTableScan are partition preserving leaf node,
consider the following case:
```
df = ...
// df.executedPlan = PhysicalRDD
df2 = df.orderBy("time").cache()
df2.count()
// df2.executedPlan = InMemoryColumnarTableScan

df3 = df2.select("time")
df2.unpersist()
// df3.executePlan = PhysicalRDD :: Project :: Sort
```
In the example, InMemoryColumnarTableScan in df2 is *NOT* a
PartitionPreservingLeafNode because it doesn't have the same
partitioning as PhysicalRDD in df3. Therefore, here we only consider
InMemoryColumnarTableScan created from a PhysicalRDD +
PartitionPreservingPhysicalNode. Invariant 1 enforces this by always
converting the input DataFrame to a PhysicalRDD.


Related code: InMemoryScan, InMemoryRelation, InMemoryColumnarTableScan


Known Order Preserving Logical Node
-----------------------------------
(1.6.3 and 2.x)
* logical.Project
* logical.Filter
* logical.EvaluatePython

Known Partition Preserving Physical Node
----------------------------------------
(1.6.3 and 2.x)


Partition Preserving SparkPlan is easy to verify, we just need to look
at its doExecute() and see if it changes partitions and order of data
in the partition. Project and Filter should cover most of DataFrame
operations we want such as select, filter/where, withColumn(with
simple columns), drop/dropna, fill/fillna.

### sql.execution.Project
```
protected override def doExecute(): RDD[InternalRow] = {
  val numRows = longMetric("numRows")
  child.execute().mapPartitionsInternal { iter =>
    val project = UnsafeProjection.create(projectList, child.output,
      subexpressionEliminationEnabled)
    iter.map { row =>
      numRows += 1
      project(row)
    }
  }
}
```
### sql.execution.Filter
```
protected override def doExecute(): RDD[InternalRow] = {
  val numInputRows = longMetric("numInputRows")
  val numOutputRows = longMetric("numOutputRows")
  child.execute().mapPartitionsInternal { iter =>
    val predicate = newPredicate(condition, child.output)
    iter.filter { row =>
      numInputRows += 1
      val r = predicate(row)
      if (r) numOutputRows += 1
      r
    }
  }
}
```

### sql.execution.BatchPythonEvalution
```
protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)

    inputRDD.mapPartitions { iter =>
      EvaluatePython.registerPicklers()  // register pickler for Row

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = new java.util.concurrent.ConcurrentLinkedQueue[InternalRow]()

      val pickle = new Pickler
      val currentRow = newMutableProjection(udf.children, child.output)()
      val fields = udf.children.map(_.dataType)
      val schema = new StructType(fields.map(t => new StructField("", t, true)).toArray)

      // Input iterator to Python: input rows are grouped so we send them in batches to Python.
      // For each row, add it to the queue.
      val inputIterator = iter.grouped(100).map { inputRows =>
        val toBePickled = inputRows.map { row =>
          queue.add(row)
          EvaluatePython.toJava(currentRow(row), schema)
        }.toArray
        pickle.dumps(toBePickled)
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator = new PythonRunner(
        udf.command,
        udf.envVars,
        udf.pythonIncludes,
        udf.pythonExec,
        udf.pythonVer,
        udf.broadcastVars,
        udf.accumulator,
        bufferSize,
        reuseWorker
      ).compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val row = new GenericMutableRow(1)
      val joined = new JoinedRow

      outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
      }.map { result =>
        row(0) = EvaluatePython.fromJava(result, udf.dataType)
        joined(queue.poll(), row)
      }
    }
  }
```

Future Work
-----------
This document only discusses leaf node PhysicalRDD and
InMemoryColumnarTableScan. However, similar properties should exist in
other types of physical node such as ParquetRelation. Catalyst will
push column filter into ParquetRelation physical node, but I suspect
the partitioning won't change by the optimization. However, whether a
filter will be pushed to ParquetRelation and change the number of
output partitions is unknown.