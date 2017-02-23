Partition Preserving Operations
===============================

What are partition preserving operations
----------------------------------------
**Definition 1: Partitioning**
```
"partitioning" of DataFrame is the mapping from the key space to
partitions. Key range of a partition is a range that covers all keys
in the partition (key range doesn't need to be tight). For the ordered
key spaces we deal with here, partitioning is equivalent to the key
ranges of each partition and the key ordering within each partition.

df1 and df2 have the same partitioning iff:
* df1 and df2 have the same number of partitions
* df1 and df2 have the same key ranges
* df1 and df2 have the same key ordering within each partition

```

**Notation 1: -> and -->**
```
We use -> to represent a DataFrame operation. A DataFrame operation is
a function on DataFrame that returns another DataFrame. For instance,
select("col") is a DataFrame operation.

For the rest of the document,
df1 -> df2 means df2 is the result of applying a DataFrame operation -> on df1
df1 --> df2 means df2 is the result of applying one or more DataFrame operations --> on df1

```

**Definition 2: Partition Preserving Operations**
```
A partition preserving operation is a DataFrame operation that doesn't
change the partitioning of the input DataFrame, or equivalently:

Given a DataFrame operation df1 -> df2, -> is a partition preserving operation iff:
df1 and df2 have the same partitioning for any DataFrame df1
```

Why partition preserving operations are important
-------------------------------------------------
`OrderedRDD` maintains metadata of each partition (i.e. dependencies
between `OrderedRDD` partitions and `DataFrame` partitions, time range
of each `OrderedRDD` partition). If a `DataFrame` operation is not
partition preserving, we will need to reconstruct the metadata when
creating a `OrderedRDD`, which can be as expensive as resorting the
entire DataFrame. On the other hand, if a DataFrame operation is
partition preserving, we can reuse old partition metadata and the new
DataFrame and create a new OrderedRDD with relatively low cost.

RDDScanDataFrame
----------------
Catalyst's operation push down (predicates push down, column pruning)
can be problematic for Flint. For instance, one would think
`select("col")` is a partition preserving operation, however, this is
not true. For instance, `df.orderBy("col").select("col")` has
different partitioning as `df.orderBy("col")`. This is because
`df.orderBy("col").select("col")` is optimized to
`df.select("col").orderBy("col")`. `df.select("col").orderBy("col")`
and `df.orderBy("col")` have different partitioning because the hashes
used by orderBy is different.

In fact, it's hard to prove any operation is a partition preserving
operation. However it's easy to prove certain operations are partition
preserving operations on a subset of DataFrames. For the rest of the
doc, we will study partition preserving operations on
RDDScanDataFrame.

**Notation 2: Physical Plan**
```
We use the following notation to describe the Physical Plan for a DataFrame

df.executedPlan = leaf :: node_1 :: node_2 :: ... :: node_n

This describes the physical plan for DataFrame df:

node_n
   |---node_n-1
         |----- ...
                 |--- node_1
                        |--- LeafNode
```

**Definition 3: RDDScanDataFrame**
```
A DataFrame df is a `RDDScanDataFrame` iff:
df.executedPlan = RDDScanExec

(RDDScanExec is a physical node in DataFrame which means this node is
created directly from scanning an existing rdd.)

```

**Assumption 1**
```
For a given DataFrame df1, where df1.executedPlan = RDDScanExecA :: op1 :: op2 :: .. :: opN
and one or more DataFrame operations df1 --> df2, where df2.executedPlan = RDDScanExecB :: op1' :: op2' :: .. :: opM'

RDDScanExecA and RDDScanExecB have same partitioning
(It's reasonable to assume RDDScanExecA == RDDScanExecB, but we
actually only need a weaker assumption that they have the same
partitioning)
```
`RDDScanDataFrame` act as a boundary for catalyst
optimization. Because a `RDDScanDataFrame` doesn't have any logical
plan information, it acts as a black box to catalyst prevents catalyst
from pushing an operation across/into it. Therefore,
`sqlContext.internalCreateDataFrame(df.orderBy("col").queryExecution.toRdd, df.schema).select("col")` and
`df.orderBy("col")` have the same partitioning.

Partition Preserving Physical Node
-----------------------------
Similar to partition preserving operations, we define partition
preserving physical node.

**Definition 3: Partition Preserving Physical Node**
```
Given a DataFrame df, df1.executedPlan = node, df2.executedPlan = node :: node_1, node_1 is a partition preserving physical node iff:
df1 and df2 have the same partitioning
```

**Definition 4: PartitionPreservingRDDScanDataFrame**
```
A DataFrame df is a PartitionPreservingRDDScanDataFrame iff:
df.executedPlan = RDDScanExec :: node_1 :: node_2 :: .. :: node_n, where node_1...node_n are partition preserving physical nodes
```

**Invariant 1**
```
The `DataFrame` inside `TimeSeriesRDD` is always a `PartitionPreservingRDDScanDataFrame`
```

TODO: Check ways to create TimeSeriesRDD and make sure this invariance hold
* TimeSeriesRDD.fromDF always create a RDDScanDataFrame
* TimeSeriesRDD.fromInternalOrderedRDD (OrderedRDD operations) always create a RDDScanDataFrame
* withUnshuffledDataFrame always (DataFrame operations) create a PartitionPreservingRDDScanDataFrame

Partition Info
-------------
TimeSeriesRDD consists of a DataFrame and an
OrderedRDD. OrderedRDD maintains partition metadata, called
`PartitionInfo`. `PartitionInfo` consists of:
1. Time ranges
2. Partition dependencies between OrderedRDD and DataFrame

**Theorem 1**
```
Given a PartitionPreservingRDDScanDataFrame df1 = RDDScanExecA :: node_1 :: node_2 :: ... :: node_n,
and one or more DataFrame operations df1 --> df2,
if df2 is also a PartitionPreservingRDDScanDataFrame: df2 = RDDScanExecB :: node_1' :: node_2' :: ... node_m', then
df1 and df2 have the same partitioning

```

Proof:
```
Assumption 1 => RDDScanExecA and RDDScanExecB have the same partitioning
node_1...node_n, node_1'...node_m' are partition preserving physical node => df1 and df2 have the same partitioning

```

**Theorem 2**
```
Given a TimeSeriesRDD tsrddA, df1 = tsrddA.dataFrame and one or more DataFrame operations df1 --> df2,
if df2 is a PartitionPreservingRDDScanDataFrame, we can construct TimeSeriesRDD tsrddB by:

tsrddB = TimeSeriesRDD(df2, tsrddA.partitionInfo)
```

Proof:
```
Invariant 1 => df1.executedPlan = RDDScanExecA :: op1 :: op2 :: ... :: opN, op1..opN are partition preserving.
Theorem 1 => df1 and df2 have the same partitioning
df1 and df2 have the same partitioning -> we can reuse tsrddA.partitionInfo
```

TimeSeriesRDD (Scala)
---------------------
From Theorem 2, for a DataFrame operation df1 -> df2, we just need to
check if df2 is a PartitionPreservingRDDScanDataFrame to decide if we can reuse
partition info (and throws exception if we cannot)

TimeSeriesDataFrame (Python)
----------------------------
Unlike TimeSeriesRDD, where it always reuse partition info (and throws
exception if can't), TimeSeriesDataFrame only reuse partition info
sometimes.

When it doesn't reuse partition info, it forces normalization to
happen when a TimeSeriesRDD function is invoked, so it's safe (but
slow).

When it reuses partition info, it should follow the same check as
Scala. If the check fails, it should not pass partition info.

TODO: In TimeSeriesDataFrame constructor, add checks for:
if partition info is passed, ensure df is a
PartitionPreservingRDDScanDataFrame.

To summarize, when to reuse partition info:
* in _from_tsrdd
* tsdf1.part_info is not None and df2 is a PartitionPreservingRDDScanDataFrame

Known Partition Preserving Physical Node
----------------------------------------
(Code based on Spark 1.6.3, Spark 2.x is similar)
Partition Preserving SparkPlan is easy to verify, we just need to look
at its doExecute() and see if it changes partitions and order of data
in the partition. Project and Filter should cover most of DataFrame
operations we want such as select, filter/where, withColumn(with
simple columns), drop/dropna, fill/fillna.

sql.execution.Project
--------------------
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
sql.execution.Filter
---------------------
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

Future Work
-----------
This document only discusses partition preserving operations on
RDDScanDataFrame. However, similar properties should exist in other
types of physical node such as ParquetRelation. Catalyst will push
column filter into ParquetRelation physical node, but I suspect the
partitioning won't change by the optimization.