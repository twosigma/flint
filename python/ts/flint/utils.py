#
#  Copyright 2015 TWO SIGMA OPEN SOURCE, LLC
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

_UNIT_TO_JUNIT = {
    "s": "SECONDS",
    "ms": "MILLISECONDS",
    "us": "MICROSECONDS",
    "ns": "NANOSECONDS"
}

def jsc(sc):
    """Returns the underlying Scala SparkContext

    :param sc: SparkContext
    :return: :class:`py4j.java_gateway.JavaObject` (org.apache.spark.SparkContext)
    """
    return sc._jsc.sc()

def jvm(sc):
    """Returns the Pyspark JVM handle

    :param sc: SparkContext
    :return: :class:`py4j.java_gateway.JavaView
   ` """
    return sc._jvm

def scala_object(jpkg, obj):
    return jpkg.__getattr__(obj + "$").__getattr__("MODULE$")

def scala_package_object(jpkg):
    return scala_object(jpkg, "package")

def pyutils(sc):
    """Returns a handle to ``com.twosigma.flint.rdd.PythonUtils``

    :param sc: SparkContext
    :return: :class:`py4j.java_gateway.JavaPackage` (com.twosigma.flint.rdd.PythonUtils)
    """
    return jvm(sc).com.twosigma.flint.rdd.PythonUtils

def copy_jobj(sc, obj):
    """Returns a Java object ``obj`` with an additional reference count

    :param sc: Spark Context
    :param obj: :class:`py4j.java_gateway.JavaObject`
    :return: ``obj`` (:class:`py4j.java_gateway.JavaObject`) with an additional reference count
    """
    return pyutils(sc).makeCopy(obj)

def to_list(lst):
    """Make sure the object is wrapped in a list

    :return: a ``list`` object, either lst or lst in a list
    """
    if isinstance(lst, str):
        lst = [lst]
    elif not isinstance(lst, list):
        try:
            lst = list(lst)
        except TypeError:
            lst = [lst]
    return lst

def list_to_seq(sc, lst):
    """Shorthand for accessing PythonUtils Java Package

    If lst is a Python None, returns a Scala empty Seq
    If lst is a Python object, such as str, returns a Scala Seq containing the object
    If lst is a Python tuple/list, returns a Scala Seq containing the objects in the tuple/list

    :return: A copy of ``lst`` as a ``scala.collection.Seq``
    """
    if lst is None:
        lst = []
    return jvm(sc).org.apache.spark.api.python.PythonUtils.toSeq(to_list(lst))

def py_col_to_scala_col(sc, py_col):
    converters = {
        list: list_to_seq,
        tuple: list_to_seq
    }
    convert = converters.get(type(py_col))

    if convert:
        return convert(sc, py_col)
    else:
        return py_col

def junit(sc, unit):
    """Converts a Pandas unit to scala.concurrent.duration object

    :return: Scala equivalent of ``unit`` as ``scala.concurrent.duration object``
    """
    if unit not in _UNIT_TO_JUNIT:
        raise ValueError("unit must be in {}".format(_UNIT_TO_JUNIT.keys()))
    return scala_package_object(jvm(sc).scala.concurrent.duration).__getattr__(_UNIT_TO_JUNIT[unit])()

def jschema(sc, schema):
    """Converts a Python schema (StructType) to a Scala schema ``org.apache.spark.sql.types.StructType``

    :return: :class:``org.apache.spark.sql.types.StructType``
    """
    import json
    return jvm(sc).org.apache.spark.sql.types.StructType.fromString(json.dumps(schema.jsonValue))
