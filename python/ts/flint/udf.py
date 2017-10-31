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

from pyspark.sql.types import StructType, StructField
from pyspark.serializers import PickleSerializer

def _fn_and_type(udf_column):
    '''Get the python function and sql date type from a spark udf column
    :return: A tuple of (function, dataType)
    '''
    ser = PickleSerializer()
    b = udf_column._jc.expr().func().command()
    return ser.loads(b)

def _children_column_names(udf_column):
    '''Get children column names from a spark udf column. (Used for column pruning)
    :return: A list of children column names.
    '''
    children_exprs = udf_column._jc.expr().children()
    size = children_exprs.size()
    return [children_exprs.apply(i).name() for i in range(size)]

def _numpy_to_python(v):
    '''Converts numpy types or tuple of numpy types to python types.

    This function is expensive, don't invoke this on every row.
    '''
    if isinstance(v, tuple):
        vs = v
        return tuple(_numpy_to_python(v) for v in vs)
    elif hasattr(v, 'item') and callable(v.item):
        return v.item()
    else:
        return v

def _wrap_data_types(returnType):
    '''
    Wrap (dataType, dataType) to StructType([StructField('_0',
    dataType0), StructField('_1', dataType1)]) to make it a valid
    pyspark returnType.
    '''
    if isinstance(returnType, tuple):
        ret = StructType([StructField("_{}".format(i), returnType[i]) for i in range(len(returnType))])
        return ret
    else:
        return returnType

def _unwrap_data_types(returnType):
    '''
    Unwrap StructType([StructField('_0', dataType0), StructField('_1', dataType1)])
    to [dataType0, dataType1]
    :param returnType:
    :return:
    '''
    return [f.dataType for f in returnType.fields]
