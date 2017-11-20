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

import collections
import itertools

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


def _required_column_names(udf_columns):
    '''
    Returns a list of column names (str) that are required for these udfs.
    :param udf_columns: A list of udf columns.
    '''
    return list(set(itertools.chain(
        *[_children_column_names(col) for col in udf_columns])))


def _check_invalid_udfs(udf_columns):
    '''
    Checks if given udf columns are valid. Raises ValueError if they are not.

    :param udf_columns: A list of udf columns.
    '''
    for col in udf_columns:
        for index in col.column_indices:
            if index is None:
                raise ValueError(
                    'Column passed to the udf function must be a column in the DataFrame, '
                    'i.e, df[col] or df[[col1, col2]]. '
                    'Other types of Column are not supported.')


def _flat_column_indices(column_indices):
    new_columns = []
    for col_name in column_indices:
        if isinstance(col_name, str):
            new_columns.append(col_name)
        elif isinstance(col_name, collections.Sequence):
            new_columns.extend(col_name)
        else:
            raise ValueError("Invalid column indices {}".format(col_name))
    return new_columns
