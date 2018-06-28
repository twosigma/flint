#
#  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

import pyarrow as pa
import io
import collections

def arrowfile_to_dataframe(file_format):
    ''' Arrow file format to Pandas DataFrame
    '''
    return pa.RecordBatchFileReader(pa.BufferReader(file_format)).read_all().to_pandas()


def dataframe_to_arrowfile(df):
    ''' Pandas DataFrame to Arrow file format
    '''
    batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
    sink = io.BytesIO()
    writer = pa.RecordBatchFileWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue()

def arrowfile_to_numpy(file_format, column_index):
    ''' Arrow file format to a numpy ndarray or a list of numpy ndarray
    '''
    df = pa.RecordBatchFileReader(pa.BufferReader(file_format)).read_all().to_pandas()

    if isinstance(column_index, str):
        return df[column_index].values
    else:
        return [df[k].values for k in column_index]
