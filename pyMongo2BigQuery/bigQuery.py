#!env/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 KBX Digital
#
# Licensed under : Apache License 2.0 
#
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


'''
'''

import os
from io import StringIO
import json
from datetime import datetime
from datetime import date

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as core_exceptions#for handling api exceptions?

from .utils import getLocalTime

class bigQuery:
    '''
    '''

    def __init__(self):
        '''
        '''
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ and 'BIG_QUERY_DETAILS' in os.environ:
            pass
        else:
            print('loading environment variables...')
            load_dotenv('.env')
        try:
            self.bq_client = bigquery.Client()#CREDENTIALS GOOGLE_APPLICATION_CREDENTIALS
            self.gs_client = storage.Client()#USES THE SAME CREDS AS 'GOOGLE_APPLICATION_CREDENTIALS'
            self.bq_connected = True
            self.gs_connected = True
            file_name = os.environ['BIG_QUERY_DETAILS']
            with open(file_name, 'r') as f:
                self.DB_CONFIG = json.loads(f.read())
                self.bq_string = '{BQ_PROJECT}.{BQ_DATASET}'.format(**self.DB_CONFIG)
                self.gs_bucket = self.DB_CONFIG['GS_BUCKET']
        except Exception as ex:
            print(ex)
            self.bq_connected = False
            self.gs_connected = True

    def connectToBq(self):
        '''
        '''
        if self.bq_connected:
            print('already connected...')
            return
        try:
            print(f'connecting to {self.bq_string}')
            self.bq_client = bigquery.Client()
            self.bq_connected = True
        except Exception as ex:
            print(ex)
            self.bq_connected = False
    
    def disconnectBq(self)-> None:
        '''
        '''
        try:
            print(f'disconnecting from {self.bq_string}')
            self.bq_client.close()
            self.connected = False
            print('disconnected...')
        except Exception as ex:
            print(ex)


    def connectToGs(self):
        '''
        '''
        if self.gs_connected:
            print('already connected...')
            return
        try:
            print(f'connecting to storage client')
            self.gs_client = storage.Client()
            self.qs_connected = True
        except Exception as ex:
            print(ex)
            self.gs_connected = False

    
    def disconnectGs(self):
        '''
        '''
        try:
            print(f'disconnecting from storage client')
            self.gs_client.close()
            self.gs_connected = False
            print('disconnected...')
        except Exception as ex:
            print(ex)

    def createSchemaFromJson(self,_json_file):
        '''
        '''
        return


    def getTableSchema(self, _table_name: str, _file_name: str=None):
        '''
        '''
        self.connectToBq()
        table  = self.bq_client.get_table(f'{self.bq_string}.{_table_name}')
        if _file_name ==None:
            f = StringIO('')
            self.bq_client.schema_to_json(table.schema, f)
            return json.loads(f.getvalue())
        return self.bq_client.schema_to_json(table.schema, _file_name)



    def getTable(self, _table_name):
        '''
        '''
        query_string = f'SELECT * FROM `{self.bq_string}.{_table_name}`'
        return self.runQuery(query_string)


    def runQuery(self, _query, _parameters= None):
        '''
        '''
        self.connectToBq()
        if _parameters == None:
            job  = self.bq_client.query(_query)
        else:
            _job_config = bigquery.QueryJobConfig(query_parameters=_parameters)
            job =  self.bq_client.query(query=_query, job_config= _job_config)
        rows = job.result()
        return rows

    def checkData(self, _schema, _csv_data):
        '''
        '''

        return True

    def insertRows(self, _table_name, _rows):
        '''
        '''
        self.connectToBq()
        bq_table = f'{self.bq_string}.{_table_name}'
        errors = self.bq_client.insert_rows_json(bq_table, _rows)  # Make an API request.
        if errors == []:
            return True
        else:
            raise Exception("{0}".format('\n'.join(errors)))

    def loadDataCSV(self, _table_name, _csv_data, _schema=None):
        '''
        if _schema= None, 
            table must exist, 
            we get json schema from bq and check with headings of csv data
        if _schema is given,
            we assume its a new table 
            check with headings of csv data
            and create the table and push into it using the schema
        '''
        if len(_csv_data) < 2:
            return None
        self.connectToBq()
        self.connectToGs()
        if _schema == None:
            try:
                _schema = self.getTableSchema(_table_name)
            except core_exceptions.NotFound as tex:
                raise Exception(f'{self.bq_string}.{_table_name} not found') from None
            except Exception as ex:
                raise ex
        else:
            #TO DO Code for inserting table
            raise Exception(f'{self.bq_string}.{_table_name} not found') from None
        gs_bucket = self.gs_client.bucket(self.gs_bucket)
        blob_name = _table_name + datetime.now().strftime('%Y%m%d%H%M%S') + '.csv' 
        blob = gs_bucket.blob(blob_name)
        with blob.open(mode='w') as f:
            for line in _csv_data:
                clean_line = [e.replace(',', '-') for e in line]
                try:
                    f.write(','.join(clean_line) + '\n')
                except Exception as ex:
                    print(ex)
                    print(line)
                    raise ex
        gs_link ='gs://' + blob.bucket.name + '/' +  blob.name
        self.disconnectGs()
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition=bigquery.WriteDisposition.WRITE_APPEND#THIS IS THE DEFAULT VALUE BUT STILL
        job_config.source_format=bigquery.SourceFormat.CSV#
        job_config.skip_leading_rows=1#IGNORE FIRST ROW
        job_config.schema = _schema
        job_config.autodetect = True#MAP SCHEMA AUTOMATICALLY
        bq_table = f'{self.bq_string}.{_table_name}'
        load_job = self.bq_client.load_table_from_uri(gs_link, bq_table, job_config=job_config)#
        rows = load_job.result()  # Waits for the job to complete.
        print(rows.output_rows)
        destination_table = self.bq_client.get_table(bq_table)  # Make an API request.
        print(f'total {destination_table.num_rows} rows.')
        return rows 

    def loadDataLocal(self, _table_name, _file_name, _format='CSV'):
        '''
        '''
        return True

    def loadDataCloudStorage(self, _table_name, _bucket_name, _file_name):
        '''
        '''
        return True

    def uploadeFileToCloudStorage(self, _file_name):
        '''
        '''
        return True

    def writeCsvToCloud(self, _csv_file):
        '''
        '''
        return True


    def writeParquetToCloud(self, _csv_file):
        '''
        '''
        return True
    

    def writeAvroToCloud(self, _csv_file):
        '''
        '''
        return True

    def testWrite(self):
        '''
        '''
        self.connectToGs()
        for gs_bucket in self.gs_client.list_buckets():
            print(gs_bucket)
            blob = gs_bucket.blob('path/to/new-blob.txt')
            with blob.open(mode='w') as f:
                f.write('hello how are you?\n')
                f.write('hello how are you?\n')
        return blob
    
    def downloadFile(self, _bucket, _path_to_file):
        '''
        '''
        self.connectToGs()
        gs_bucket = self.gs_client.bucket(_bucket)
        blob = gs_bucket.blob(_path_to_file)
        path , file_name = os.path.split(_path_to_file)
        with open(file_name, 'wb') as file_object:
            blob.download_to_file(file_object)
        return

    def getTableData(self, _table_name):
        '''
        '''
        self.connectToBq()
        bq_table_name = f'{self.bq_string}.{_table_name}'
        bq_table = self.bq_client.get_table(bq_table_name)
        print('Loaded {} rows and {} columns to {}'.format(
        bq_table.num_rows, len(bq_table.schema), bq_table_name))
        print(type(bq_table.schema))
        for column in bq_table.schema:
            print(column)



if __name__ == '__main__':
    print('works')
    bq =bigQuery()
    #WRITE YOUR TEST CODE HERE
    bq.disconnectBq()
    bq.disconnectGs()
    print(getLocalTime())

