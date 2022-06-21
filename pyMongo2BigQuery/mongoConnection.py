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

import os
import json
import sys
import time
from datetime import datetime, timedelta, date
import pytz

from dotenv import load_dotenv
import pymongo
from bson import ObjectId

from .utils import loadJsonFile, saveJsonFile




class mongoConnection:

    def __init__(self):
        try:
            if 'DB_CREDS' in os.environ:
                pass
            else:
                load_dotenv('.env')
            file_name = os.environ['DB_CREDS']
            with open(file_name, 'r') as f:
                self.DB_CREDS = json.loads(f.read())
            with open('customer_ignore.json', 'r') as c:
                self.customer_ignore = json.loads(c.read())
            with open('doctor_ignore.json', 'r') as d:
                self.doctor_ignore = json.loads(d.read())
            self.nowrite_flag = os.getenv('NO_UPDATE_INSERT') == 'YES'
            self.reports_directory = os.getenv('EXPORT_PATH')
            self.connection_string = 'mongodb+srv://{username}:{password}@{url}/{database}?{permissions}'.format(**self.DB_CREDS)
            self.connected = False
            self.client = None
            self.db = None
        except Exception as ex:
            print(ex)
            raise ex


    def __del__(self):
        '''
        '''
        if self.connected:
            try:
                self.client.close()
            except Exception as ex:
                pass

    def connectToDb(self):
        '''
        '''
        if self.connected:
            print('already connected')
            return
        try:
            print(f'connecting to {self.connection_string}')
            self.client = pymongo.MongoClient(self.connection_string)
            self.db = self.client.get_default_database()#as we are passing the db in the connection string
            self.connected = True
            print('connection success')
        except Exception as ex:
            self.connected = False 
            print(ex)

    

    def disconnectDb(self):
        '''
        '''
        if self.connected:
            try:
                print(f'disconnecting from {self.connection_string}')
                self.client.close()
                self.connected = False
                print('disconnected...')
            except Exception as ex:
                print(ex)

    def getTableList(self):
        '''
        '''
        self.connectToDb()
        return self.db.list_collection_names()

    def getTable(self, _table_name, _filter= None, _projection = None):
        '''
        '''
        self.connectToDb()
        if _filter == None:
            _filter = {}
        collection = self.db.get_collection(_table_name)
        if _projection == None:
            return collection.find(_filter)
        else:
            return collection.find(_filter, _projection)

    def getTableDict(self, _table_name, _filter= None, _key = None):
        '''
        '''
        if _filter == None:
            _filter = {}
        self.connectToDb()
        collection = self.db.get_collection(_table_name)
        cur = collection.find(_filter)
        table_dict= {}
        for e in cur:
            if _key == None:
                c_id = str(e['_id'])
                headers = [ h for h in e.keys() if h not in ['_id', '__v']]
            else:
                if _key not in e:
                    continue
                c_id = e[_key]
                headers = [ h for h in e.keys() if h not in ['_id', '__v', _key]]
            table_dict[c_id] = { header : e[header] for header in headers}
        return table_dict

    def exportTable(self, _table_name, _format='json', _file_name=''):
        '''
        '''
        formats = ['json', 'csv']
        print(f'exportiing {_table_name} to {_format} format')
        if _format not in formats:
            raise Exception(f'{_format} not supported {os.linesep}please choose from {os.linesep}{os.linesep.join(formats)}')
        table_data = self.getTableDict(_table_name)
        print('got data')
        if _file_name != '':
            file_name = _file_name
        else:
            file_name = _table_name + datetime.now().strftime('-%Y-%m-%d_%H%M%S.') + _format
        if _format == 'json':
            with open(file_name, 'w') as f:
                json.dump(table_data, f,indent=4, default=str)
        elif _format == 'csv':
            csv_header = ['_id']
            data = []
            for _id, row in table_data.items():
                keys = set(row.keys())
                new_headers = keys -set(csv_header) 
                missing_headers = set(csv_header) - keys
                for e in sorted(new_headers):
                    csv_header.append(e)
                row_data = [_id]
                for e in csv_header[1:]:
                    if e in row:
                        row_data.append(str(row[e]))
                    else:
                        row_data.append('')
                data.append(row_data)
            with open(file_name, 'w') as f:
                f.write('|'.join(csv_header) + '\n')
                for row in data:
                    f.write('|'.join(row) + '\n')
            print(f'file saved...{file_name}')


    def getTableDictFromFile(self, _table_name, _filter = None, _timestamp_flag = False):
        '''
        load a json file - if file is not there create the jsonfile for future use
        _timestamp_flag creates a new file in that _time_stamp
        '''
        if _filter == None:
            _filter = {}
        _fileName = f'{_table_name}.json'
        if _timestamp_flag:
            _fileName = f'{table_name}-{datetime.now().strptime("%Y-%m-%d_%H%M%S")}.json'
        try:
            return loadJsonFile(_fileName)
        except FileNotFoundError as fex: 
            table_dict = self.getTableDict(_table_name, _filter)
            saveJsonFile(_fileName, table_dict)
            return table_dict
        except Exception as ex:#LOGGING CODE REQUIRED TO UNDERSTAND THIS EXCEPTION
            table_dict = self.getTableDict(_table_name, _filter)
            saveJsonFile(_fileName, table_dict)
            return table_dict


    



    def insertRecord(self, _table_name, _record):
        '''
        '''
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'inserting....{_table_name}{os.linesep}')
        self.connectToDb()
        collection = self.db.get_collection(_table_name)
        try:
            return collection.insert_one(_record)
        except Exception as ex:
            print(ex)
            self.disconnectDb()
            raise Exception(ex.message)


    def insertRecords(self, _table_name, _records):
        '''
        '''
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        if _records == []:
            print('empty insert')
            return
        print(f'inserting....{_table_name}{os.linesep}')
        self.connectToDb()
        collection = self.db.get_collection(_table_name)
        try:
            return collection.insert_many(_records)
        except Exception as ex:
            print(ex)
            self.disconnectDb()
            raise Exception(ex.message)

    def updateTable(self, _table_name, _update, _filter= None):
        '''
        '''
        if _filter == None:
            _filter = {}
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'updating....{_table_name}{os.linesep}')
        self.connectToDb()
        try:
            collection = self.db.get_collection(_table_name)
            return collection.update_many(_filter, _update)
        except Exception as ex:
            print(ex)
            self.disconnectDb()


    def updateRecord(self, _table_name, _update, _filter= None):
        '''
        '''
        if _filter == None:
            _filter = {}
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'updating....{_table_name}{os.linesep}')
        self.connectToDb()
        try:
            collection = self.db.get_collection(_table_name)
            return collection.update_one(_filter, _update)
        except Exception as ex:
            print(ex)
            self.disconnectDb()

    def dropTable(self, _table_name):
        '''
        '''
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'dropping table....{_table_name}{os.linesep}')
        self.connectToDb()
        try:
            collection = self.db.get_collection(_table_name)
            result = collection.drop()
            return result
        except Exception as ex:
            print(ex)
            self.disconnectDb()

    def dropRecords(self, _table_name, _filter= None): 
        '''
        '''
        if _filter == None:
            _filter = {}
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'delete records....{_table_name}{os.linesep}')
        self.connectToDb()
        try:
            collection = self.db.get_collection(_table_name)
            result = collection.delete_many(_filter)
            return result
        except Exception as ex:
            print(ex)
            self.disconnectDb()

    def dropRecord(self, _table_name, _filter= None):
        '''
        '''
        if _filter == None:
            _filter = {}
        if self.nowrite_flag:
            print('Not connected to DB')
            return
        print(f'delete single record....{_table_name}{os.linesep}')
        self.connectToDb()
        try:
            collection = self.db.get_collection(_table_name)
            result = collection.delete_one(_filter)
            return result
        except Exception as ex:
            print(ex)
            self.disconnectDb()
         
 

if __name__ == '__main__':
    print('works')
    db = mongoConnection()
    try:
        print(db.customer_ignore)
        print(db.doctor_ignore)
    except Exception as ex:
        print(ex)
    db.disconnectDb()


