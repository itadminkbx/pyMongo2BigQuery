#!env/bin/python

import time
import time
import json
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pytz
import os
import sys
from dotenv import load_dotenv
from typing import List, Dict, Type, Union


def loadJsonFile(_file_name: str)-> Dict:
    '''
    '''
    with open(_file_name, 'r') as f:
        return json.loads(f.read())

def saveJsonFile(_file_name: str, _dict: Dict, _default: Type = str)-> None:
    '''
    '''
    with open(_file_name, 'w') as f:
        json.dump(_dict, f, default=str, indent=4)
    return 

def loadCsv(_fileName :str)-> dict:
    '''
    return list of dictonaries...
    '''
    _list = open(_fileName).readlines()
    _splitList = [[f.strip().replace('"','') for f in e.split('|')] for e  in _list]
    return [ dict(zip(_splitList[0],e)) for e in _splitList[1:]]

def saveCsv(_csv_data: List[List[str]], _fileName: str)-> None:
    '''
    '''
    with open(_fileName, 'w') as f:
        for row in _csv_data:
            f.write(','.join([f'"{e}"' for e in row]) + '\n')

def prettyPrintDict(_dict: Dict):
    '''
    '''
    print('-'*10)
    dict_string = json.dumps(_dict, default=str, indent=4)
    print(f'\n{dict_string}\n')
    print('-'*10)






def getProperDate(_date_string : str, _date_format : str)-> date:
    '''
    '''
    return_date = datetime.strptime(_date_string, _date_format).date()
    if return_date >= date.today():
        return_date-= relativedelta(years = 100)
    return return_date


def getLocalTime(_utc: Union[None, str, datetime]= None, _tmz: str = 'Asia/Yangon')-> datetime:
    '''
    '''
    if _utc == None:
        utc_time = datetime.utcnow()
    elif isinstance(_utc, str):
        utc_time = datetime.strptime(_utc, '%Y-%m-%d %H:%M:%S.%f')
    elif isinstance(_utc, datetime):
        utc_time = _utc
    else:
        raise Exception('Unsupported type {0}, getLocal(_utc), \n_utc needs to be of type str/datetime'.format(type(_utc)))
    return pytz.utc.localize(utc_time).astimezone(pytz.timezone(_tmz))



def saveDictCsv(_dict: dict, _file_name: str)-> None:
    '''
    dict is {key : {dict}}
    '''
    csv_header = ['_id']
    data = []
    for _id, row in _dict.items():
        keys = set(row.keys())
        new_headers = keys -set(csv_header) 
        missing_headers = set(csv_header) - keys
        for e in sorted(new_headers):
            csv_header.append(e)
        row_data = [_id]
        for e in csv_header[1:]:
            if e in row:
                row_data.append(f'"{str(row[e])}"')
            else:
                row_data.append('""')
        data.append(row_data)
    with open(_file_name, 'w') as f:
        f.write('|'.join(csv_header) + '\n')
        for row in data:
            f.write('|'.join(row) + '\n')
    print(f'file saved...{_file_name}')


def getEnv(_env_variable:str)-> str:
    '''
    '''
    if _env_variable in os.environ:
        pass
    else:
        load_dotenv('.env')
        if _env_variable in os.environ:
            pass
        else:
            raise Exception(f'Environment Variable {_env_variable} not set')
    return os.environ[_env_variable]

def getNumber(_str: str)-> str:
    '''
    '''
    try:
        return str(int(_str))
    except Exception as ex:
        return '0'

if __name__ == '__main__':
    print('works')
    a = getLocalTime()
    print (type(a))
    print(a)
