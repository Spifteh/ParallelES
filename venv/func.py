import psutil
from elasticsearch import Elasticsearch
import time
import requests
from datetime import datetime
import sys
import logging
logging.basicConfig(level=logging.ERROR)

if __name__ == "__main__":
    import doctest
    doctest.testmod()

def get_client(ip='localhost'):
    """
    >>> client = get_client('localhost')
    >>>

    """
    while True:
        try:
            requests.get('http://' + ip + ':9200')
            print("Connected to: http://" + ip + ':9200')
            return Elasticsearch(['http://' + ip + ':9200'])
        except requests.exceptions.ConnectionError:
            print("Unable to connect, retrying...")


def stringChoppedToFloat(inputString, chopUp, chopLow):

    """
    >>> stringChoppedToFloat('hello75494758test', 'hello', 'test')
    75494758.0

    >>> stringChoppedToFloat('hello1test', 'hello', 'test')
    1.0
    """
    inputString = str(inputString)
    return float(stringChopUp(stringChopLow(inputString, chopUp),chopLow))

def stringChopUp(inputString, chopUp):

    """
    >>> stringChopLow('hello my name is test', 'hello')
    ' my name is test'

    >>> stringChopLow('tests are the worst', 'tests')
    ' are the worst'

    >>> stringChopLow('tests are the worst', 'tests ')
    'are the worst'

    >>> stringChopLow('tests are the worst', 'hello ')
    'failed to find chop value in input; tests are the worst hello '
    """

    if type(inputString and chopUp) is str:
        if chopUp in inputString:
            return inputString[:inputString.find(chopUp)]
        else:
            return 'failed to find chop value in input; ' + inputString + ' ' + chopUp
    else:
        return 'incompatible types; ' + inputString + ' ' + chopUp

def stringChopLow(inputString, chopLow):

    """
    >>> stringChopUp('hello my name is test', 'test')
    'hello my name is '

    >>> stringChopUp('hello my name is test', 'hello')
    ''

    >>> stringChopUp('tests are the worst', 'hello')
    'failed to find chop value in input; tests are the worst hello'
    """

    if type(inputString and chopLow) is str:
        if chopLow in inputString:
            return inputString[inputString.find(chopLow) + len(chopLow):]
        else:
            return 'failed to find chop value in input; ' + inputString + ' ' + chopLow
    else:
        return 'incompatible types; ' + inputString + ' ' + chopLow

def netvals():

    """
    >>> r1, r2 = netvals()
    >>> isinstance(r1, float)
    True
    >>> isinstance(r2, float)
    True
    """

    net = psutil.net_io_counters()
    values1 = stringChoppedToFloat(net,'ts_sent=', ', packets_recv=')
    values2 = stringChoppedToFloat(net,'ts_recv=', ', errin=')
    if type(values1 or values2) is str:
        return 0.0, 0.0
    else:
        return values1, values2

def get_cpuvals(inteval = 1):
    """
    >>> doc = get_cpuvals()
    >>> type(doc)
    <class 'dict'>
    >>> type(doc.get('CPUCore1'))
    <class 'float'>
    >>> type(doc.get('CPUCore2'))
    <class 'float'>
    >>> type(doc.get('CPUCore3'))
    <class 'float'>
    >>> type(doc.get('CPUCore4'))
    <class 'float'>
    >>> type(doc.get('CPU'))
    <class 'float'>

    """
    cpu_array = psutil.cpu_percent(inteval, percpu=True)
    return {
    'CPUCore1': cpu_array[0],
    'CPUCore2': cpu_array[1],
    'CPUCore3': cpu_array[2],
    'CPUCore4': cpu_array[3],
    'CPU': sum(cpu_array)/4
    }
def get_mem():
    """
    >>> doc = get_mem()
    >>> type(doc)
    <class 'dict'>
    >>> type(doc.get('MEM'))
    <class 'float'>
    """
    return {
    'MEM': stringChoppedToFloat(psutil.virtual_memory(),'percent=', ', used'),
    }

def get_hdd():
    """
    >>> doc = get_hdd()
    >>> type(doc)
    <class 'dict'>
    >>> type(doc.get('HDD'))
    <class 'float'>
    """
    return {
    'HDD': stringChoppedToFloat(psutil.disk_usage('/'),'percent=', ')'),
    }

def get_net(netsnt, netrcv, tempsnt, temprcv):
    """
    >>> doc = get_net(155.0,155.0,150.0,150.0)
    >>> type(doc)
    <class 'dict'>
    >>> type(doc.get('NET-PKT-SNT'))
    <class 'float'>
    >>> type(doc.get('NET-PKT-RCV'))
    <class 'float'>
    >>> doc.get('NET-PKT-SNT')
    5.0
    >>> doc.get('NET-PKT-RCV')
    5.0

    """
    return {
    'NET-PKT-SNT': (netsnt - tempsnt),
    'NET-PKT-RCV': (netrcv - temprcv),
    }

def get_time():
    """
    >>> doc = get_time()
    >>> type(doc)
    <class 'dict'>
    >>> type(doc.get('timestamp'))
    <class 'datetime.datetime'>
    """
    return {
    'timestamp': datetime.now()
    }

def set_tmp(netsnt, netrcv):
    """
    >>> tmp1, tmp2 = set_tmp(155.0,155.0)
    >>> type(tmp1)
    <class 'float'>
    >>> type(tmp2)
    <class 'float'>
    >>> tmp1
    155.0
    >>> tmp2
    155.0

    """
    tempsnt = netsnt
    temprcv = netrcv
    return tempsnt, temprcv

def send_data(client, index, data):
    """
    >>> testclient = get_client('localhost')
    >>> res1 = testclient.search(index='testingindex', body={"query": {"match_all": {}}})
    >>> send_data(testclient,'testingindex',{'test':10})
    >>> time.sleep(1)
    >>> res2 = testclient.search(index='testingindex', body={"query": {"match_all": {}}})
    >>> print((res2['hits']['total']) - (res1['hits']['total']))
    1

    """
    client.index(index=index, doc_type="data", body=data)
