import psutil
from elasticsearch import Elasticsearch
import time
import requests
from datetime import datetime, timedelta
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


def gen_data(queue):
    for i in range(500):
        b_data = queue.get()
        if b_data is None:
            time.sleep(0.1)
        else:
            yield {
                b_data
            }

def string_chopped_to_float(input_string, chop_up, chop_low):

    """
    >>> string_chopped_to_float('hello75494758test', 'hello', 'test')
    75494758.0

    >>> string_chopped_to_float('hello1test', 'hello', 'test')
    1.0
    """
    input_string = str(input_string)
    return float(string_chop_up(string_chop_low(input_string, chop_up), chop_low))


def string_chop_up(input_string, chop_up):

    """
    >>> string_chop_low('hello my name is test', 'hello')
    ' my name is test'

    >>> string_chop_low('tests are the worst', 'tests')
    ' are the worst'

    >>> string_chop_low('tests are the worst', 'tests ')
    'are the worst'

    >>> string_chop_low('tests are the worst', 'hello ')
    'failed to find chop value in input; tests are the worst hello '
    """

    if type(input_string and chop_up) is str:
        if chop_up in input_string:
            return input_string[:input_string.find(chop_up)]
        else:
            return 'failed to find chop value in input; ' + input_string + ' ' + chop_up
    else:
        return 'incompatible types; ' + input_string + ' ' + chop_up


def string_chop_low(input_string, chop_low):

    """
    >>> string_chop_up('hello my name is test', 'test')
    'hello my name is '

    >>> string_chop_up('hello my name is test', 'hello')
    ''

    >>> string_chop_up('tests are the worst', 'hello')
    'failed to find chop value in input; tests are the worst hello'
    """

    if type(input_string and chop_low) is str:
        if chop_low in input_string:
            return input_string[input_string.find(chop_low) + len(chop_low):]
        else:
            return 'failed to find chop value in input; ' + input_string + ' ' + chop_low
    else:
        return 'incompatible types; ' + input_string + ' ' + chop_low


def netvals():

    """
    >>> r1, r2 = netvals()
    >>> isinstance(r1, float)
    True
    >>> isinstance(r2, float)
    True
    """

    net = psutil.net_io_counters()
    values1 = string_chopped_to_float(net, 'ts_sent=', ', packets_recv=')
    values2 = string_chopped_to_float(net, 'ts_recv=', ', errin=')
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
    'MEM': string_chopped_to_float(psutil.virtual_memory(), 'percent=', ', used'),
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
    'HDD': string_chopped_to_float(psutil.disk_usage('/'), 'percent=', ')'),
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
    'timestamp': datetime.now()+ timedelta(hours=-1)
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
