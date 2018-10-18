import threading
import queue
import elasticsearch
import time
from func import *
import logging
from elasticsearch.helpers import bulk


def index_worker():
    global es
    es = get_client(ip)
    while True:
        i_data = q.get()
        data_sent = False
        while not data_sent:
            try:
                requests.get('http://' + ip + ':9200')
                send_data(es, 'threaded_collector', i_data)
                data_sent = True
                q.task_done()
            except requests.exceptions.ConnectionError:
                es = get_client(ip)
                send_data(es, 'threaded_collector', i_data)


def bulk_worker():
    global es
    es = get_client(ip)
    data_sent = False
    while True:
        while not data_sent:
            try:
                requests.get('http://' + ip + ':9200')
                bulk(es, gen_data(q))
                data_sent = True
                q.task_done()
            except requests.exceptions.ConnectionError:
                es = get_client(ip)
                bulk(es, gen_data(q))


def collector_worker():
    tempsnt, temprcv = netvals()
    while True:
        if q.qsize() < 1000:
            cpu_i = 0.001
        elif q.qsize() > 1000:
            cpu_i = 0.01
        data = get_cpuvals(cpu_i)
        data.update(get_mem())
        data.update(get_hdd())
        netsnt, netrcv = netvals()
        data.update(get_net(netsnt, netrcv, tempsnt, temprcv))
        data.update(get_time())
        print(q.qsize())
        q.put(data)
        tempsnt, temprcv = set_tmp(netsnt, netrcv)


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)

    collector_threads = 1
    bulk_threads = 0
    indexing_threads = 3
    ip = 'localhost'

    q = queue.Queue()

    i_threads = []
    b_threads = []
    c_threads = []

    for c in range(collector_threads):
        print('collector created ' + str(c))
        c = threading.Thread(target=collector_worker)
        c_threads.append(c)

    for i in range(indexing_threads):
        print('indexer created ' + str(i))
        i = threading.Thread(target=index_worker)
        i_threads.append(i)

    for b in range(bulk_threads):
        print('bulk indexer created ' + str(b))
        b = threading.Thread(target=bulk_worker)
        b_threads.append(b)

    for c in c_threads:
        c.start()

    for b in b_threads:
        b.start()

    for i in i_threads:
        i.start()




