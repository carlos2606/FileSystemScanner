import os
import grp
import itertools
import multiprocessing
import schedule
import time
from pwd import getpwuid
from datetime import datetime as dt


def getsize(filename):
    return os.path.getsize(filename)


def getname(root, filename):
    return os.path.join(root, filename)


def getctime(path):
    return dt.fromtimestamp(os.path.getctime(path)).strftime('%Y-%m-%d %H:%M:%S')


def getmtime(path):
    return dt.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')


def getatime(path):
    return dt.fromtimestamp(os.path.getatime(path)).strftime('%Y-%m-%d %H:%M:%S')


def find_owner(filename):
    return getpwuid(os.stat(filename).st_uid).pw_name


def find_group(path):
    gid = os.stat(path).st_gid
    group = grp.getgrgid(gid)[0]
    return group


def get_access_bits(path):
    bits = oct(os.stat(path)[0])[-3:]
    return bits


def worker(path):
    '''
    Gathers data from one file
    '''
    realpath = path.split('/')[:-1]
    try:
        data = {
            'file': path.split('/')[-1],
            'path': '/'.join(realpath),
            'changedTime': getctime(path),
            'modifiedTime': getmtime(path),
            'accessedTime': getatime(path),
            'size': os.path.getsize(path),
            'owner': find_owner(path),
            'group': find_group(path),
            'accesBits': get_access_bits(path)
        }
        print (data)
    except:
        pass


def scanner():
    '''
    Parallel file system walk using multiple processes.
    Each process will run a worker.
    '''

    path = input("Please enter a directory to be scanned: \n")
    if os.path.exists(path):
        with multiprocessing.Pool(8) as Pool:  # pool of 8 processes

            walk = os.walk(path, followlinks=False)
            fn_gen = itertools.chain.from_iterable((os.path.join(root, file)
                                                    for file in files)
                                                   for root, dirs, files
                                                   in walk)

            # parallel processing
            results = Pool.map(
                worker, [j for j in fn_gen if os.path.isfile(j)])


def scan():

    schedule.every(3).seconds.do(scanner)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    scan()
