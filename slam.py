import argparse
import ctypes
import multiprocessing
import os
import Queue
import random
import tempfile
import time
import uuid

import keystoneclient.v2_0.client as keystoneclient
import swiftclient

# Gotta love StackOverfliw
# http://stackoverflow.com/questions/4358285/is-there-a-faster-way-to-convert-an-arbitrary-large-integer-to-a-big-endian-sequ/4358429#4358429
PyLong_AsByteArray = ctypes.pythonapi._PyLong_AsByteArray
PyLong_AsByteArray.argtypes = [ctypes.py_object, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int]

parser = argparse.ArgumentParser(description='Slam a Swift endpoint.')
parser.add_argument('--objects', type=int, default=1, help='Number of objects to upload across all workers.')
parser.add_argument('--object-size', type=float, default=1, help='Size of each object to upload in MB.')
parser.add_argument('--workers', type=int, default=1, help='Number of parallel processes to utilize.')
parser.add_argument('--random', action='store_true', default=False,
                    help='Generate random data for every object. Otherwise, each worker will use the same random data for each object it uploads.')
parser.add_argument('--container-name', default='slam', help="Specify the container to load with objects. Defaults to 'slam'")
args = parser.parse_args()

AUTH_USER = os.getenv('OS_USERNAME')
AUTH_PASSWORD = os.getenv('OS_PASSWORD')
AUTH_TENANT_NAME = os.getenv('OS_TENANT_NAME')
AUTH_URL = os.getenv('OS_AUTH_URL')
TEST_OBJECT_SIZE = int(args.object_size * 1024 * 1024)

kc = keystoneclient.Client(username=AUTH_USER, password=AUTH_PASSWORD,
                           tenant_name=AUTH_TENANT_NAME, auth_url=AUTH_URL)

AUTH_TOKEN = kc.auth_token
SERVICE_URL = kc.service_catalog.url_for(service_type='object-store',
                                         endpoint_type='publicURL')

class PRNGFile:
    position = 0

    def __init__(self, prng, size):
        self.prng = prng
        self.size = size

    def close(self):
        pass

    def flush(self):
        pass

    def read(self, size=None):
        read_length = self.size - self.position
        if size != None and size < read_length:
            read_length = size
        if read_length == 0:
            return ''
        self.position += read_length
        random_data = self.prng.getrandbits(read_length * 8)
        byte_str = ctypes.create_string_buffer(read_length + 1)
        PyLong_AsByteArray(random_data, byte_str, len(byte_str), 0, 1)
        return byte_str.raw


def get_client():
    return swiftclient.Connection(preauthurl=SERVICE_URL, preauthtoken=AUTH_TOKEN)

def put_object(sc, blob):
    object_name = str(uuid.uuid4())
    sc.put_object(args.container_name, object_name, blob)

def nonrandom_worker_func(queue):
    (fd, fname) = tempfile.mkstemp(prefix='slam', dir='/tmp')
    blob = open(fname, 'w+b')
    blob.write(os.urandom(TEST_OBJECT_SIZE))
    blob.seek(0)

    while True:
        try:
            item = queue.get_nowait()
        except Queue.Empty:
            return

        sc = get_client()
        blob.seek(0)
        put_object(sc, blob)

    blob.close()
    os.unlink(fname)


def random_worker_func(queue):
    prng = random.Random()
    while True:
        try:
            item = queue.get_nowait()
        except Queue.Empty:
            return

        prng.seed(item)
        blob = PRNGFile(prng, TEST_OBJECT_SIZE)
        sc = get_client()
        put_object(sc, blob)

# Ensure the test container exists
sc = get_client()
sc.put_container(args.container_name)

queue = multiprocessing.Queue()
map(lambda x: queue.put(x), xrange(args.objects))

worker_func = random_worker_func if args.random else nonrandom_worker_func
procs = [multiprocessing.Process(target=worker_func, args=(queue,)) for x in xrange(args.workers)]
map(lambda p: p.start(), procs)

#pool = multiprocessing.Pool(args.workers)
#signal.signal(signal.SIGINT, kill)
#pool.map_async(put_object, xrange(args.objects), callback=job_callback)

time_start = time.time()
while True:
    qsize = queue.qsize()
    print '%d/%d objects uploaded' % (args.objects - qsize, args.objects)
    if qsize > 0:
        time.sleep(3)
    else:
        break

for proc in procs:
    proc.join()

time_taken = time.time() - time_start
print '%d objects uploaded in %.2f seconds' % (args.objects, time_taken)
print 'Transactions per second: %.2f' % (args.objects/time_taken)
print 'Throughput (mb/s) %.2f' % (args.objects * args.object_size / time_taken)
