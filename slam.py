import argparse
import multiprocessing
import os
import Queue
import tempfile
import time
import uuid

import keystoneclient.v2_0.client as keystoneclient
import swiftclient


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

def get_client():
    return swiftclient.Connection(preauthurl=SERVICE_URL, preauthtoken=AUTH_TOKEN)

def put_object(sc, fap):
    object_name = str(uuid.uuid4())
    sc.put_object(args.container_name, object_name, fap)


def get_blob():
    (fd, fname) = tempfile.mkstemp(prefix='slam', dir='/tmp')
    fap = open(fname, 'w+b')
    fap.write(os.urandom(TEST_OBJECT_SIZE))
    fap.seek(0)
    return (fap, fname)


def reap_blob(blob):
    blob[0].close()
    os.unlink(blob[1])


def nonrandom_worker_func(queue):
    blob = get_blob()

    while True:
        try:
            item = queue.get_nowait()
        except Queue.Empty:
            return

        sc = get_client()
        blob[0].seek(0)
        put_object(sc, blob[0])

    reap_blob(blob)


def random_worker_func(queue):
    while True:
        try:
            item = queue.get_nowait()
        except Queue.Empty:
            return

        blob = get_blob()
        sc = get_client()
        put_object(sc, blob[0])
        reap_blob(blob)



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
time_taken = time.time() - time_start
print '%d objects uploaded in %.2f seconds' % (args.objects, time_taken)
print 'Transactions per second: %.2f' % (args.objects/time_taken)
print 'Throughput (mb/s) %.2f' % (args.objects * args.object_size / time_taken)

for proc in procs:
    proc.join()
