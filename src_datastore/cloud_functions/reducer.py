from bisect import bisect
import bisect
import queue
import time
from google.cloud import datastore, storage
from google.cloud.exceptions import Conflict


def reducer(event, context):

    datastore_client = datastore.Client()

    # The ID of your GCS bucket
    bucket_name = "saas-docs"

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    kind = "sort_reducer"

    # Create incomplete key
    key = datastore_client.key(kind)

    # Creates unsaved job object in the datastore
    reducer = datastore.Entity(key)
    reducer["job_id"] = int(job_id)
    reducer["perc"] = 0
    datastore_client.put(reducer)
    # Get storage client
    storage_client = storage.Client()
    # The bucket for retrieving objects
    bucket = storage_client.bucket(bucket_name)

    prefix = str(job_id) + '/' + 'sort_results/'

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=None)
    q = queue.Queue()

    docs = []
    for blob in blobs:
        obj = bucket.blob(blob.name)
        contents = obj.download_as_string().decode("utf-8")
        q.put(contents)
        docs.append(contents)
    total = q.qsize()

    while q.qsize() > 2:
        d1 = q.get()
        d2 = q.get()

        lines1 = d1.split('\n')
        lines2 = d2.split('\n')

        d12 = reduce(lines1, lines2)
        q.put(d12)

        perc = q.qsize() * 100 / total
        reducer["perc"] = perc
        datastore_client.put(reducer)

    final = ''
    if q.qsize() > 1:

        d1 = q.get()
        d2 = q.get()

        lines1 = d1.split('\n')
        lines2 = d2.split('\n')

        final = reduce(lines1, lines2)
    else:
        final = '\n'.join(q.get())
    
    destination_blob_name = str(job_id) + '/' + "sorted.txt"

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(final)

    # we need to update job document to say that the reducer is done

    # go to datastore (jobs) in order to update the job
    with datastore_client.transaction():
        # Create a key for an entity of kind "Task", and with the supplied
        # `task_id` as its Id
        key = datastore_client.key("Job", int(job_id))

        # Use that key to load the entity
        job = datastore_client.get(key)

        # check if job exists. It should never go here..
        if not job:
            raise ValueError(f"Job {job_id} does not exist.")

        job["sorting_done"] = True

        for i in range(10):
            try:
                time.sleep(5)
                datastore_client.put(job)
                break
            except Conflict:
                continue
            except Exception:
                continue
        else:
            print("Transaction failed.")
    
    reducer["perc"] = 100
    reducer["end_time"] = time.time()
    datastore_client.put(reducer)

def reduce(sorted1, sorted2):
    # Declaring a map.
    # using map as a inbuilt tool
    # to store elements in sorted order.
    mp=[]
  
    # Inserting values to a map.
    for i in range(len(sorted1)):
        bisect.insort(mp, sorted1[i])
         
    for i in range(len(sorted2)):
        bisect.insort(mp, sorted2[i])
     
    sorted = []
    
    for i in mp:
        sorted.append(i)
    
    sorted = '\n'.join(sorted)

    return sorted