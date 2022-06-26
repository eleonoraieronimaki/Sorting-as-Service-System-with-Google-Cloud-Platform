from bisect import bisect
import bisect
import queue
import time
from google.cloud import datastore, storage, firestore
from google.cloud.exceptions import Conflict


def reducer(event, context):

    client = firestore.Client()

    # The ID of your GCS bucket
    bucket_name = "test-project-docs"

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    kind = "sort_reducer"

    doc_ref = client.collection(u''+kind).document(u'' + job_id)
    doc_ref.set({
        'job_id': job_id,
        'perc': 0
    })

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
        doc_ref.update({
            'perc': perc
        })

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

    query = client.collection(u'Job').document(u''+ job_id)
    query.update({
        'sorting_done': True
    })

    doc_ref.update({
        'perc': 100,
        'end_time': time.time()
    })
    

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