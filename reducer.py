from bisect import bisect
import os
import json
import bisect
import queue

from google.cloud import pubsub_v1
from google.cloud import datastore, storage

def reducer(event, context):

    publisher = pubsub_v1.PublisherClient()
    PROJECT_ID = 'saas-0101'
    datastore_client = datastore.Client()

    # The ID of your GCS bucket
    bucket_name = "saas-docs"

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

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

    while q.qsize() > 2:
        d1 = q.get()
        d2 = q.get()

        lines1 = d1.split('\n')
        lines2 = d2.split('\n')

        d12 = reduce(lines1, lines2)
        q.put(d12)
    
    d1 = q.get()
    d2 = q.get()

    lines1 = d1.split('\n')
    lines2 = d2.split('\n')

    final = reduce(lines1, lines2)

    destination_blob_name = str(job_id) + '/' + "sorted.txt"

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(final)



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