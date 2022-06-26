import json, time

from google.cloud import datastore, storage
from google.cloud.exceptions import Conflict
from google.cloud import firestore

def palindrome_reducer(event, context):

    client = firestore.Client()

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    kind = 'palindrome_reducer'

    worker_id = job_id
    doc_ref = client.collection(u''+kind).document(u'' + worker_id)
    doc_ref.set({
        'job_id': job_id,
        'perc': 0
    })

    # Create a reference to the cities collection
    query = client.collection(u'palindrome_worker')

    # Create a query against the collection
    query_results = query.where(u'job_id', u'==', u''+ job_id)
    query_len = len(query_results.get())

    longest = -1
    count = 0
    word = ''
    index = 0
    for i in query_results.stream():
        worker = i.to_dict()
        print(worker)
        if worker["longest"] > longest:
            longest = worker["longest"]
            word = worker["word"]
        count += worker["count"]

        if index >= (query_len / 2):
            doc_ref.update({
                'perc': 50
            })
        index += 1


    # Create a reference to the cities collection
    query = client.collection(u'Job').document(u''+ job_id)
    query.update({
        "palindromes": {
        "longest": longest,
        "count": count,
        "word": word
        },
        "palindrome_done": True
    })

    doc_ref.update({
        'perc': 100,
        'end_time': time.time()
    })
