from google.cloud import datastore, storage, firestore
from google.cloud import pubsub_v1
from google.cloud.exceptions import Conflict
import random, time

import string

def palindrome_worker(event, context):

    publisher = pubsub_v1.PublisherClient()
    PROJECT_ID = 'rich-crowbar-354417'

    client = firestore.Client()

    # The ID of your GCS bucket
    bucket_name = "test-project-docs"

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    
    job_id = attributes["job"]
    start_offset = int(attributes["start_offset"])
    end_offset = int(attributes["end_offset"])
    worker_id = attributes["worker_id"]
    worker_idjob = job_id + attributes["worker_id"]

    # Get storage client
    storage_client = storage.Client()
    # The bucket for retrieving objects
    bucket = storage_client.bucket(bucket_name)
    # Object to be retrieved
    blob = bucket.blob(blob_name)
    # Retrieve only the part we want to sort
    contents = blob.download_as_string(start=start_offset, end=end_offset)
    contents = contents.decode("utf-8")
    contents.translate(str.maketrans('', '', string.punctuation))
    words = contents.split()


    # call palindrome function
    results = palindrome(words)

    query = client.collection('palindrome_worker').document(u''+worker_idjob)
    query.update({
        "done": True,
        "count": results[0],
        "longest": results[1],
        "word": results[2]
    })

    if worker_id == '0':

        # initialize variable, only need for the first time
        count_false = -1
        # while the not done workers are more than 1
        # because worker 0 will update their own document in the end of this
        while count_false != 0:
            # fetch all palindrome_worker documents
            pali_workers = client.collection('palindrome_worker')
            workers = pali_workers.where(u'job_id', u'==', u''+ job_id).stream()

            # initialize variable
            count_false = 0
            # go through workers and check if they are not done
            for work in workers:
                tmp = work.to_dict()
                if not tmp["done"]:
                    # if not done, increase variable
                    count_false += 1
            
        
        if count_false == 0:
            topic_path = publisher.topic_path(PROJECT_ID, "palindrome_reducer")

            data = f""

            data = data.encode("utf-8")
            # Publishes a message
            try:
                publish_future = publisher.publish(topic_path, data=data, job=job_id, obj=blob_name)
                publish_future.result()  # Verify the publish succeeded
            except Exception as e:
                print(e)
                return (e, 500)

def palindrome(section):
    count = 0
    max_len = 0
    longest_word = ''
    for word in section:
        if word == ' ':
            continue
        if word == word[::-1]:
            count+=1
            temp = len(word)
            if temp > max_len:
                max_len = temp
                longest_word = word
    return count, max_len, longest_word