from google.cloud import datastore, storage
from google.cloud import pubsub_v1
from google.cloud.exceptions import Conflict
import time
import string

def palindrome_worker(event, context):

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

    
    job_id = attributes["job"]
    start_offset = int(attributes["start_offset"])
    end_offset = int(attributes["end_offset"])
    worker_id = int(attributes["worker_id"])

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

    # store back the results
    query = datastore_client.query(kind="palindrome_worker")
    query.add_filter("job_id", "=", int(job_id))
    query.add_filter("worker_num", "=", int(worker_id))
    query_results = list(query.fetch())

    me = query_results[0]
    key = me.id
    # set our worker document to done, we will store it in the end
    me["done"] = True

    me["count"] = results[0]
    me["longest"] = results[1]
    me["word"] = results[2]
    
    if not worker_id == 0:
        # update worker document
        for i in range(5):
            try:
                with datastore_client.transaction():
                    key = datastore_client.key(
                        "palindrome_worker", key
                    )
                    worker = datastore_client.get(key)
                    if worker:
                        # worker = datastore.Entity(key)
                        worker.update({"count": results[0], "longest": results[1], "word": results[2], "done": True})
                        print(worker)
                        datastore_client.put(worker)
                        return
                time.sleep(2)
                break
            except Conflict:
                continue
            except Exception:
                continue
        return
    
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

        count_false = -1
        
        while not count_false == 1:
            time.sleep(2)
            query = datastore_client.query(kind="palindrome_worker")
            query.add_filter("job_id", "=", int(job_id))
            query_results = list(query.fetch())

            count_false = 0
            for work in query_results:
                if not work["done"]:
                    count_false += 1
        
        if count_false == 1:
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


    # update worker document
    datastore_client.put(me)

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