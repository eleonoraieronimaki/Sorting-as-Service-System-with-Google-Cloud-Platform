import json, time

from google.cloud import datastore, storage
from google.cloud.exceptions import Conflict

def palindrome_reducer(event, context):

    datastore_client = datastore.Client()

    # The ID of your GCS bucket
    bucket_name = "saas-docs"

    # The attributes in message
    attributes = event["attributes"]

    job_id = attributes["job"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    query = datastore_client.query(kind="palindrome_worker")
    query.add_filter("job_id", "=", int(job_id))
    query_results = list(query.fetch())

    longest = -1
    count = 0
    word = ''

    for worker in query_results:
        if worker["longest"] > longest:
            longest = worker["longest"]
            word = worker["word"]
        count += worker["count"]
    
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

        palindromes = json.loads(json.dumps(job["palindromes"]))
        palindromes["longest"] = longest
        palindromes["count"] = count
        palindromes["word"] = word

        job["palindromes"] = palindromes
        job["palindrome_done"] = True

        for _ in range(5):
            try:
                datastore_client.put(job)
                time.sleep(5)
                break
            except Conflict:
                continue
            else:
                print("Transaction failed.")
