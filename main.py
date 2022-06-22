import os
import json
import time

from google.cloud import pubsub_v1
from google.cloud import datastore, storage
from google.cloud.exceptions import Conflict


def sort_worker(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    publisher = pubsub_v1.PublisherClient()
    PROJECT_ID = 'saas-0101'
    datastore_client = datastore.Client()

    # The ID of your GCS bucket
    bucket_name = "saas-docs"

    # The attributes in message
    attributes = event["attributes"]

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
    # get lines
    lines = contents.split('\n')
    lines = list(filter(None, lines))
    # sort lines
    sorted_txt = sorted(lines, key=None)

    sorted_contents = '\n'.join(sorted_txt)
    # create new name with subfolder for sorting result
    blob_parts = blob_name.split('/')
    ch = '.'
    dot_idx = [i for i, ltr in enumerate(blob_parts[-1]) if ltr == ch]
    last_dot = dot_idx[-1]
    new_name = blob_parts[-1][:last_dot] + "_worker_" + str(worker_id) + blob_parts[-1][last_dot:]
    destination_name = blob_parts[0] + "/sort_results/" + new_name

    # store the result
    blob = bucket.blob(destination_name)
    blob.upload_from_string(sorted_contents)

    start_reduce = False

    query = datastore_client.query(kind="sort_worker")
    query.add_filter("job_id", "=", int(job_id))
    query.add_filter("worker_num", "=", int(worker_id))
    results = list(query.fetch())

    me = results[0]
    
    # set our worker document to done, we will store it in the end
    me["done"] = True
    print(me)


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


        query = datastore_client.query(kind="sort_worker")
        query.add_filter("job_id", "=", int(job_id))
        results = list(query.fetch())

        count_false = 0
        for work in results:
            if not work["done"]:
                count_false += 1
        
        if count_false == (len(results) - 1):
            # fix topic
            topic_path = publisher.topic_path(PROJECT_ID, "Reduce")

            data = f""

            data = data.encode("utf-8")
            # Publishes a message
            try:
                publish_future = publisher.publish(topic_path, data=data, job=job_id, obj=blob_name)
                publish_future.result()  # Verify the publish succeeded
            except Exception as e:
                print(e)
                return (e, 500)
            
            # get reduce dict
            reduce = json.loads(json.dumps(job["reduce"]))
            
            # set reduce in the dictionary
            reduce['running'] = "True"

            # set it in the job
            job["reduce"] = reduce

            datastore_client.put(job)
        
    datastore_client.put(me)