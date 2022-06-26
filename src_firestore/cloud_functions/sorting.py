import json
import random, time
from google.cloud import pubsub_v1
from google.cloud import datastore, storage, firestore
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
    PROJECT_ID = 'rich-crowbar-354417'


    client = firestore.Client()

    # The ID of your GCS bucket
    bucket_name = "test-project-docs"

    # The attributes in message
    attributes = event["attributes"]

    # The ID of your GCS object
    blob_name = attributes["obj"]

    print(blob_name)
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
    new_name = blob_parts[-1][:last_dot] + "_worker_" + str(worker_idjob) + blob_parts[-1][last_dot:]
    destination_name = blob_parts[0] + "/sort_results/" + new_name

    # store the result
    blob = bucket.blob(destination_name)
    blob.upload_from_string(sorted_contents)

    start_reduce = False

    doc_ref = client.collection(u'sort_worker').document(u'' + worker_idjob)
    doc_ref.update({
        'done': True
    })


    if worker_id == '0':
        # initialize variable, only need for the first time
        count_false = -1
        # while the not done workers are more than 1
        # because worker 0 will update their own document in the end of this
        while count_false != 0:
            query = client.collection(u'sort_worker')
            query_results = query.where(u'job_id', u'==', u''+job_id).stream()

            count_false = 0
            for work in query_results:
                tmp = work.to_dict()
                if not tmp["done"]:
                    count_false += 1
        
        if count_false == 0:
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
