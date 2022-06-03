from google.cloud import datastore
datastore_client = datastore.Client()

def hello_pubsub(event, context):
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
    import base64

    # print("Event: ", event)

    # print("""This Function was triggered by messageId {} published at {} to {}
    # """.format(context.event_id, context.timestamp, context.resource["name"]))

    query = datastore_client.query(kind="Document")
    data = query.fetch()
    
    for docs in data:
        print(docs)

    # if 'data' in event:
    #     name = base64.b64decode(event['data']).decode('utf-8')
    # else:
    #     name = 'World'
    print("Hello")
    # print('Hello {}!'.format(name))