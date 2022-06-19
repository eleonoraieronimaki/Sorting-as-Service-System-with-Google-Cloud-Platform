import os
from google.cloud import pubsub_v1


credentials = '/Users/eleon/Desktop/Leiden University/Semester 2/Cloud Computing/Assignment 2/assignment2_cc/saas-0101.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
# create publisher
publisher = pubsub_v1.PublisherClient()

# topic_name = 'projects/saas-0101/topics/upload_text'.format(
#     project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
#     topic='upload_text',
# )
topic_name = 'projects/saas-0101/topics/upload_text'

# publisher.create_topic(name=topic_name)
data = 'A text has been uploaded!'
data = data.encode('utf-8')


# attributes = {
#     'garden': 'garden-001',
#     'temperature': '75.0',
#     'humidity': '60'
# }

# future = publisher.publish(topic_name, data,**attributes)

future = publisher.publish(topic_name, data)
print(future.result())


# subscription_name = 'projects/saas-0101/subscriptions/upload_text_sub'.format(
#     project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
#     sub='upload_text_sub',
# )

# def callback(message):
#     print(message.data)
#     message.ack()

# with pubsub_v1.SubscriberClient() as subscriber:
#     subscriber.create_subscription(
#         name=subscription_name, topic=topic_name)
#     future = subscriber.subscribe(subscription_name, callback)
