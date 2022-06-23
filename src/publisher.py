from google.cloud import pubsub_v1

project_id = "saas-0101"
# sort_topic_id = "upload_text"

SORTING_TOPIC = 'upload_text'

PALINDROME_TOPIC = 'palindrome_topic'

def getClient() -> pubsub_v1.PublisherClient():
    return pubsub_v1.PublisherClient()

def sendMessages(mode, job_id, obj_name, offsets):
    topic = ''
    if mode == "sort":
        topic = SORTING_TOPIC
    else:
        topic = PALINDROME_TOPIC
    publisher = getClient() 
    topic_path = publisher.topic_path(project_id, topic)
    publish_futures = []

    for i,chunk in enumerate(offsets):

        data = f"Message number {i}"

        data = data.encode("utf-8")

        start = str(chunk[0])
        end = str(chunk[1])
        obj = obj_name

        future = publisher.publish(
            topic_path, data, job=str(job_id), obj=obj_name, start_offset=start, end_offset=end, worker_id=str(i)
        )
        publish_futures.append(future.result())
    
    return publish_futures


if __name__ == '__main__':
    offsetList = [(0,10), (11, 20), (21, 30)]
    job_id = str(-9999)
    obj_name = '/5643280054222848/test.txt'
    sendMessages("sort", job_id=job_id, obj_name=obj_name, offsets=offsetList)
