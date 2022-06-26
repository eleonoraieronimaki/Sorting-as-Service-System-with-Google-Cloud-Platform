from google.cloud import datastore
import time

KIND = "Job"
SORT_WORKER = "sort_worker"
PALINDROME_WORKER = "palindrome_worker"

def create_client():
    return datastore.Client()

def add_job(filename:str, data: str, chunk_sort: int, chunk_pal: int, num_offsets: int):

    client = create_client()

    kind = KIND

    # Create incomplete key
    key = client.key(kind)

    # Creates unsaved job object in the datastore
    job = datastore.Entity(key)
    job["filename"] = filename
    
    # get number of lines
    nlines = data.count("\n")

    # get job attributes
    create_job_attributes(job, chunk_sort, chunk_pal, num_offsets)

    client.put(job)

    job_key = job.key.id

    create_worker_docs(client, "sort", job_key, num_offsets)
    create_worker_docs(client, "palindrome", job_key, num_offsets)


    return job_key

def create_worker_docs(client: datastore.Client, worker_type: str, job_id: int, num_offsets: int):
    kind = ''

    if worker_type == "sort":
        kind = SORT_WORKER
    else:
        kind = PALINDROME_WORKER
    
    worker_list = []
    client = create_client()

    for i in range(num_offsets):
        key = client.key(kind)
        worker = datastore.Entity(key)
        worker["worker_num"] = i
        worker["job_id"] = job_id
        worker["done"] = False
        client.put(worker)
        worker_list.append(worker.key.id)
    
    return worker_list

def list_jobs(client: datastore.Client):
    # Create a query against all of your objects of kind "Task"
    query = client.query(kind=KIND)

    return list(query.fetch())


def create_job_attributes(job: datastore.Entity, chunk_sort: int, chunk_pal: int, num_offsets: int):
    job["sorted"] = False
    job["perc_sorted"] = 0
    job["chunk_sort"] = chunk_sort
    job["chunk_palindrome"] = chunk_pal
    job["palindromes"] = {}
    job["sort_workers"] = {}
    for i in range(num_offsets):
        job["sort_workers"][str(i)] = "False"
    job['palindrome_workers'] = {}
    job["palindrome_done"] = False
    job["sorting_done"] = False
    job['reduce'] = {}
    job['reduce']['running'] = "False"
    job['reduce']['done'] = "False"
    job["start_time"] = time.time()

def getJob(job_id: int):
    client = create_client()

    kind = KIND

    key = client.key(kind, int(job_id))

    # Use that key to load the entity
    job = client.get(key)

    # check if job exists. It should never go here..
    if not job:
        return False
    
    return job

def getWorkers(job_id):
    workers = []

    client = create_client()

    query = client.query(kind="palindrome_worker")
    query.add_filter("job_id", "=", int(job_id))
    query_results = list(query.fetch()) 
    workers.append(query_results)

    query = client.query(kind="sort_worker")
    query.add_filter("job_id", "=", int(job_id))
    query_results = list(query.fetch())
    workers.append(query_results)

    return workers

def getReducers(job_id):
    client = create_client()

    reducers = []

    query = client.query(kind="palindrome_reducer")
    query.add_filter("job_id", "=", int(job_id))
    query_results = list(query.fetch()) 
    reducers.append(query_results)

    query = client.query(kind="sort_reducer")
    query.add_filter("job_id", "=", int(job_id))
    query_results = list(query.fetch()) 
    reducers.append(query_results)

    return reducers
    

if __name__=="__main__":
    add_job(filename="test2.txt", data="\n\n", chunk_sort=100, chunk_pal=100)