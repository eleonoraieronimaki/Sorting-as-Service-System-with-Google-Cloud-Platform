from google.cloud import firestore
import time
import random
KIND = "Job"
SORT_WORKER = "sort_worker"
PALINDROME_WORKER = "palindrome_worker"

def create_client():
    return firestore.Client(project='rich-crowbar-354417')

def add_job(filename:str, data: str, chunk_sort: int, chunk_pal: int, num_offsets: int):

    client = create_client()

    kind = KIND

    t = time.time()
    r = random.randint(0,999999)
    filename = filename.replace('.','')
    t = str(t).replace('.','')
    doc_name = u''+str(t)+filename+str(r)
    # Creates unsaved job object in the datastore
    doc_ref = client.collection(u'Job').document(doc_name)
    doc_ref.set({
        u'filename': u'' + filename,
        u'start_time': time.time(),
        u'sorting_done': False,
        u'palindrome_done': False,
        u'sort_chunk': chunk_sort,
        u'chunk_pal': chunk_pal
    })

    job_key = doc_name

    create_worker_docs("sort", job_key, num_offsets)
    create_worker_docs("palindrome", job_key, num_offsets)

    return job_key

def create_worker_docs(worker_type: str, job_id: int, num_offsets: int):
    client = create_client()

    kind = ''

    if worker_type == "sort":
        kind = SORT_WORKER
    else:
        kind = PALINDROME_WORKER
    
    worker_list = []
    client = create_client()

    for i in range(num_offsets):
        worker_id = job_id + str(i)
        doc_ref = client.collection(u''+kind).document(worker_id)
        doc_ref.set({
            'worker_num': i,
            'job_id': job_id,
            'done': False
        })

def getJob(job_id: int):
    client = create_client()

    kind = KIND

    job = client.collection(u'' + kind).document(u''+job_id).get()

    # check if job exists. It should never go here..
    if not job:
        return False
    
    return job.to_dict()

def getWorkers(job_id):
    workers = []

    client = create_client()

    pali = client.collection(u''+PALINDROME_WORKER)
    results = pali.where(u'job_id', u'==', u''+job_id)
    palindrome_worker_len = len(results.get())
    workers.append(results.stream())

    sort = client.collection(u''+SORT_WORKER)
    results = sort.where(u'job_id', u'==', u''+job_id)
    sort_worker_len = len(results.get())
    workers.append(results.stream())

    workers.append(palindrome_worker_len)
    workers.append(sort_worker_len)


    return workers

def getReducers(job_id):
    client = create_client()

    reducers = []

    pali = client.collection(u'palindrome_reducer')
    results = pali.where(u'job_id', u'==', u''+job_id)
    palindrome_reducer_len = len(results.get())
    reducers.append(results.stream())

    sort = client.collection(u'sort_reducer')
    results = sort.where(u'job_id', u'==', u''+job_id)
    sort_reducer_len = len(results.get())
    reducers.append(results.stream())
    
    reducers.append(palindrome_reducer_len)
    reducers.append(sort_reducer_len)
    return reducers
    

if __name__=="__main__":
    add_job(filename="test2.txt", data="\n\n", chunk_sort=100, chunk_pal=100)