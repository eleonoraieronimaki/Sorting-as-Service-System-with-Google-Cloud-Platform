from google.cloud import datastore

KIND = "Job"

def create_client():
    return datastore.Client()

def add_job(filename:str, data: str, chunk_sort: int, chunk_pal: int):

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
    create_job_attributes(job, chunk_sort, chunk_pal)

    client.put(job)
    print(job)
    print(job.key)
    return job.key.id

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

    job['reduce'] = {}
    job['reduce']['running'] = "False"
    job['reduce']['done'] = "False"
    

if __name__=="__main__":
    add_job(filename="test2.txt", data="\n\n", chunk_sort=100, chunk_pal=100)