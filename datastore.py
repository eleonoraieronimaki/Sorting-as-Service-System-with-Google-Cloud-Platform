from google.cloud import datastore

KIND = "Job"

def create_client():
    return datastore.Client()

def add_job(filename:str, data: str):

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
    create_job_attributes(job, nlines)

    client.put(job)

    return job.key.id

def list_jobs(client: datastore.Client):
    # Create a query against all of your objects of kind "Task"
    query = client.query(kind=KIND)

    return list(query.fetch())


def create_job_attributes(job: datastore.Entity, lines: int):
    job["sorted"] = False
    job["perc_sorted"] = 0
    job["lines"] = lines
    job["palindromes"] = {}
