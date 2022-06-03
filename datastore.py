from google.cloud import datastore

KIND = "Document"

def create_client():
    return datastore.Client()

def add_document(client: datastore.Client, filename:str, data: str):
    kind = KIND

    name = filename

    # Create incomplete key
    key = client.key(kind)

    # Creates unsaved document object in the datastore
    # Excludes description from index
    document = datastore.Entity(key, exclude_from_indexes=["data"])
    document["filename"] = filename
    document["data"] = data

    client.put(document)

    return document.key.id

def list_documents(client: datastore.Client):
    # Create a query against all of your objects of kind "Task"
    query = client.query(kind=KIND)

    return list(query.fetch())



