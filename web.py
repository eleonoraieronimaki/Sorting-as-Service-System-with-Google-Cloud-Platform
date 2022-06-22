from queue import Empty
from flask import Flask,render_template,request, jsonify
import numpy as np
import datastore, storage, publisher
import os
 
SORT_CHUNK = 1000
PALINDROME_CHUNK = 1000

app = Flask(__name__)
 
@app.route('/form')
def form():
    return render_template('form.html')

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        print(request.files['file'].filename)
        if request.files['file'].filename == '':
            return "No file uploaded"
        
        filename = request.files['file'].filename
        content = request.files['file'].read()
        content = content.decode("utf-8")
        offsets = create_offsets(content, SORT_CHUNK)
        result = handle_storage(filename=filename, content=content, chunk_sort=SORT_CHUNK, chunk_palindrome=PALINDROME_CHUNK, num_offsets=len(offsets))
        job_id = result[0]
        destination_name = result[1]
        sorting_messages = publisher.sendSorting(job_id=job_id, obj_name=destination_name, offsets=offsets)
        if result == False:
            return "Failed to upload object"
        
        return str(job_id)
 
@app.route('/index/', methods = ['POST', 'GET'])
def data():
    client = datastore.create_client()
    content = datastore.list_documents(client=client)
    print(content)
    return "hello"

def handle_storage(filename: str, content: str, chunk_sort: int, chunk_palindrome: int, num_offsets: int):
    job_id = datastore.add_job(filename, content, chunk_sort, chunk_palindrome, num_offsets)
    
    if job_id is Empty or job_id is None:
        return False
    
    destination_name = str(job_id)+'/'+filename
    result = storage.upload_doc(destination_blob_name=destination_name, contents=content)
    
    if result == False:
        return False

    return job_id, destination_name

def create_offsets(text, chunk_len):
    l = len(text)
    parts = int(np.ceil(l / chunk_len))
    end = -1
    offsets = []
    for i in range(parts):
        start = end + 1
        end = start + chunk_len
        n = text[end:].split('\n')[0]
        prev_offset = len(n)
        end = end+prev_offset
        if end > l:
            end = l
            offsets.append((start, end))
            break
        offsets.append((start, end))
    return offsets

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)