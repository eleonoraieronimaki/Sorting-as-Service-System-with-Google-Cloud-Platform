from queue import Empty
from flask import Flask,render_template,request, jsonify
import datastore, storage
import logging
import os
import sys
 
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
        result = handle_storage(filename=filename, content=content)
        if result == False:
            return "Failed to upload object"
        
        return str(result)
 
@app.route('/index/', methods = ['POST', 'GET'])
def data():
    client = datastore.create_client()
    content = datastore.list_documents(client=client)
    print(content)
    return "hello"

def handle_storage(filename: str, content: str):
    job_id = datastore.add_job(filename, content)
    
    if job_id is Empty or job_id is None:
        return False
    
    destination_name = str(job_id)+'/'+filename
    result = storage.upload_doc(destination_blob_name=destination_name, contents=content)
    
    if result == False:
        return False

    return job_id

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)