from flask import Flask,render_template,request, jsonify
import datastore
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
        client = datastore.create_client()
        id = datastore.add_document(client=client, filename=filename, data=content)
        return str(id)
 
@app.route('/index/', methods = ['POST', 'GET'])
def data():
    client = datastore.create_client()
    content = datastore.list_documents(client=client)
    print(content)
    return "hello"

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)