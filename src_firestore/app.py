from queue import Empty
from flask import Flask,render_template,request, jsonify, Response
import numpy as np
import datastore, storage, publisher
import os

SORT_CHUNK = 1000
PALINDROME_CHUNK = 1000

app = Flask(__name__)
 
@app.route('/')
def form():
    return render_template('results.html')

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():

    if request.method == 'POST':
        if request.form["form"] == "file_form_input":
            if request.files['file'].filename == '':
                return "No file uploaded"
            
            filename = request.files['file'].filename
            content = request.files['file'].read()
            content = content.decode("utf-8")

            chunk_size = SORT_CHUNK
            if request.form["chunk"]:
                tmp = request.form["chunk"]
                if tmp.isnumeric():
                    chunk_size = int(tmp)
            offsets = create_offsets(content, chunk_size)
            result = handle_storage(filename=filename, content=content, chunk_sort=SORT_CHUNK, chunk_palindrome=PALINDROME_CHUNK, num_offsets=len(offsets))
            job_id = result[0]
            destination_name = result[1]
            sorting_messages = publisher.sendMessages(mode="sort", job_id=job_id, obj_name=destination_name, offsets=offsets)
            palindrome_messages = publisher.sendMessages(mode="palindrome", job_id=job_id, obj_name=destination_name, offsets=offsets)
            if result == False:
                return "Failed to upload object"
            
            return render_template('results.html', job_id=job_id)
        
        if request.form["form"] == "job_form_input":
            # handle job
            if request.form["searchid"] == '':
                return render_template('results.html')

            job_id = request.form["searchid"] # str

            # get job from datastore
            job = datastore.getJob(job_id)

            if not job:
                return render_template('results.html')

            # get workers for the job
            workers = datastore.getWorkers(job_id)

            reducers = datastore.getReducers(job_id)

            job_stats = job_statistics(job, workers, reducers)

            elapsed_time = np.round(job_stats["elapsed_time"], 3)
            sorting_perc = job_stats["sort_perc"]
            palindrome_perc = job_stats["palindrome_perc"]
            if job_stats["done"]:
                longest = job_stats["longest"]
                word = job_stats["longest_pal_word"]
                count = job_stats["palindrome_count"]
                return render_template('results.html', sorting=sorting_perc, palindrome=palindrome_perc, n_palindrome=count, longest=word, size=longest, done=True, searchid=job_id, elapsed=elapsed_time)

            return render_template('results.html', sorting=sorting_perc, palindrome=palindrome_perc)

        if request.form["form"] == "download":
            job_id = request.form["searchid"] # str

            if not job_id.isnumeric:
                return render_template('results.html')

            filename = "sorted.txt"
            contents = storage.getFile(filename=filename, job_id=job_id)

            return Response(contents,
                       mimetype="text/plain",
                       headers={"Content-Disposition":
                                    "attachment;filename=sorted.txt"})
                        

    return render_template('results.html')


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
        if start > l:
            break
        if end > l:
            end = l
            offsets.append((start, end))
            break
        offsets.append((start, end))

    return offsets

def job_statistics(job, workers, reducers):
    job_done = False
    if job["palindrome_done"] and job["sorting_done"]:
        job_done = True

    longest = 0
    word = ''
    pal_count = 0
    if job_done:
        longest = job["palindromes"]["longest"]
        word = job["palindromes"]["word"]
        pal_count = job["palindromes"]["count"]
    
    palindrome_workers = workers[0]
    sort_workers = workers[1]

    palindrome_perc = 0
    # the last 10% is for the palindrome reducer
    per_worker = 90 / workers[2]
    for worker in palindrome_workers:
        tmp = worker.to_dict()
        if tmp["done"]:
            palindrome_perc += per_worker
    sorting_perc = 0
    # the other 50% is for the sort reducer
    per_worker = 50 / workers[3]
    for worker in sort_workers:
        tmp = worker.to_dict()
        if tmp["done"]:
            sorting_perc += per_worker
    
    palindrome_red = reducers[0]
    palindrome_red_perc = 0
    sort_red = reducers[1]
    sort_red_perc = 0
    palindrome_end_time = -1
    if palindrome_red:
        count = 0
        for reducer in palindrome_red:
            tmp = reducer.to_dict()
            if tmp["perc"] == 100:
                count += tmp["perc"]
                palindrome_end_time = tmp["end_time"]
        palindrome_red_perc += count * 0.1

    sort_red_time = -1
    if sort_red:
        count = 0
        for reducer in sort_red:
            tmp = reducer.to_dict()
            if tmp["perc"] == 100:
                count += tmp["perc"]
                sort_red_time = tmp["end_time"]
        sort_red_perc += count * 0.5

    sorting_perc += sort_red_perc
    palindrome_perc += palindrome_red_perc

    longest_time = sort_red_time
    if palindrome_end_time > longest_time:
        longest_time = palindrome_end_time
    
    job_stats = {}
    job_stats['done'] = job_done
    job_stats["palindrome_perc"] = np.round(palindrome_perc, 2)
    job_stats["sort_perc"] = np.round(sorting_perc, 2)
    job_stats["longest"] = longest
    job_stats["palindrome_count"] = pal_count
    job_stats["longest_pal_word"] = word
    job_stats["elapsed_time"] = longest_time - job["start_time"]
    return job_stats

if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))