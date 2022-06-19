from flask import Flask

app = Flask(__name__)


@app.route('C:\Users\eleon\Desktop\Leiden University\Semester 2\Cloud Computing\Assignment 2\assignment2_cc')
def hello():
    return 'Hello, World!'