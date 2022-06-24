from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def hello():
    return render_template('results.html', sorting= , palidrome = , n_palindrome = , longest = , size = )