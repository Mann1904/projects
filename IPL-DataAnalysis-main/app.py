# Importing essential libraries
from flask import Flask, render_template, request
import sys
sys.dont_write_bytecode=True
from datacollection import data_collection
import pandas as pd
import requests

app = Flask(__name__)

@app.route('/')
def home():
	return render_template("index.html")

@app.route('/home', methods=['GET','POST'])
def winner_predictor():
    return render_template('index.html')

@app.route('/data_analysis', methods=['GET','POST'])
def data_analysis():
	return render_template('data-analysis.html')
    
@app.route('/ShowRecord', methods=['GET','POST'])
def ShowRecord():
    
    category = request.form['category']
    year = request.form['year']
        
    header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
            }
    r = requests.get("https://www.iplt20.com/stats/{0}/{1}".format(year,category), headers=header)
    df = pd.read_html(r.text)[0]
    print(df)
    return render_template("record.html", name=category, data=df.to_html(classes="table table-striped table-hover"))
        

@app.route('/playersrecord', methods=['GET','POST'])
def playersrecord():
     return render_template('players-record.html')

@app.route('/datacollection',methods=['GET','POST'])
def datacollection():
	data_collection()
	return render_template('index.html')
	
if __name__ == '__main__':
	app.run(debug=True)
	#app.run(port=8000,debug=True)