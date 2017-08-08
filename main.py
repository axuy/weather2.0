from flask import Flask, render_template, request
# from google.cloud import bigquery
from google.appengine.ext import ndb
import time, uuid

app = Flask(__name__)

class Station(ndb.Model):
    station = ndb.IntegerProperty()
    location = ndb.StringProperty()

@app.route('/')
def form():
    return render_template('form.html')

@app.route('/submitted', methods=['POST'])
def submitted_form():
    station = request.form['station']
    weatherData = query_weather(station)
    return render_template('submitted_form.html', weatherData=weatherData)

@app.route('/data_store', methods=['POST'])
def data_store():
    name = request.form['name']
    location = request.form['location']
    
    return render_template('data_store.html')

def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

def query_weather(station):
    client = bigquery.Client.from_service_account_json(
        'weather-94b1ef1a67fd.json')
    query = """
        SELECT
        temp, mo, da
        FROM
        `bigquery-public-data.noaa_gsod.gsod2017`
        WHERE
        wban = @station
        ORDER BY mo desc, da desc
        """

    # return "aowefij"
    query_job = client.run_async_query(
        str(uuid.uuid4()),
        query,
        query_parameters=(
            [bigquery.ScalarQueryParameter('station', 'STRING', station)]))
    query_job.use_legacy_sql = False

    # Start the query and wait for the job to complete.
    query_job.begin()
    wait_for_job(query_job)
    return query_job.results().fetch_data(max_results=7)
