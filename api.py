#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import requests

# External libraries
from dotenv import load_dotenv
from flask import Flask, request, redirect
from simple_salesforce import Salesforce
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Load environment variables
load_dotenv()
SERVICE_IP = os.getenv('SERVICE_IP') or '0.0.0.0'
SERVICE_PORT = os.getenv('SERVICE_PORT') or 4445
SALES_KEY = os.getenv('SALES_CLIENT_KEY')
SALES_SECRET = os.getenv('SALES_CLIENT_SECRET')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') or 'salesforce'
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Util func
def get_salesforce_object_data(instance_url, access_token, object_name, from_date, to_date):
    sf = Salesforce(instance_url=instance_url, session_id=access_token)
    meta = getattr(sf, object_name).describe()
    fields = [field['name'] for field in meta['fields']]
    query = 'SELECT ' + ', '.join(fields)

    query += f' FROM {object_name} WHERE IsDeleted=False'
    if from_date != '':
        query += f' AND LastModifiedDate>={from_date}T00:00:00Z'

    if to_date != '':
        query += f' AND LastModifiedDate<={to_date}T23:59:59Z'
    
    return sf.query_all(query), meta

# API routes
@app.post("/get_oauth_url")
def get_oauth_url():
    try:
        redirect_uri =  request.form['redirect_uri']
        url = "https://login.salesforce.com/services/oauth2/authorize?response_type=code&scope=refresh_token&client_id="+SALES_KEY+"&redirect_uri="+redirect_uri
        return url
    except Exception as err:
        return str(err), 405

@app.post("/login_oauth_callback")
def login_oauth_callback():
    try:
        redirect_uri =  request.form['redirect_uri']
        authorization_code =  request.form['authorization_code']
        uri_token_request = 'https://login.salesforce.com/services/oauth2/token'
        return requests.post(uri_token_request, data={
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'client_id': SALES_KEY,
            'client_secret': SALES_SECRET,
            'redirect_uri': redirect_uri
        }).json()
    except Exception as err:
        return str(err), 405

@app.post("/get_new_access_token")
def get_new_access_token():
    try:
        refresh_token =  request.form['refresh_token']
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id' : SALES_KEY,
            'client_secret' : SALES_SECRET
        }

        uri_token_request = 'https://login.salesforce.com/services/oauth2/token'
        return requests.post(uri_token_request, data=data).json()
    except Exception as err:
        return str(err), 405


@app.post("/get_object_data")
def get_object_data():
    try:
        instance_url = request.form['instance_url']
        access_token = request.form['access_token']
        object_name = request.form['object_name']
        from_date = request.form['from_date'] if 'from_date' in request.form else '' # 2024-02-06
        to_date = request.form['to_date'] if 'to_date' in request.form else ''
        data, meta = get_salesforce_object_data(instance_url, access_token, object_name, from_date, to_date)
        return data

    except Exception as err:
        return str(err), 405


@app.post("/create_custom_object")
def create_custom_object():
    try:
        access_token = request.form['access_token']
        instance_url = request.form['instance_url']
        full_name = request.form['full_name']
        label = request.form['label']
        pluralLabel = request.form['pluralLabel']
        fields = json.loads(request.form['fields'])

        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        custom_object = sf.mdapi.CustomObject(
            fullName = full_name,
            label = label,
            pluralLabel = pluralLabel,
            nameField = sf.mdapi.CustomField(
                label = "Name",
                type = sf.mdapi.FieldType("Text")
            ),
            fields = fields,
            deploymentStatus = sf.mdapi.DeploymentStatus("Deployed"),
            sharingModel = sf.mdapi.SharingModel("Read")
        )
        sf.mdapi.CustomObject.create(custom_object)
    
        return 'Created successfully'
    except Exception as err:
        return str(err), 405


@app.post("/upload_object_data")
def upload_object_data():
    try:
        instance_url = request.form['instance_url']
        access_token = request.form['access_token']
        object_name = request.form['object_name']
        data = json.loads(request.form['data'])

        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        obj = getattr(sf.bulk, object_name)
        return obj.insert(data, batch_size=10000)
    except Exception as err:
        return str(err), 405


@app.post("/save_object_data_to_bigquery")
def save_object_data_to_bigquery():
    try:
        instance_url = request.form['instance_url']
        access_token = request.form['access_token']
        object_name = request.form['object_name']
        from_date = request.form['from_date'] if 'from_date' in request.form else '' # 2024-02-06
        to_date = request.form['to_date'] if 'to_date' in request.form else ''

        # Prepare data
        data, meta = get_salesforce_object_data(instance_url, access_token, object_name, from_date, to_date)
        records = []
        for record in data['records']:
            r = {}
            for key, value in record.items():
                if key == 'attributes':
                    continue
                if isinstance(value, dict):
                    r[key] = str(value)
                else:
                    r[key] = value
            records.append(r)

        # Connect bigquery
        client = bigquery.Client.from_service_account_json("./googleapis.json")
        dataset = client.dataset(DATASET_ID, project = GOOGLE_CLOUD_PROJECT)
        table_ = dataset.table(object_name)
        schema = []
        gsql_types = 'ARRAY, BIGNUMERIC, BOOL, BYTES, DATE, DATETIME, FLOAT64, GEOGRAPHY, INT64, INTERVAL, JSON, NUMERIC, RANGE, STRING, STRUCT, TIME, TIMESTAMP,'
        for field in meta['fields']:
            type = field['type'].upper()
            if gsql_types.find(type + ',') == -1 or type == 'DATETIME':
                type = 'STRING'
            schema.append(bigquery.SchemaField(field['name'], type))

        # Get a table
        try:
            table = client.get_table(table_)
        except NotFound:
            table = bigquery.Table(table_, schema=schema)
            table.clustering_field=['OwnerId']
            table = client.create_table(table)

        # Save data
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema = schema
        return str(client.load_table_from_json(records, table, job_config=job_config).result().job_id)
    except Exception as err:
        return str(err), 405


# Test route
@app.route("/redirect")
def redirect():
    return redirect(request.args.get("oauth_url"))

@app.route("/login/callback")
def callback():
    return request.args.get("code")


if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")