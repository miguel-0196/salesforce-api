#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import requests
from simple_salesforce import Salesforce

# External libraries
from dotenv import load_dotenv
from flask import Flask, request, redirect

# Load environment variables
load_dotenv()
SERVICE_IP = os.getenv('SERVICE_IP') or '0.0.0.0'
SERVICE_PORT = os.getenv('SERVICE_PORT') or 4445
SALES_KEY = os.getenv('SALES_CLIENT_KEY')
SALES_SECRET = os.getenv('SALES_CLIENT_SECRET')

# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)


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

        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        custom_object = True if object_name.endswith('__c') else False
        if custom_object == True:
            meta = getattr(sf, object_name).describe()
            fields = [field['name'] for field in meta['fields']]
            query = 'SELECT ' + ', '.join(fields)
        else:
            query = 'SELECT FIELDS(STANDARD)'

        query += f' FROM {object_name} WHERE IsDeleted=False'
        if from_date != '':
            query += f' AND LastModifiedDate>={from_date}T00:00:00Z'

        if to_date != '':
            query += f' AND LastModifiedDate<={to_date}T23:59:59Z'

        return sf.query_all(query)
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


# Test route
@app.route("/redirect")
def redirect():
    return redirect(request.args.get("oauth_url"))

@app.route("/login/callback")
def callback():
    return request.args.get("code")


if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")