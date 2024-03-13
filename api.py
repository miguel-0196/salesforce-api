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
        return {'statusCode': 200, 'body': url}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


@app.post("/login_oauth_callback")
def login_oauth_callback():
    try:
        redirect_uri =  request.form['redirect_uri']
        callback_code =  request.form['callback_code']
        uri_token_request = 'https://login.salesforce.com/services/oauth2/token'
        response = requests.post(uri_token_request, data={
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri,
            'code': callback_code,
            'client_id': SALES_KEY,
            'client_secret': SALES_SECRET
        }).json()

        if 'error_description' in response:
            return {'statusCode': 406, 'body': str(response['error_description'])}

        return {'statusCode': 200, 'access_token': response['access_token'], 'instance_url': response['instance_url']}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


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
        response = requests.post(uri_token_request, data=data).json()
        return {'statusCode': 200, 'body': response}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


@app.post("/get_object_data")
def get_object_data():
    try:
        access_token = request.form['access_token']
        instance_url = request.form['instance_url']
        object_name = request.form['object_name']
        custom_object =  request.form['custom_object']
        from_date = request.form['from_date'] or ''
        to_date = request.form['to_date'] or ''

        if custom_object == True:
            query = 'SELECT+FIELDS(CUSTOM)+FROM+'+ object_name +'+WHERE+IsDeleted=False'
        else:
            query = 'SELECT+FIELDS(STANDARD)+FROM+'+ object_name +'+WHERE+IsDeleted=False'

        if from_date != '' or to_date != '':
            if from_date != '':
                query += '+AND+LastModifiedDate>=' + from_date + 'T00:00:00Z'

                if to_date != '':
                    query += '+AND+LastModifiedDate<=' + to_date + 'T23:59:59Z'
            else:
                query += '+AND+LastModifiedDate<=' + to_date + 'T23:59:59Z'

        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Content-Encoding': 'gzip'
        }

        response = requests.get(instance_url + '/services/data/v59.0/query/?q=' + query, headers=headers)
        return {'statusCode': 200, 'body': response.json()}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


@app.post("/get_object_data_extra")
def get_object_data_extra():
    try:
        access_token = request.form['access_token']
        instance_url = request.form['instance_url']
        next_records_url = request.form['next_records_url']

        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Content-Encoding': 'gzip'
        }

        response = requests.get(instance_url + next_records_url, headers=headers)
        return {'statusCode': 200, 'body': response.json()}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


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
    
        return {'statusCode': 200, 'body': 'Success'}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}


# Test route
@app.route("/redirect")
def redirect():
    return redirect(request.args.get("oauth_url"))


@app.route("/login/callback")
def callback():
    return request.args.get("code")


if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")