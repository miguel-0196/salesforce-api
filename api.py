#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import requests
from simple_salesforce import Salesforce

# External libraries
from dotenv import load_dotenv
from flask import Flask, request

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
        url = "https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id="+SALES_KEY+"&redirect_uri="+redirect_uri
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

        if 'error' in response:
            return {'statusCode': 406, 'body': str(response['error'])}

        return {'statusCode': 200, 'access_token': response['access_token'], 'instance_url': response['instance_url']}
    except Exception as err:
        return {'statusCode': 405, 'body': str(err)}

if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")