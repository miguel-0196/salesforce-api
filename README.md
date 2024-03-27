This project provides the following APIs for salesforce integration:

1. get_oauth_url:		get oauth url for salesforce login
2. login_oauth_callback:	get refresh token from oauth response code
3. get_new_access_token:	get access token from refresh token
4. get_object_data:		get object's records from salesforce
5. create_custom_object:	create a custom object to salesforce
6. upload_object_data:		add object's records to salesforce
7. save_object_data_to_bigquery: get object's records from salesforce and then save to bigquery

This project can run as a Flask service or Docker service.
