import json

from fastapi.testclient import TestClient
from typing import *
from app.main import app


def test_root_api():
	client = TestClient(app)
	response = client.get('/api')

	assert response.status_code == 200

#test case where api request body takes two dates
def test_hts_index_api():
	client = TestClient(app)
	data = {
		"start_date": "2020-01-01",
		"end_date": "2020-01-07"
	}
	response = client.post('/hts_index', data=json.dumps(data))
	print(response.status_code)
	assert response.status_code == 200
	assert type(response.json()) == dict
	assert response.json()["message"] == "Success"
	assert response.json()["start_date"] == "2020-01-01"

#test case where api request body only takes one date
def test_tx_curr_numerator_api():
	client = TestClient(app)
	data = {
		"end_date": "2020-01-07"
	}
	response = client.post('/tx_curr_numerator', data=json.dumps(data))
	print(response.status_code)
	assert response.status_code == 200
	assert type(response.json()) == dict
	assert response.json()["message"] == "Success"
	assert response.json()["end_date"] == "2020-01-07"

#test where api endpoint doesnt take any data
def test_facility_details_api():
	client = TestClient(app)
	response = client.get('/facility_details_list')
	print(response.status_code)
	assert response.status_code == 200
	assert response.json()["message"] == "Success"



