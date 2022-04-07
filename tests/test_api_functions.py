from itertools import islice
import json
from operator import contains
import unittest
from dirty_equals import Contains, IsDict, IsJson, IsList, IsListOrTuple, IsPartialDict, IsPositiveInt, IsStr, IsStrictDict
from fastapi.testclient import TestClient
from typing import *
from app.main import app
from unittest import TestCase


# def test_root_api():
# 	client = TestClient(app)
# 	response = client.get('/api')

# 	assert response.status_code == 200
class Test_api_functions (unittest.TestCase):
	def setUp(self):
		self.expected_json={
		"start_date": IsStr,
		"end_date": IsStr,
		"status_code": IsPositiveInt,
		"message": IsStr,
		"selected_date": None,
		"query_list": IsListOrTuple()

		}

#test case where api request body takes two dates
	def test_hts_index_api(self):
		client = TestClient(app)
		data = {
			"start_date": "2020-01-01",
			"end_date": "2020-01-07"
		}

		response = client.post('/hts_index', data=json.dumps(data))
		print(response.status_code)
		print (type(response.json()))
		self.assertEqual (response.status_code, 200)
		self.assertTrue (response)
		self.assertTrue(type(response.json()), IsDict)
		# assert response.json()["message"] == "Success"
		# assert response.json()["start_date"] == "2020-01-01"
		# assert response.json()["query_list"] == IsListOrTuple(check_order=False)
	# assert response.json()["query_list"] == IsList
	# assert response.json() == {
	# 	"start_date": IsStr,
	# 	"end_date": IsStr,
	# 	"status_code": IsPositiveInt,
	# 	"message": IsStr,
	# 	"selected_date": None,
	# 	"query_list": IsListOrTuple()
	# 	}
#test case where api request body only takes one date
	def test_tx_curr_numerator_api(self):
		client = TestClient(app)
		data = {
			"end_date": "2020-01-07"
		}
		response = client.post('/tx_curr_numerator', data=json.dumps(data))
		print(response.status_code)
		self.assertTrue(response.status_code, 200)
		self.assertTrue (type(response.json()), dict)
		self.assertIn (response.json()["message"],  "Success")
		self.assertIsNotNone( response.json()["query_list"])

# #test where api endpoint doesnt take any data
# def test_facility_details_api():
# 	client = TestClient(app)
# 	response = client.get('/facility_details_list')
# 	print(response.status_code)
# 	assert response.status_code == 200
# 	assert response.json()["message"] == "Success"



