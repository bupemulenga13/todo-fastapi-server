from typing import Any
import unittest
from urllib import response
from dirty_equals import IsDict, IsStr
from sqlalchemy.orm import Session
from middleware.dqa_views.hts.hts import get_hts_index
from middleware.dqa_views.facility_details import facility_details
from middleware.utility import case__db, case__db_sd_ed


class Test_view_util_func(unittest.TestCase):
	def test_case_db_func(self):
		db = Session()
		result = case__db(db, facility_details.get_facility_details)
		print(type(result["items_list"]))
		print(result)
		self.assertNotIn(500, result)
		self.assertTrue(type(result), IsDict())
		self.assertEqual(200, result["status_code"])

	def test_case_db_sd_ed_func(self):
		db = Session()
		start_date = "2020-01-01"
		end_date = "2020-01-07"
		result = case__db_sd_ed(db,get_hts_index, start_date, end_date)
		self.assertNotIn(500, result)
		self.assertTrue(type(result), IsDict())
		self.assertEqual(200, result["status_code"])
		self.assertEqual(start_date, result["start_date"])




