#!/usr/bin/python
from nose.tools import *
import unittest
import alphakit.main as mn

class ScenarioTestCase(unittest.TestCase):
	""" test the Scenario class
	"""

	def setUp(self):
		self.scenario = mn.Scenario(['soyfutures'])

	def tearDown(self):
		self.scenario.close()
		self.scenario = None

class ScenarioTestMethods(ScenarioTestCase):
	""" test the Scenario methods
	"""

	def test_get_data(self):
		self.assertEqual(self.scenario.get_data(), )
		pass

	def test_store_data(self):
		pass

	def test_read_data(self):
		pass

	def test_check_data(self):
		pass

	def test_clean_data(self):
		pass

if __name__ == '__main__':
    unittest.main()