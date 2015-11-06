#!/usr/bin/python
from nose.tools import *
import unittest
import alphakit.main as mn

class AlphakitTestMethods(unittest.TestCase):
    
    def test_main_runme(self):
    	self.assertEqual(mn.runme(2), 4)

if __name__ == '__main__':
    unittest.main()