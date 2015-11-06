#!/usr/bin/python

# project to create a way to download data from Quandl's API, manage downloaded files (download, storing, reading), 
#
# 4 Classes defined:
#    (1) ImportedData: generic class to define imported data objects from APIs
#    (2) QuandlData: child class to ImportedData specific to interaction with the Quandl API
#    (3) Scenario: class for downloading, munging, analyzing, and visualizing comparisons between datasets
#    (4) CorrAnalysis: class with tools to determine correlations between variables
#
# Future extensions:
#    (1) run the Scenario.get_data method as a multi-threaded process to reduce the data download bottleneck
#    (2) create benchmarks and parameter checking to see if data is clean (e.g., store historical 52 week high/low)

import pandas as pd
import numpy as np
import urllib
from matplotlib import pyplot as pl
from datetime import date
from dateutil.relativedelta import relativedelta
from utils.resources import get_path

def decode_filename(filename):
    """ decodes the standardized filename into a dictionary with the embedded data
        [source_name]-[database_code]-[dataset_code]-[start_date]-[end_date]
    """
    if source_name == 'Quandl':
        return dict(zip(['source_name', 'database_code', 'dataset_code', 'start_date', 'end_date'],  filename[:-4].split('-')}))
    else:
        return None

class ImportedData(object):
    """ general data import object for APIs
    """
    def __init__(self, source_name, index_name, data_record_ID, obs_start_date, obs_end_date):
        self.source_name = source_name
        self.test_vars = []
        self.independent_vars = []
        self.data_record_ID = data_record_ID
        self.API_key = load_API_key()
        self.obs_start_date = obs_start_date
        self.obs_end_date = obs_end_date
        self.data_frame = None
        self.index_name = index_name

    def get_index(self):
        return self.index_name

    def encode_filename(self):
        return

    def load_API_key():
        return

    def decode_filename(self):
        return

    def URL_string(self):
        return

    def close(self):
        self.source_name = None
        self.obs_start_date = None
        self.obs_end_date = None

    def __exit__(self, *err):
        self.close()


class QuandlData(ImportedData):
    """ object for storing Quandl data
    """
    def __init__(self):
        ImportedData.__init__(self, 'Quandl', 'Date', '[API KEY]', data_record_ID, obs_start_date, obs_end_date)

    def code_lookup(self):
	    """ lookup and return dataset identifiers required for building Quandl API calls for soybean and soybean oil futures data
            format of dictionary is: quandl_code_dict = { short_description: {database_code: '', dataset_code: ''} }
	        this data would have been defined in an external text file or database if the fully formatted data were available
	    """
	    quandl_code_dict = {'soyfutures': {'database_code': 'CHRIS', 'dataset_code': 'CME_S1'}, 
	                        'soyoilfutures': {'database_code': 'CHRIS', 'dataset_code': 'CME_B01'},
	                        'soycts': {'database_code': 'CFTC', 'dataset_code': 'S_F_ALL'},
	                        'soyoilcts': {'database_code': 'CFTC', 'dataset_code': 'BO_F_ALL'}
	                       }
	    try:
		    return quandl_code_dict[self.data_record_ID]
	    except:
		    return None

    def get_API_key(self):
        """ get the API key from a text file
        """
        with open('resources.txt', 'r') as f:
            self.API_key = f.read_CSV()

    def short_code(self):
	    """ return a formatted string for the quandl short_code: 'database_code/dataset_code'
	    """
	    return '/'.join(code_dict_entry[database_code], code_dict_entry[dataset_code])

    def encode_filename(self, save_location=''):
	    """ returns a formatted string for the filename for saving a dataframe to disk
	    """
	    code_dict_entry = code_lookup(self.data_record_ID)
        if len(save_location) > 0:
            prefix = save_location + '/'
	    return prefix + '-'.join(self.data_source, code_dict_entry['database_code'], code_dict_entry['dataset_code'], self.obs_start_date.strftime('%Y%m%d'), self.obs_end_date.strftime('%Y%m%d')) + '.csv'

    def URL_string(self):
        """ create the URL string for making the API call
        """
        base_str = 'https://www.quandl.com/api/v3/datasets/'
        quandl_api_param = []
        quandl_api_param.append(('api_key', self.api_key))
        quandl_api_param.append(('start_date', self.obs_start_date.strftime('%Y-%m-%d')))
        quandl_api_param.append(('end_date', self.obs_end_date.strftime('%Y-%m-%d')))
        return base_str + '/' + short_code(code_lookup(self.data_record_ID)) + '/' + 'data.csv' + '?' +  '&'.join([str(x[0]) + '=' + str(x[1]) for x in quandl_api_param])   


class Scenario(object):
    """ builds an analysis dataset and scenario

    """

    def __init__(self, data_record_list=[], obs_start_date=date.today()-relativedelta(years=1), obs_end_date=date.today()):
    	""" default observation window of trailing 1 year of data
    		
    	"""

    	self.data_record_list = data_record_list       # list of ImportedData objects
    	self.obs_start_date = obs_start_date           # start of observation window
    	self.obs_end_date = obs_end_date               # end of observation window
    	self.data_pulled = False
    	self.data_cleaned = False
    	self.data_saved = False
        self.data_check_errors = []
        self.combined_data_frame = None

    def close(self):
        """ clean up instance
        """
        self.data_record_list = None
        self.obs_start_date = None
        self.obs_end_date = None
        self.data_pulled = None
        self.data_cleaned = None
        self.data_saved = None
        self.data_check_errors = None
        self.combined_data_frame = None

    def __exit__(self, *error):
        self.close()

    def __repr__(self):
    	""" ipython friendly display of object
    	"""
    	print 'Scenario instance:\nTime interval from %s to %s\nFor datasets: %s' % (self.obs_start_date, self.obs_end_date, self.quandl_short_desc_list)

    def get_data(self, save_location='data'):
    	""" retrieves data from API, saves it to disk as csv
    	"""
    	for data_record in [x for x in self.data_record_list if x.data_frame != None]:		
    		url_reader = urllib.URLopener()
    		url_reader.retrieve(data_record.URL_string(), data_record.encode_filename(save_location))

        self.data_pulled = True

    def store_data(self, save_location='data'):
    	"""
    	    writes all DataFrames to CSV files
    	"""
    	for data_record in self.data_record_list:
            new_file_name = data_record.encode_filename(save_location)
            if data_record.data_frame != None:
    	        data_record.data_frame.to_CSV(new_file_name, sep=',')

        self.data_saved = True

    def read_data(self, data_location='data'):
    	""" open up all relevant files and store in a data frame
    	"""
    	for data_record in self.data_record_list:
    		try:
    			with open(data_record.encode_filename(data_location), 'r') as f:
    				data_record.data_frame = pd.read_csv(f, index_col=data_record.get_index(), parse_dates=True)    # put data from csv file into data frame and set index
    		except:
    			data_record.data_frame.append(None)


    def check_data(self):
    	""" return list of problems with data such as missing values
    	"""
        for data_record in self.data_record_list:
            # verify 

    	# check date max/min matches begin/end submitted
    	# check for missing data

    	# check for 

    def clean_data(self):
    	""" perform delete rows with null values
    	"""
        
        for data_record in data_record_list:
            # Remove rows with missing data
            data_record.data_frame = data_record.data_frame[np.isnotnull(data_record.data_frame[data_record.test_vars])]
        
        # merge and interpolate datasets to match reporting intervals of data sets (e.g., weekly and daily)
        if self.combined_data_frame == None and len(data_record) > 0:
            self.combined_data_frame = reduce(lambda x,y: pd.ordered_merge(x, y), [z.data_frame for z in data_record])
            self.combined_data_frame.interpolate()
    	
        # future code to handle outliers
        
        self.data_cleaned = True

    def show_result(self, cross_compare_list=[]):
    	""" visualizations using matplotlib
            included in these are: (1) Cumulative Daily Return  (2) Total Reportable Long / Short Ratio  (3) Pearson correlation
            cross_compare_list
    	"""

        for data_record in data_record_list:
            # create data columns for daily return and cumulative daily return
            data_record.data_frame['daily_rtn'] = pd.Series((data_record.data_frame.Settle - data_record.data_frame.Settle.shift(-1)) / data_record.data_frame.Settle.shift(-1), index=data_record.data_frame.index)
            data_record.data_frame['cml_daily_rtn'] = pd.Series(data_record.data_frame['daily_rtn'] + 1, index=data_record.data_frame.index).cumprod()
            # plot cumulative return against index 'Date'
            data_record.data_frame[['cml_daily_rtn']].plot()

            # create data columns for total reportable longs and shorts ratio
            data_record.data_frame['long_short_ratio'] = pd.Series.div(data_record.data_frame['Total Reportable Longs'], data_record.data_frame['Total Reportable Shorts'])
            data_record.data_frame[['long_short_ratio']].plot()

        # Run Pearson correlation calculation
        self.combined_data_frame.corr()

        # plot
        self.combined_data_frame.plot()


class CorrAnalysis(object):
    """ Toolkit for linear regression and machine learning to identify correlation between variables
    """

if __name__ == '__main__':
	pass