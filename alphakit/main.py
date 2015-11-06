#!/usr/bin/python
# CHRIS = futures prices
# CME_B01 = soybean oil futures
# CME_S1 = soybean futures
# CFTC = CTR report
# S_F_ALL = soybean CTR
# BO_F_ALL = soybean oil CTR
# pandas .read_CSV DataFrame.to_CSV


import pandas as pd
import numpy as np
import urllib
from matplotlib import pyplot as pl
from datetime import date
from dateutil.relativedelta import relativedelta
from utils.resources import get_path

def quandl_code_lookup(description):
	""" quandl_code_dict = { short_description: {database_code: '', dataset_code: ''} }
	    this data could be defined in an external text file if there were a comprehensive dictionary of all possible quandl codes
	"""
	quandl_code_dict = {'soyfutures': {'database_code': 'CHRIS', 'dataset_code': 'CME_S1'}, 
	                    'soyoilfutures': {'database_code': 'CHRIS', 'dataset_code': 'CME_B01'},
	                    'soycts': {'database_code': 'CFTC', 'dataset_code': 'S_F_ALL'},
	                    'soyoilcts': {'database_code': 'CFTC', 'dataset_code': 'BO_F_ALL'}
	                   }
	try:
		return quandl_code_dict[description]
	except:
		return None

def quandl_short_code(code_dict_entry):
	""" return a formatted string for the quandl short_code: 'database_code\dataset_code'
	"""
	return '/'.join(code_dict_entry[database_code], code_dict_entry[dataset_code])

def quandl_filename_encoder(quandl_short_desc, start_date, end_date):
	""" returns a formatted string for the filename for saving a dataframe to disk
	"""
	code_dict_entry = quandl_code_lookup(quandl_short_desc)
	return '-'.join(code_dict_entry['database_code'], code_dict_entry['dataset_code'], start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')) + '.csv'

def quandl_filename_decoder(filename):
	""" decodes the standardized filename into a dictionary with the embedded data
	"""
	return dict(zip(['database_code', 'dataset_code', 'start_date', 'end_date'],  filename[:-4].split('-')}))

class Scenario(object):
    """ builds analysis dataset and scenario

    Attributes:
        quandl_short_desc_list
        obs_start_date
        obs_end_date
    """

    def __init__(self, quandl_short_desc_list=[], obs_start_date=date.today()-relativedelta(years=1), obs_end_date=date.today()):
    	""" default observation window of last year from today
    		
    	"""
    	self.quandl_short_desc_list = quandl_short_desc_list
    	self.obs_start_date = obs_start_date
    	self.obs_end_date = obs_end_date
    	self.df_list = []
    	self.data_pulled = False
    	self.data_cleaned = False
    	self.data_saved = False

    def __repr__(self):
    	""" ipython friendly display of object
    	"""
    	print 'Scenario instance:\nTime interval from %s to %s\nFor datasets: %s' % (self.obs_start_date, self.obs_end_date, self.quandl_short_desc_list)

    def get_data(self, api_key='', save_location='/data'):
    	""" retrieves data from quandl, saves it to disk as csv
    	"""
    	base_str = 'https://www.quandl.com/api/v3/datasets/'
    	for quandl_desc in self.quandl_short_desc_list:
    		quandl_api_param = []
    		quandl_api_param.append(('api_key', api_key))
    		quandl_api_param.append(('start_date', self.obs_start_date.strftime('%Y-%m-%d')))
    		quandl_api_param.append(('end_date', self.obs_end_date.strftime('%Y-%m-%d')))
    		exec_str = base_str + '/' + quandl_short_code(quandl_code_lookup(quandl_desc)) + '/' + 'data.csv' + '?' +  '&'.join([str(x[0]) + '=' + str(x[1]) for x in quandl_api_param])   
    		
    		# download the required dataset using and save to disk in CSV format
    		url_reader = urllib.URLopener()
    		url_reader.retrieve(exec_str, '/'.join(save_location, quandl_filename_encoder(quandl_desc, self.obs_start_date, self.obs_end_date)))


    def store_data(self, save_location='/data'):
    	"""
    	    writes all DataFrames to CSV files
    	"""
    	for short_desc in 
    	new_file_name = quandl_filename_encoder(short_desc, self.obs_start_date, self.obs_end_date)
    	df.to_CSV(new_file_name, sep=',')

    def read_data(self, data_location='/data'):
    	""" open up all relevant files and store in df_list
    	"""
    	for quandl_desc in self.quandl_short_desc_list:
    		try:
    			with open(quandl_filename_encoder(quandl_desc, self.obs_start_date, self.obs_end_date), 'r') as f:
    				df = pd.read_csv(f, index_col='Date', parse_dates=True)
    				self.df_list.append(df)
    		except:
    			self.df_list.append(None)


    def check_data(self):
    	""" return list of problems with data such as missing values, 
    	"""
    	# verify counts and databox
    	# check date max/min matches begin/end submitted
    	# check for missing data

    	# check for 

    def clean_data(self):
    	"""
    	"""
    	# delete rows where there are missing data elements
    	# handle outliers


    def show_result(self):
    	""" visualizations using matplotlib
    	"""
# 1 & 3: soybean/soybean oil cumulative daily returns for futures
        df['cml_daily_rtn'] = pd.Series((df.Settle - df.Settle.shift(-1)) / df.Settle.shift(-1), index=df.index).cumprod()
        df[['cml_daily_rtn']].plot()

# 2 & 4: soybean/soybean oil total reportable longs and shorts ratio
        df['long_short_ratio'] = pd.Series.div(df['Total Reportable Longs'], df['Total Reportable Shorts'])
        df[['long_short_ratio']].plot()

# 5 & 6: soybean/soybeal oil Pearson weekly correlation between total reportable longs/shorts ratio and commodity futures return
        # clean datasets
        # fill in blanks and match intervals of data (cts is weekly while CHRIS data is daily)

        # append CTS and CHRIS dataframes for each commodity by Date

        # Run Pearson correlation calculation

        # plot

# analysis
    def 

# Date Last df[['Date', 'Last']]

# visualization %matplotlib inline



def runme(x):
    return x*x

if __name__ == '__main__':
	pass