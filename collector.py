import time
import pandas as pd
import datetime
from sqlalchemy import create_engine, VARCHAR
import json
import requests
import pymysql


def data_collector(eng, date = '2022-04-01', date2 = None):

	"""
	*THIS IS SPECIFIC FUNCTION ONLY FOR LISTING DB*

	Collecting information of sold houses from listing database (from mainView),
	that were sold between specified dates.

	---------------------------------
	params:
	
	eng  : SQL engine to connect to db	
	date : starting date of transactions
	date2: end date of transactions. Default is None, that means taking all houses from start date
	________________________________
	Returns datafram of sold houses with charachteristics
	
	
	"""

	if date2 == None:
		criteria = f">= '{date}'"
	else:
		criteria = f"BETWEEN '{date2}' and '{date}'"
 
	full_data = pd.read_sql_query(f"""SELECT * FROM mainView							   
								 WHERE Sold_date {criteria}														
									;""", eng)

	return full_data




def n_addresses_listing(batch, length, eng):
	"""
	*THIS IS SPECIFIC FUNCTION ONLY FOR LISTING DB*

	Function to get specified number of addresses from listing db
	----------------------------------------------
	params:
	
	batch  :  controls the number of batch to return
	length :  controls the length of rows to pass
	eng    :  engine to connect to db
	_______________________________________________
	Returns dataframe of n addresses
	"""
	try:
		nth_addresses = pd.read_sql(f"""SELECT * FROM address
										WHERE address.Listing_id_ad IN (SELECT listing.idListing FROM listing
																		 WHERE listing.Property_UPRN IS NULL)
                                        AND address.Found_address IS NOT NULL
										LIMIT {batch} OFFSET {length} 
									;""", eng)
	except pymysql.err.ProgrammingError as e:# Does not work
		print("Please check, if listing and address tables are exist or you have provided correct engine")
		raise ValueError
	else:
		return nth_addresses[['Listing_id_ad', 'Found_address']]


def n_addresses_sold(batch, eng):
    """
    *THIS IS SPECIFIC FUNCTION ONLY FOR LISTING DB*

    Function to get specified number of addresses from sold db
    ----------------------------------------------
    params:

    batch  :  controls the number of batch to return    
    eng    :  engine to connect to db
    _______________________________________________
    Returns dataframe of n addresses
    """
    try:
        nth_addresses = pd.read_sql(f"""SELECT Sold_id, Address FROM sold_property
                                        WHERE UPRN IS NULL                                        
                                        LIMIT {batch}  
                                    ;""", eng)
    except pymysql.err.ProgrammingError as e:# Does not work
        print("Please check, if listing and address tables are exist or you have provided correct engine")
        raise ValueError
    else:
        return nth_addresses[['Sold_id', 'Address']]




def UPRN_finder(address, url_port, headers, temp_table, eng):

	"""
	*THIS IS SPECIFIC FUNCTION THAT USES ADDRESS_MATCHING DOCKER*

	Function takes dataframe of addresses and matches UPRN to each of them 
	using address_matching docker and writes in temprorary table
	---------------------------------------------
	params:
	
	address    : dataframe with addresses
	url_port   : URL address of the docker with port
	temp_table : Table to save matched addresses
	eng        : Engine to connect to db				
	"""

	input_dict = address.to_dict()        
	json_dict = json.dumps(input_dict)        
	response = requests.post(url_port, data=json_dict, headers=headers)        
	output = pd.DataFrame(json.loads(response.content)["message"]) 
	output.to_sql(name=temp_table, con=eng, if_exists='append', index=False, chunksize=1000)





def transactions(eng, trans_id, date1 = '2021-01-01', date2 = '2021-12-31'):
	"""
	*SPECIFIC FUNCTION FOR TRANSACTIONS TABLE FROM LISTING*
	
	Function returns all transactions for the houses with specified id
	----------------------------------
	params:
	
	eng      : Engine to connect to db
	trans_id : Tuple of id for which transactions are needed
	date1    : Transaction start date
	date2    : Transaction end date
	___________________________________
	Return Dataframe of transactions per id
	"""

	trans = pd.read_sql_query(f"""SELECT * FROM transaction
									WHERE Sold_id_transaction in {str(trans_id)} and Sold_date BETWEEN '{date1}' and '{date2}'
								""", eng)	
	trans['transactions'] = trans.apply(lambda x: (x['Price'], x['Sold_date']), axis = 1)
	df= trans.groupby('Sold_id_transaction')['transactions'].apply(list).to_frame().reset_index()	
	return df
				   



def mobile_internet(df, url, headers, port = 8050):
	"""
	Matching mobile data by postcodes using mobile/internet docker
	
	params
	-----------------------------
	port     : docker port
	url      : docker url
	headers  : docker header
	______________________________
	Returns merged df
	"""
	url = url.format(port)
	json_dict = json.dumps(df[['matcher', 'Full_postcode']].to_dict(), default=str)
	response = requests.post(url, data=json_dict, headers=headers)
	output = json.loads(response.content)["message"]
	return pd.merge(df, pd.DataFrame(output).drop('Full_postcode', axis = 1), on = 'matcher')




def school_transport_metro(df, url, headers, port = 8070):
	"""
	Matching school data by coordinates using school/transort docker
	
	params
	-----------------------------
	port   : docker port
	url    : docker url
	headers: docker header
	______________________________
	Returns merged df
	"""	
	url = url.format(port)
	coords = df[['matcher', 'LATITUDE', 'LONGITUDE']].to_dict()
	input_dict = {'coordinates': coords}
	json_dict = json.dumps(input_dict, default=str)
	response = requests.post(url, data=json_dict, headers=headers)
	output = json.loads(response.content)["message"]
	return pd.merge(df,  pd.DataFrame(output).drop(['LATITUDE', 'LONGITUDE'], axis = 1), on = 'matcher')