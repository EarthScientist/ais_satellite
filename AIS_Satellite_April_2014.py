# a new way forward with working with the AIS Satellite Data from ExactEarth
# bring in some of the libs we will need:

import fiona
import numpy as np
import os, sys, re, time, glob, shapefile, shutil, csv, StringIO
import pyproj
from pyproj import Proj
import pandas as pd
from collections import OrderedDict
from osgeo import gdal, ogr, osr

# set up some output directory structure based on the output base_path
def setup_dirs(output_base_path):
	"""
	set up the output directory structure for the creation of the density rasters

	creates an output directory structure that is populated by the subsequent 
	analyses and data manipulation outputs.  

	directory structure created:
		
		-parent_dir
			-csv
				-cleaned
				-joined
				-grouped
			-lines
				-individual_shapefiles
				-merged_shapefiles
			-density
				-GeoTiffs
				-PNGs

	* it is important to note that not all folders will populate with this script, but 
	will be populated by subsequent visualizations of the map outputs.

	arguments: 
	output_base_path = string path to the output folder directory location

	"""
	
	t = np.array([re.sub('\W','',i) for i in time.asctime().split()])
	parent_dir = '_'.join(t[[0,1,2,4,3]].tolist())
	base_path = os.path.join(output_base_path, parent_dir)

	os.mkdir(base_path)
	os.mkdir(os.path.join(base_path, 'csv'))
	os.mkdir(os.path.join(base_path, 'csv', 'cleaned'))
	os.mkdir(os.path.join(base_path, 'csv', 'joined'))
	os.mkdir(os.path.join(base_path, 'csv', 'grouped'))
	os.mkdir(os.path.join(base_path, 'csv', 'dropped'))
	os.mkdir(os.path.join(base_path, 'lines'))
	os.mkdir(os.path.join(base_path, 'lines', 'individual_shapefiles'))
	os.mkdir(os.path.join(base_path, 'lines', 'merged_shapefiles'))
	os.mkdir(os.path.join(base_path, 'points'))
	os.mkdir(os.path.join(base_path, 'density'))
	os.mkdir(os.path.join(base_path, 'density', 'GeoTiffs'))
	os.mkdir(os.path.join(base_path, 'density', 'PNGs'))

	return base_path


def remove_garbage_rows(in_csv_path, out_csv_folder):
	"""
	arguments: 
	in_csv_path = path to a csv file (not an open file object)
	out_csv_folder = path to the base output folder to put the outputs

	takes as input an exactearth data dump csv file and will remove all of the 
	'dirty' data.  It removes the data based on a few simple criteria that make sense
	for the purposes of the data needs of the ABSI Risk Assessment.

	In order for a row to be deemed clean:
	1. length of row fields must match the number of header column names
	2. MMSI must be able to be coerced to an integer value
	3. Latitude and Longitude must be able to be coerced to float values
	4. Time must be able to be split on the '_', rejoined with a '.' 
		and able to be coerced to a float.

	if a row passes all of the above criteria tests it is written into a 'clean' file, 
	and if it does not it is written to a 'dirty' file.


	"""
	clean_file = open( os.path.join( out_csv_folder, os.path.basename( in_csv_path ).replace('.csv', '_clean.csv') ), 'w' )
	trash_file = open( os.path.join( out_csv_folder, os.path.basename( in_csv_path ).replace('.csv', '_dirty.csv') ), 'w' )

	with open(in_csv_path, 'r') as in_csv:
		dat = in_csv.read()
		dat_lines = dat.splitlines()
		colnames = dat_lines[0].split( ',' )

		clean_file.writelines( dat_lines[0] + '\n' )
		trash_file.write( dat_lines[0] + '\n' )
		count = 0
		for row in dat_lines[ 1 : len( dat_lines ) ]: # this loop should not start at 0
			try:
				row_split = csv.reader( StringIO.StringIO( row ), delimiter=',' ).next()
				if len( row_split ) == len( colnames ): # test nfields
					row_dict = dict( [ [i,j] for i,j in zip( colnames, row_split ) ] )
					int(row_dict['MMSI'])
					float(row_dict['Longitude'])
					float(row_dict['Latitude'])
					float('.'.join(row_dict['Time'].split('_')))
					clean_file.writelines( row + '\n')
			except:
				count += 1
				trash_file.writelines( row + '\n' )
				pass

		clean_file_writer = None
		trash_file_writer = None
	trash_file.flush()
	trash_file.close()
	clean_file.flush()
	clean_file.close()
	print('number of bad rows = %s' % count)
	return os.path.join(out_csv_folder, os.path.basename(in_csv_path).replace('.csv', '_clean.csv'))



# def create_lines(dataframe, longitude_column, latitude_column, unique_column):
# 	"""
# 	do something here to create the needed input to create a linestring
# 		using shapely from the pandas data_frame
# 		--> we can deal with the creation of an acceptable dbf to attach to
# 		the lines following the generation of the density maps

# 		Dont forget about removing less than 2 element lines!
# 	"""
# 	out_lines = []
# 	trip_list = [dataframe[dataframe[unique_column] == i] for i in dataframe[unique_column].unique()]
# 	for trip in trip_list:
# 		trip = np.unique(trip['Time'])
# 		lonlat = trip[[longitude_column, latitude_column]]
# 		cur_line = [(float(i[1][longitude_column]), float(i[1][latitude_column])) for i in lonlat.iterrows()]
# 		if len(cur_line) > 2:
# 			out_lines.append( LineString( cur_line ) )
# 	return out_lines


def create_lines(dataframe, longitude_column, latitude_column, unique_column, input_epsg, output_epsg):
	"""
	takes as input a pandas data_frame object (this may change in the future to a file path) and converts 
	the data frame into unique transects (this is also most likely to change) and writes them out as a
	line shapefile.  

	arguments:
		dataframe - a pandas dataframe object created from a cleaned / joined / dropped ExactEarth CSV file
		longitude_column - string name of the column to be used to determine longitude.
		latitude_column - string name of the column to be used to determin latitude
		unique_column - string column name to be used to determine the a unique ship transect
		input_epsg - integer representation of the epsg code of the input file reference system
		output_epsg - integer representation of the epsg code of the output file reference system

	depends:
		pandas, shapely, pyproj
	
	returns:
		a tuple of lines (shapely) and a dataframe (pandas) of the values for the lines in that order

	"""
	
	out_lines = []
	out_rows = []
	trip_list = [dataframe[dataframe[unique_column] == i] for i in dataframe[unique_column].unique()]
	
	for trip in trip_list:
		trip = trip.sort('Time')
		lonlat = trip[[longitude_column, latitude_column]]
		
		# project the points to a new epsg BEFORE creating lines
		p1 = Proj(init='epsg:'+str(input_epsg))
		p2 = Proj(init='epsg:'+str(output_epsg))
		lonlat = [pyproj.transform(p1,p2,pt[1][longitude_column],pt[1][latitude_column]) for pt in lonlat.iterrows()]
		cur_line = [(float(i[0]), float(i[1])) for i in lonlat]
		
		if len(cur_line) > 2:
			out_lines.append( LineString( cur_line ) )
			out_rows.append( trip.head(1).to_dict() )

	out_df = pd.DataFrame.from_dict( out_rows )
	return out_lines, out_df 


def csv_to_shape(in_csv, out_shape, month=None, output_epsg=3338, input_epsg=4326, col_dtypes=None):
	"""
	convert a cleaned ExactEarth csv file and generate point shapefiles data.

	arguments:
	1. in_csv - a path to a csv file to be converted to a points shapefile
	2. out_shape - filename (with path) of the desired output shapefile
	3. month - an individual month integer between 1 and 12 to break the data
		into some more interesting subsets for seasonal visualizations.
	4. output_epsg - an accepted integer EPSG code for the output reference system
	5. input_epsg - an accepted integer EPSG code for the input reference system

	This function will take the points in the native reference system and output them 
	as a newly projected system.  In the case of this work, it has involved input_epsg of 4326
	and an output epsg of 3338.  These are the programmed defaults.

	depends:
	pyproj, fiona(ogr), shapely, shapefile

	"""
	from pyproj import Proj
	import fiona
	from fiona.crs import from_epsg

	r = csv.DictReader(open(in_csv, 'r'))
	header = r.next()

	if col_dtypes == None:
		# convert the cols into strings (bad practice but solves and issue)
		col_dtypes = dict([(i, str) for i in header.keys()])
		del r
	
	schema = { 'geometry': 'Point', 'properties': col_dtypes }

	with collection( out_shape, "w", "ESRI Shapefile", schema=schema, crs=from_epsg(output_epsg) ) as output:
		with open(in_csv, 'rb') as in_data:
			reader = csv.DictReader(in_data)
			for row in reader:
				# use pyproj to reproject x,y to output_epsg
				p1 = Proj(init='epsg:'+str(input_epsg))
				p2 = Proj(init='epsg:'+str(output_epsg))
				lon, lat = pyproj.transform(p1,p2,float(row['Longitude']),float(row['Latitude']))
				# deal with months - this is ugly - hack
				if month:
					# this line is hairy, but works for our purposes currently
					if int(row['Time'].split('_')[0][4:6]) == month:
						out_properties = OrderedDict([(i,str(row[i])) for i in col_dtypes.keys()])
						try:
							point = Point(lon, lat)
							output.write({
								'properties': out_properties,
								'geometry': mapping(point)
							})
						except:
							pass
				else:
					out_properties = dict([(i,str(row[i])) for i in header])
					try:
						point = Point(lon, lat)
						output.write({
							'properties': out_properties,
							'geometry': mapping(point)
						})
					except:
						pass
	return True


def drop_fields(in_csv, output_path, column_list):
	"""

	this function takes as input, the path to a csv file
	and a list of column names to keep from that file and will 
	write out a version of the file with only those columns named.

	arguments: 
	in_csv - path to a cleaned and joined (with IHS table) csv file
	output_path - a string of the output folder to put the outputs
	column_list - a python list of strings of filenames to keep.  This uses the 
	pandas dataframe read_csv with the usecols argument and writes it back out
	with the subset of columns retained.

	depends:
	pandas

	"""
	import pandas as pd
	
	output_name = os.path.join(output_path, os.path.basename(in_csv).replace('.csv', '_dropcols.csv'))
	f = open(in_csv, 'r')
	df = pd.read_csv(f, usecols=column_list)
	df.to_csv(output_name,  header=True, index=False)
	
	f.close()
	del f, df
	return True




# some prep columns lists to be read in to the data.frame
ship_types = ['Bulk Carriers','Dry Cargo/Passenger','Fishing','Miscellaneous','Non-Merchant Ships','Offshore','Tankers']

target_table_colnames = ['MMSI', 'Message_ID', 'Repeat_indicator', 'Time', 'Millisecond', 'Region', 'Country', 'Base_station', 
						'Online_data', 'Group_code', 'Vessel_Name', 'Call_sign', 'IMO', 'Ship_Type', 'Destination', 'ROT', 
						'SOG', 'Longitude', 'Latitude', 'COG', 'Heading']

join_table_colnames = ['ShipName', 'MMSI', 'PortofRegistryCode', 'ShiptypeLevel2', 'IMO']

col_dtypes_dict = dict([ ('MMSI','int'), ('Message_ID','str'), ('Repeat_indicator','str'), ('Time','str'), ('Millisecond','str'), 
				('Country','str'), ('Base_station','str'), ('Online_data','str'), ('Group_code','str'), ('ROT','str'), 
				('SOG','str'), ('Longitude','float'), ('Latitude','float'), ('COG','str'), ('Heading','str'), ('ShipName','str'), 
				('PortofRegistryCode','str'), ('ShiptypeLevel2','str'), ('IMO_ihs','int'), ('unique_trips','str'), ('unique_trips_dest','str'), 
				('time_modified','float') ] )

# mainline it
# def main():
# 	"""
# 	developing main function to do the work of the project

# 	"""
# step 1 set up some output directories 
# 	these will be sys.args
output_base_path = '/workspace/UA/malindgren/projects/ShippingLanes/AIS_Satellite_v2/outputs'
input_data_path = '/workspace/UA/malindgren/projects/ShippingLanes/AIS_Satellite_v2/data/ExactEarth_extracted_v2'
base_path = setup_dirs(output_base_path) # returns the output base path generated parent folder
# base_path = '/workspace/UA/malindgren/projects/ShippingLanes/AIS_Satellite_v2/outputs/Mon_Mar_31_2014_190122'
join_csv_path = '/workspace/UA/malindgren/projects/ShippingLanes/AIS_Satellite_v2/data/IHS_table/IHS_ShipData_Tim_Robertson_ShipTypeCodes_MLeditColnames.csv'
join_column = 'MMSI'
join_type = 'inner'


# step 2 read in the needed data and output to cleaned csv files
files = glob.glob(os.path.join(input_data_path, '*.csv'))
for f in files:
	print(f)
	print('cleaning data - intial row dirty row drop')
	f = remove_garbage_rows(f, os.path.join(base_path, 'csv', 'cleaned'))

	target_csv = pd.io.parsers.read_csv(f, usecols=target_table_colnames, dtype=str, error_bad_lines=False)
	join_csv = pd.io.parsers.read_csv(join_csv_path, usecols=join_table_colnames, dtype=str)

	try:
		# change the MMSI dtypes
		target_csv['MMSI'] = target_csv['MMSI'].astype(int)
		join_csv['MMSI'] = join_csv['MMSI'].astype(int)

		# now join the data with the IHS table
		# joined = target_csv.join(join_csv, on=join_column, how=join_type, lsuffix='_ee', rsuffix='_ihs', sort=True)
		print('joining data - inner join with IHS table')
		joined = target_csv.merge(join_csv, on=join_column, how=join_type, suffixes=('_ee','_ihs') )
		# cleanup

		del target_csv

		print('creating unique fields')
		# add in a unique field ( currently is MMSI, IMO and the date )
		#  this is a very simple way of doing this to get a single 
		#  transects for a ship for each day.
		date = [ i.split('_')[0] for i in joined[ 'Time' ].tolist() ]
		joined['unique_trips'] = joined['MMSI'].astype(str) + '_' + joined['IMO_ihs'].astype(str) + '_' + date

		# create a new field combining the MMSI, date, and Destination
		destination = [str(i) for i in joined['Destination'].tolist()]
		joined['unique_trips_dest'] = joined['MMSI'].astype(str) + '_' + joined['IMO_ihs'].astype(str) + '_' +date + '_' + destination

		# create a new Time field to be used for sorting
		date_time = [float('.'.join(i.strip().split('_'))) for i in joined[ 'Time' ].tolist()]
		joined[ 'time_modified' ] = date_time		

		# write out the joined data 
		joined.to_csv( os.path.join( base_path, 'csv', 'joined', os.path.splitext(os.path.basename(f))[0] + '_joined.csv'), mode='w', header=True, index=False)

		# get the unique values of ship_type as a list
		ship_types = np.unique(joined['ShiptypeLevel2']).tolist()
		
		print(ship_types)
		print(' - - - - - - - - - - - - - ')

		for ship_type in ship_types:
			print('  '+ship_type)
			output_filename = os.path.join(base_path, 'csv', 'grouped', re.sub('\W','_', ship_type) + '_grouped.csv')
			if os.path.exists(output_filename):
				# open the already instantiated outputs in append mode
				subset = joined[ joined[ 'ShiptypeLevel2' ] == ship_type ]
				subset.to_csv(output_filename, header=False, mode='a', index=False)
			else:
				subset = joined[ joined[ 'ShiptypeLevel2' ] == ship_type ]
				subset.to_csv(output_filename, header=True, mode='w', index=False)
	except:
		pass

	subset = None
	joined = None


print( ' # # # # # # DROP SOME UNNEEDED COLUMNS # # # # # # # # # ' )
file_list = glob.glob( os.path.join( base_path,'csv','grouped','*_grouped.csv' ) )
output_path = os.path.join(base_path, 'csv', 'dropped')
for f in file_list:
	print( f )
	try:
		drop_fields( f, output_path, col_dtypes_dict.keys() )
	except:
		print( 'ERROR!' )
		pass




# THIS IS POSSIBLY NOT WORKING AS EXPECTED!  FIX THIS !
print('# # # CONVERT TO POINTS SHAPEFILES # # # # # # #')
import csv
from shapely.geometry import Point, mapping
from fiona import collection


months = range(1,(12 + 1))
file_list = glob.glob(os.path.join(base_path,'csv','dropped','*_dropcols.csv'))
output_path = os.path.join(base_path, 'points')

for f in file_list:
	print(f)
	for month in months:
		out_shape = os.path.join(output_path, os.path.basename(f).replace('_grouped_dropcols.csv', '_pts_' + str(month) + '.shp'))
		print(month)
		csv_to_shape(f, out_shape, month, output_epsg=3338, input_epsg=4326, col_dtypes=col_dtypes_dict)


# convert the full set for each group
for f in file_list:
	print(f)
	out_shape = os.path.join(output_path, os.path.basename(f).replace('_grouped_dropcols.csv', '_pts_' + 'FULL' + '.shp'))
	csv_to_shape(f, out_shape, output_epsg=3338, input_epsg=4326, col_dtypes=col_dtypes_dict)



# # # # CONVERT POINT SHAPEFILES TO RASTERS # # # # # #
# send out an R worker to do the needed 
print('converting point shapefiles to rasters')
os.system('Rscript /workspace/UA/malindgren/projects/ShippingLanes/CODE/points_to_raster_hack.r ' + base_path)


# # # CONVERT TO LINES SHAPEFILES # # # # # # #

# [ ML ] THIS IS VERY CLOSE TO BEING COMPLETE BUT WE NEED TO WRITE IN THE 
#        DATA PASSING AND THE MONTH PARSER.  THEN ADD IN THE RASTERIZATION 
#		 WE ALSO NEED TO GET THE UNIQUE TRIPS ROWS ADDED IN !

import shapely
from shapely.geometry import *
import fiona
from fiona import collection
from fiona.crs import from_epsg

files = glob.glob(os.path.join(base_path,'csv','dropped','*_dropcols.csv'))

for f in files:
	count=0
	# cur_df_reader = pd.read_csv(f, dtype=str, error_bad_lines=False, chunksize=500000)
	cur_df_full = pd.read_csv(f, usecols=col_dtypes_dict.keys(), dtype=str, error_bad_lines=False)
	
	count += 1
	for month in months:
		cur_df = cur_df_full[cur_df_full['Time'].apply(lambda x: int(x.split('_')[0][4:6])) == month]
		try:
			trip_names = np.unique(cur_df['unique_trips'])
			print(f)
		
			cur_df['Longitude'] = cur_df['Longitude'].astype(float)
			cur_df['Latitude'] = cur_df['Latitude'].astype(float)
			cur_df['MMSI'] = cur_df['MMSI'].astype(int)
			cur_df['IMO_ihs'] = cur_df['IMO_ihs'].astype(int)
			cur_df['time_modified'] = cur_df['time_modified'].astype(float)

			# drop all rows with None the Longitude or Latitude field
			cur_df = cur_df[np.isfinite(cur_df['Longitude']) & np.isfinite(cur_df['Latitude'])]

			print(' creating lines @ ' + time.asctime())

			# create lines and output dataframe
			lines, out_df = create_lines(cur_df, 'Longitude', 'Latitude', 'unique_trips', 4326, 3338)

			# take the newly created lines layer and join it with the data frame that has been 
			#  modified to have one record per unique line set. Write that out as a shapefile for distribution
			print(' writing shapefile @ ' + time.asctime())
			
			# Define a polygon feature geometry with one attribute
			schema = {'geometry': 'LineString', 
					'properties': col_dtypes_dict}

			# Write a new Shapefile
			input_lines_path = os.path.join(base_path, 'lines', 'individual_shapefiles',\
				os.path.splitext(os.path.basename(f))[0] + '_LINES_' + str(month) + '.shp')
			# the id here needs to be iterative I think from the first line to end all lines (at least by group)
			with collection(input_lines_path, 'w', 'ESRI Shapefile', schema, crs=from_epsg(3338)) as c:
				for i in range(len(lines)):
					count += 1
					line = lines[i]
					row = out_df.ix[i,:]
					out_properties = dict([(i, row[i].values()[0]) for i in cur_df_full.columns.tolist() ])
					c.write({
						'geometry': mapping(line),
						'properties': out_properties,
					})

			# cleanup
			del cur_df, lines 

		except:
			print('ERROR: exception handled')
			pass


# # # # CONVERT LINES SHAPEFILES TO RASTERS # # # # # #





# as an aside we will run these through the ArcGIS 10.2 Simple Line Density algorithm because mine is too slow
#  even though it is parallelized. I will do more work on that function as I have some time

