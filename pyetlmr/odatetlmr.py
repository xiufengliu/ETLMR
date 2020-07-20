'''   	
  ODAT - This Post-fixing Dimension Processing 
'''
#  Copyright (c) 2011 Xiufeng Liu (xiliu@cs.aau.dk)
#  
#  This file is free software: you may copy, redistribute and/or modify it  
#  under the terms of the GNU General Public License version 2 
#  as published by the Free Software Foundation.
#  
#  This file is distributed in the hope that it will be useful, but  
#  WITHOUT ANY WARRANTY; without even the implied warranty of  
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU  
#  General Public License for more details.  
#  
#  You should have received a copy of the GNU General Public License  
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.  
#  

import datetime, time, sys, os, getopt, tempfile
from disco.core import result_iterator, Params
from disco.func import re_reader, default_partition, msg
from mapreader import map_csv_reader

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'

map_reader = map_csv_reader

def dim_map_init(row, params):
	pass
	#if config.connection and config.connection.isclose():
	#	config.connection = config.UDF_createConnection()

def dim_map_func(row, params):
	dimensions = [dim for dim in config.dimensions.keys()]
	for dimension in dimensions:
		rowhandlers = config.dimensions[dimension].get('rowhandlers',[])
		namemapping = config.dimensions[dimension].get('namemappings',{})
		if rowhandlers:
			for handler in rowhandlers:
				handler(row, namemapping)
		dimension.ensure(row, namemapping)
	return []

dim_partition_func = default_partition

def dim_combiner_func(table, rows, tab_rows, done, params):
	if done:
		dimensions = [dim for dim in config.dimensions.keys()]
		for dimension in dimensions:	
			dimension.endload()
		config.connection.close()

def dim_reduce_func(iter, out, params):
	pass

# ----------------------------------------------------------------------
#         Fact table                                                            #
# ----------------------------------------------------------------------
def fact_map_init(row, params):
	pass

def fact_map_func(row, params):
	facts = config.facts.keys()
	for fact in facts:
		refereddims = config.facts[fact].get('refdims',[])
		namemappings = config.facts[fact].get('namemappings', {})
		rowhandlers = config.facts[fact].get('rowhandlers', [])
		for handler in rowhandlers:
			handler(row)
		for dim in refereddims:
			row[dim.key] = dim.lookup(row, namemappings)

		start = time.time()
		fact.insert(row)
		params.totalcopytime += time.time()-start
	return []

def fact_combiner_func(key, value, comb_buffer, done, params):
	if done:
		facts = config.facts.keys()
		for fact in facts:	
			fact.endload()
		config.connection.commit()

def golive(config, shelvedb_paths=[]):
	pass