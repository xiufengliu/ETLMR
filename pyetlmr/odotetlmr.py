'''
    ODOT (Online): One Dimension One Reducer

	Phase-1: Dimension Processing
	The input data is prepared by partitioning into several input files. Each mapper reads rows from a input file, and processes it by the UDF,  i.e., pruning unnecessary data.
	The map output is partitioned based on the hash value of the name of a dimension table such that all the data targeted for this dimension table
	will  be processed within a single reducer. Finally, the data for a dimension table will be loaded into the corresponding dimension table in the DW
	by bulk-load utility, COPY.

	Phase-2: Fact Processing:
	In mappers, ETLMR looks up the foreign key online (lookup dimension table in DW) .
	The map output is partitioned based on the names of  fact table such that the data of same fact will go into the same reducer, and the reducers will generate the CSV files and
    COPY the fact data to DW.
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
from disco.core import Disco, result_iterator, Params
from disco.func import default_partition
from mapreader import map_csv_reader

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'



map_reader = map_csv_reader

def dim_map_init(row, params):
	pass


def dim_map_func(row, params):
	dimnames = eval(params.dimnames)
	dimensions = [dim for dim in config.dimensions.keys() if dim.name in dimnames]
	dim_row = []
	for dimension in dimensions:
		srcfields = config.dimensions[dimension].get('srcfields',[])
		nrow = dict([(field, row[field]) for field in srcfields if row.has_key(field)])
		dim_row.append((dimension.name, repr(nrow)))
	return dim_row

dim_partition_func = default_partition

def dim_combiner_func(name, row, comb_buffer, done, params):
	if params.count>=50000:
		comb_buffer.clear()
	rows = comb_buffer.get(name, list())
	rows.append(row)
	if name and rows:
		comb_buffer[name] = rows
		params.count = params.count + 1
		if params.count>=50000:
			return comb_buffer.iteritems()
	if done:
		return comb_buffer.iteritems()


def dim_reduce_func(iter, out, params):
	dimnames = eval(params.dimnames)
	dimdict = dict([(dim.name, dim) for dim in config.dimensions.keys() if dim.name in dimnames])
	refdimdict = {}
	import pyetlmr
	for dim, refdims in config.references:
		if isinstance(refdims, pyetlmr.odottables.Dimension):
			refdims = (refdims, )
		refdimdict[dim] = refdims	
	for name, par_rows in iter:
		rows = eval(par_rows)
		dimension = dimdict.get(name)
		refdims = refdimdict.get(dimension, [])
		rowhandlers = config.dimensions[dimension].get('rowhandlers',[])
		namemapping = config.dimensions[dimension].get('namemappings',{})
		for row in rows:
			row = eval(row)
			for handler in rowhandlers:
				handler(row, namemapping)
			for refdim in refdims:
				refnamemapping = config.dimensions[refdim].get('namemappings',{})
				row[refdim.key] = refdim.lookup(row, refnamemapping)
			dimension.ensure(row, namemapping)

	config.connection.commit()



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