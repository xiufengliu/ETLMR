'''
This is only applicable for the STAR SCHEMA processing.

Dimension processing: Process ONE big dimension in all mappers; All the other dimesions (called small dimmensions) are processed in reducers, 
and each recuder processes only data for one dimension table.
Fact processing: Process fact data in maps, no reducer.

------------ Dimension Processing -------------
1) The input data is partitioned by the business key of ONE very big dimension, for example hash(row[uri])%nr_map.
2) Each map only processes the very big dimension data which the value of hash(row[uri])%nr_map equals to its own taskid.
3) The data of small dimensions will be sent to reducers;
4) The combiner of each map will combine the values of the same dimension, and send key-value pair (dimensionname, [(...),(...),]) to reducer;
5) The reduce process the data of each small dimension;
8) Generate generate offline dimmension for prefilled dimension, then synchronized the offline dimensions across all the workers

--------- Fact processing ---------------------
1) Each mapper read part of all offline dimension data into memory in map init(for speed up the lookup but be care memory overflow)
2) In map, lookup dimension key from offline dimensions and generate temporary csv file when buffer is full and commit to DW;
3) No reducer

The advantage of this offline dimension methodology is good for  denormalized dimension (star-schema) loading.
'''
#
# Copyright (c) 2011 Xiufeng Liu (xiliu@cs.aau.dk)
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
import tempfile, datetime, time, sys, os, socket, getopt
from disco.core import Disco, result_iterator, Params
from disco.func import re_reader, default_partition
from subprocess import Popen, call
from commands import getstatusoutput
from mapreader import map_csv_reader_bkey
from mapreader import map_csv_reader
from lrustore import LRUShelve
from unicodecsv import UnicodeWriter

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'


#map_reader = map_csv_reader_bkey
map_reader = map_csv_reader

def dim_map_init(row, params):
	for dimension in config.dimensions.keys():
		if dimension.is_bigdim():
			dimension.open_shelveddb(taskid=this_partition())

def dim_map_func(row, params):
	dimensions = config.dimensions.keys()
	dim_row = []
	for dimension in dimensions:
		if dimension.is_bigdim():  # Process the large dimension in mapper
			rowhandlers = config.dimensions[dimension].get('rowhandlers',[])
			namemapping = config.dimensions[dimension].get('namemappings',{})
			for handler in rowhandlers:
				handler(row, namemapping)
			dimension.ensure(row, namemapping)
		else: # Send the data of small dimensions to reducers
			srcfields = config.dimensions[dimension].get('srcfields',[])
			nrow = dict([(field, row[field]) for field in srcfields if row.has_key(field)])
			dim_row.append((dimension.name, repr(nrow)))
	return dim_row

dim_partition_func = default_partition

dim_combiner_func = None



def dim_combiner_func_bigdim(name, row, comb_buffer, done, params):
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
		dimensions = config.dimensions.keys()
		for dimension in dimensions:
			if dimension.is_bigdim():
				dimension.endload()
		return comb_buffer.iteritems()


def dim_combiner_func_using_list(name, row, comb_buffer, done, params):
	if params.count>=50000:
		comb_buffer.clear()
		params.count = 0

	rows = comb_buffer.get(name, [])
	if not row in rows:
		rows.append(row)
		comb_buffer[name] = rows
		params.count = params.count + 1
		if params.count>=50000:
			return comb_buffer.iteritems()

	if done:
		dimensions = config.dimensions.keys()
		for dimension in dimensions:
			if dimension.is_bigdim():
				dimension.endload()
		return comb_buffer.iteritems()


def dim_reduce_func(iter, out, params):
	'''
	Process the data of all small dimensions
	'''
	rowsdict = {}
	for name, par_row in iter:
		rows = rowsdict.get(name, [])
		row = eval(par_row)
		if not row in rows:
			rows.append(row)
		rowsdict[name] = rows
		
	dimdict = dict([(dim.name, dim) for dim in config.dimensions.keys()])
	for name, rows in rowsdict.iteritems():
		dimension = dimdict.get(name)
		dimension.open_shelveddb() # open the offline dimension
		rowhandlers = config.dimensions[dimension].get('rowhandlers',[])
		namemapping = config.dimensions[dimension].get('namemappings',{})
		for row in rows:
			for handler in rowhandlers:
				handler(row, namemapping)
			dimension.ensure(row, namemapping)
		out.add(dimension.shelvedpath, this_host())
		dimension.endload()
	#config.connection.commit()


def dim_reduce_func_bigdim(iter, out, params):
	opened_dims = []
	dimdict = dict([(dim.name, dim) for dim in config.dimensions.keys()])
	for name, par_rows in iter:
		rows = par_rows
		dimension = dimdict.get(name)
		if not dimension in opened_dims:
			dimension.open_shelveddb()
			opened_dims.append(dimension)
		rowhandlers = config.dimensions[dimension].get('rowhandlers',[])
		namemapping = config.dimensions[dimension].get('namemappings',{})
		for row in rows:
			row = eval(row)
			for handler in rowhandlers:
				handler(row, namemapping)
			dimension.ensure(row, namemapping)

	for dimension in opened_dims:
		out.add(dimension.shelvedpath, this_host())
		dimension.endload()
	#config.connection.commit()



# ----------------------------------------------------------------------
#         Fact table                                                            #
# ----------------------------------------------------------------------
def fact_map_init(row, params):
	facts = config.facts.keys()
	dims = set()
	for fact in facts:
		refdims = config.facts[fact].get('refdims',[])
		for dim in refdims:
			dims.add(dim)
	for dim in dims:
		if dim.is_bigdim():
			dim.open_shelveddb(taskid=this_partition(),readonly=True)
		else:
			dim.open_shelveddb(readonly=True)

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

def fact_combiner_func(key, value, comb_buffer, flush, params):
	if flush:
		facts = config.facts.keys()
		for fact in facts:
			fact.endload()
		#config.connection.commit()
	#	msg("TotalCopyTime=%d"%params.totalcopytime)

# -------------------------------------------------------------------------------

def run_command(cmd):
	print 'cmd: "%s"' % cmd
	status, text = getstatusoutput(cmd)
	exit_code = status >> 8 # high byte
	#print 'Exit  : %d' % exit_code
	#print text
	return exit_code



def scp_file(shelvedpath, hostname):
	ssh = []
	if "SSH_KEY" in os.environ:
		ssh += ["-i", os.environ["SSH_KEY"]]
	user = os.environ.get("SSH_USER", "")

	status = Popen('which ionice'.split()).wait()
	if status == 0:
		ionice = 'ionice -n 7'
	else:
		ionice = ''
	unode = hostname
	if user:
		unode = "%s@%s" % (user, hostname)
	call(["ssh"] + ssh + [unode, "rm -f %s" % shelvedpath])
	p = Popen("nice -19 %s scp %s %s %s:%s" % (ionice, " ".join(ssh), shelvedpath, unode, shelvedpath), shell = True)
	if p.wait():
		print >> sys.stderr, "Copying to %s failed" % hostname
	else:
		print >> sys.stderr, "%s ok" % hostname

def pre_fill_dimensions():
	path_addr = {}
	from config import prefilleddims
	for dim in prefilleddims:
		dim.shelve_prefill_dim()
		path_addr[dim.shelvedpath] = socket.getfqdn()
	return path_addr

def sync_dims_across_servers(results):
	if "DISCO_CONFIG" not in os.environ:
		print >> sys.stderr, "Specify DISCO_CONFIG(data lives at $DISCO_ROOT/data)"
		sys.exit(1)
	conf = os.environ["DISCO_CONFIG"]
	f = open(conf, 'r')
	line = f.readline()
	serverlist = eval(line.rstrip('\n'))
	servers = set()
	for addr, nodes in serverlist:
		if int(nodes):
			if addr in ['127.0.0.1', 'localhost']:
				addr = socket.getfqdn()
			servers.add(addr)
	prefilldim_addr = pre_fill_dimensions()
	for path, addr in prefilldim_addr.iteritems():
		targetservers = servers - set([addr])
		for target in targetservers:
			scp_file(path, target)

	for path, addr in result_iterator(results):
		if addr in ['127.0.0.1', 'localhost']:
			addr = socket.getfqdn()
		targetservers = servers - set([addr])
		for target in targetservers:
			scp_file(path, target)


def golive(config, shelvedb_paths=[]):
	if shelvedb_paths:
		csvfilepath = None
		for shelvedb_path in shelvedb_paths:
			columns = []
			name = shelvedb_path.rpartition('/')[-1]
			for dimension in config.dimensions:
				if name==dimension.name:
					columns = dimension.all
					break				
			shelveddb = LRUShelve(shelvedb_path, 2000, readonly=True)
			fd, csvfilepath = tempfile.mkstemp(suffix='.csv', prefix=name)
			tmpfile = file(csvfilepath, 'w')
			csvwriter = UnicodeWriter(tmpfile, delimiter='\t')          
			for key, rows in shelveddb.iterms():
				for row in rows:
					values = []
					for i in range(len(columns)):
						value = row[i]
						if value == None:
							value = ''
						values.append(value)
					csvwriter.writerow(values)
			tmpfile.close()
			os.close(fd)
			config.UDF_pgcopy(name, columns, '\t', None, '', file(csvfilepath))
			shelveddb.close()
			os.remove(csvfilepath)