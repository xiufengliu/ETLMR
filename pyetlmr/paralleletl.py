'''
Usage: python etlmr.py [options] input_paths
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

import os, getopt, sys, time, socket, multiprocessing
from thread import *
from optparse import OptionParser
from os import getenv
import pyetlmr
from conf import config
from disco.core import Disco, Params, result_iterator
import offdimetlmr, odotetlmr, odatetlmr
from postfix import post_fix

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'


def seq_init():
	conn = config.connection
	dims = set()
	seq={}
	for dimension in config.dimensions:
		if type(dimension)==pyetlmr.odattables.SnowflakedDimension:
			dims.update(dimension.sfdims)
		else:
			dims.add(dimension)
	for dim in dims:
		conn.execute("SELECT MAX(%(key)s) FROM %(name)s" %\
			                      {'key':dim.key, 'name':dim.name})
		maxid = conn.fetchonetuple()[0]
		if maxid:
			seq[dim.name] = maxid
	return seq

def client_thread(conn, seq):
	while True:	
		data = conn.recv(1024)
		nextid = seq.get(data, 1);
		seq[data] = nextid + 1
		if data=='END' or data=='': 
			break
		conn.sendall(str(nextid))
	conn.close()


def seq_server():
	HOST = ''	
	PORT = 8888
	MAX_CON_NUM = 20
	seq = seq_init()
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		s.bind((HOST, PORT))
	except socket.error , msg:
		print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
		sys.exit()    
	s.listen(MAX_CON_NUM)
	while True:
		conn, addr = s.accept()
		start_new_thread(client_thread ,(conn, seq))
	s.close()    

def load_dim(master, input, config_path, nr_maps=1, \
             nr_reduces=1, load_method=offdimetlmr, \
             post_fixing=-1, go_live=1, profile=False):
	try:
		order = config.order
	except Exception:
		order = [config.dimensions.keys()]

	dim_starttime = time.time()
	for dims in order:
		dimnames = repr([dim.name for dim in dims])
		print "Loading %s ..." % str(dimnames)
		load_one_dim(master, input, config_path, nr_maps,\
		             nr_reduces, load_method, dimnames, go_live, profile)
	dim_endtime = time.time()
	print "Time of loading dimensions: %f seconds" % (dim_endtime-dim_starttime)
	
	if post_fixing==1:
		post_fix(config)
	
def load_one_dim(master, input, config_path, nr_maps=1, nr_reduces=1,\
                 load_method=offdimetlmr, dimnames= repr([]), \
                 go_live=1, profile=False):
	dim_job = master.new_job(
		name = 'dim',
		input = input,
		map_init = load_method.dim_map_init,
		map_reader = load_method.map_reader,
		map = load_method.dim_map_func,
	        partition = load_method.dim_partition_func,
		combiner = load_method.dim_combiner_func,
		reduce = load_method.dim_reduce_func,
		scheduler = {'max_cores': nr_maps},
		nr_reduces = nr_reduces,
		required_modules=[('config', config_path)],
		profile = profile,
		status_interval = 1000000,
		params = Params(count=0, dimnames=dimnames, \
	                        nr_maps=nr_maps, nr_reduces=nr_reduces)
	)
	results = dim_job.wait()
	shelvedb_paths = []
	if results!=None:
		for key,value in result_iterator(results):
			shelvedb_paths.append(key)
		if go_live==1:
			load_method.golive(config, shelvedb_paths)
	#results = dim_job.wait(show=True, poll_interval = 100, timeout = 10*3600)
	#dim_job.purge()

def load_fact(master, input, config_path, nr_maps=1, nr_reduces=1, \
              load_method=offdimetlmr, profile=False):
	#disco = Disco("disco://"+host)
	fact_starttime = time.time()
	fact_job = master.new_job(
		name = 'fact',
		input = input,
		map_init = load_method.fact_map_init,
		map_reader = load_method.map_reader,
		map = load_method.fact_map_func,
		combiner = load_method.fact_combiner_func,
		scheduler = {'max_cores': nr_maps},
		nr_reduces = nr_reduces,
		required_modules=[('config', config_path),],
		status_interval = 1000000,
		profile = profile,
		params = Params(totalcopytime=0, nr_maps=nr_maps, \
	                        nr_reduces=nr_reduces)
	)
	results = fact_job.wait()
	#results = fact_job.wait(show=True, poll_interval = 100, timeout = 10*3600)
	fact_endtime = time.time()
	print "Time of loading facts: %f seconds" % (fact_endtime-fact_starttime)
	#fact_job.purge()



if __name__== "__main__":
	disco_home = os.environ["DISCO_HOME"]
	parser = OptionParser(usage='%etlmr [options] input_paths')
	parser.add_option('--disco-master',
	                  default=getenv('DISCO_MASTER'),
	                  help='Disco master')
	parser.add_option('--nr-maps',
	                  default=2,
	                  help='Numbers of mappers (default=1)')
	parser.add_option('--nr-reducers',
	                  default=2,
	                  help='Numbers of reducers (default=1)')
	parser.add_option('--load-step',
	                  default=1,
	                  help='Loading step (default=1): 1. Load dimensions; \
	                  2. Load facts')
	parser.add_option('--load-method',
	                  default=1,
	                  help='Loading method of dimensions (default=1): 1. Online ODOT; \
	                  2. Online ODAT; 3. Offline dim')
	parser.add_option('--post-fix',
	                  default=1,
	                  help='Does post-fixing for ODAT? (default=1): 1. Yes; 2. No')	
	parser.add_option('--go-live',
	                  default=1,
	                  help='Load offline dim data to DW DBMS? (default=1): 1. yes; 2. No')		
	parser.add_option('--profile',
	                  default=False,
	                  help='Profile (default=False)')
	parser.add_option('--config',
	                  default='conf/config.py',
	                  help='The path to config.py (default=conf/config.py)')

	(options, input_paths) = parser.parse_args()
	master = Disco("disco://"+options.disco_master)	
	
	load_method = odotetlmr
	seq_process = None
	post_fixing = -1
	load_step = int(options.load_step)
	if options.load_method=='2':
		load_method = odatetlmr
		if  load_step==1:
			post_fixing = int(options.post_fix)
			seq_process = multiprocessing.Process(target=seq_server)
			seq_process.start()		
	elif options.load_method=='3':
		load_method = offdimetlmr
		
	input_file_urls = []	
	for input_path in input_paths:
		input_files = [f for f in os.listdir(input_path) if \
		               os.path.isfile(os.path.join(input_path, f))]
		prefix = input_path.partition(os.path.join(disco_home, 'root', 'input'))[2]
		input_file_urls.extend(['dfs://%s%s' % (options.disco_master, \
		                                        os.path.join(prefix, f)) \
		                        for f in input_files])
	print "input_file_urls=%s" % str(input_file_urls)
	if load_step==1:
		load_dim(master, input_file_urls, config_path=options.config,\
		         nr_maps=int(options.nr_maps), 
		         nr_reduces=int(options.nr_reducers), load_method=load_method, \
		         post_fixing=post_fixing, go_live=int(options.go_live), profile=options.profile)
		if seq_process:
			seq_process.terminate()		
	elif load_step==2:
		load_fact(master, input_file_urls, config_path=options.config, \
		          nr_maps=int(options.nr_maps), 
		         nr_reduces=int(options.nr_reducers),load_method=load_method,\
		         profile=options.profile)
	else:
		parser.print_help()
		
	
	