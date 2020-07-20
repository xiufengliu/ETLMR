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
import shelve

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'


def map_csv_reader_re(fd, content_len, fname):   
	content = re_reader("(.*?)\r\n", fd, content_len, fname, output_tail = True)
	line = content.next()[0]
	fieldnames = line.split('\t')
	while True:
		line = content.next()[0]
		if not line:
			break
		yield dict(zip(fieldnames, line.split('\t')))

def map_csv_reader_bkey(fd, content_len, fname, params):
	from csv import DictReader
	rows = DictReader(fd, delimiter='\t')  
	nr_maps = params.nr_maps
	for row in rows:
		if hash(row['url'])%nr_maps==this_partition():
			yield row

def map_csv_reader(fd, content_len, fname):
	from csv import DictReader
	rows = DictReader(fd, delimiter='\t')
	for row in rows:
		yield row

def map_socket_reader(fd, content_len, fname):
	import socket    
	lsnr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	lsnr.connect(config.datasrc_conn)
	flo = lsnr.makefile('r', 1024*16)
	for row in flo:
		yield eval(row)
		
