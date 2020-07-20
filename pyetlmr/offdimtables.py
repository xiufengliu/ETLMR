#  This file contains the code for the pygrametl-based solution
#  presented in C. Thomsen & T.B. Pedersen's
# "pygrametl:  A Powerful Programming Framework for Extract--Transform--Load Programmers"
#
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
import types, tempfile, os, time
import pyetlmr as etlmr
from lrustore import LRUShelve
from disco.util import msg
from unicodecsv import UnicodeWriter

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'


__all__ = ['CachedDimension', 'SlowlyChangingDimension',  'BulkFactTable']

class CachedDimension(object):

	def __init__(self, name, key, attributes, lookupatts=(), defaultidvalue=None, 
	             targetconnection=None, shelvedpath=None, cachesize=2000, 
	             prefill=False, bigdim=False):

		if not type(key) in types.StringTypes:
			raise ValueError, "Key argument must be a string"
		if not len(attributes):
			raise ValueError, "No attributes given"

		if targetconnection is None:
			targetconnection = etlmr.getdefaulttargetconnection()
			if targetconnection is None:
				raise ValueError, "No target connection available"

		self.con = targetconnection

		self.name = name
		self.attributes = attributes
		self.key = key
		if lookupatts == ():
			lookupatts = attributes
		self.lookupatts = lookupatts    
		self.all = [key,]
		self.all.extend(attributes)
		self.defaultidvalue = defaultidvalue

		self.shelvedpath = shelvedpath
		self.cachesize = cachesize
		self.shelveddb = None
		self.prefill = prefill
		self.bigdim = bigdim
		self.bigdimid = 0


	def open_shelveddb(self, taskid=None, readonly=False):
		if self.shelveddb is None:
			if taskid is not None:
				self.shelvedpath = (self.shelvedpath + "%d") % taskid
			self.shelveddb = LRUShelve(self.shelvedpath, self.cachesize, readonly=readonly)

	def is_bigdim(self):
		return self.bigdim

	def _getnextid(self):
		if self.bigdim:
			if self.bigdimid % 10000==0:
				self.con.execute("SELECT NEXTVAL('seq_%s')"%self.key)
				row = self.con.fetchone((self.key,))
				self.bigdimid = row[self.key]
				return self.bigdimid
			else:
				self.bigdimid = self.bigdimid + 1
				return self.bigdimid
		else:       	
			return self.shelveddb.get_nextid()


	def shelve_prefill_dim(self):
		if self.prefill:
			self.open_shelveddb()
			sql = 'SELECT %s FROM %s' % (','.join(self.all), self.name)
			self.con.execute(sql)
			for row in self.con.fetchalltuples():
				nrow = dict(zip(self.all, row))
				searchtuple = tuple(nrow[n] for n in self.lookupatts)
				rows = self.shelveddb.get(searchtuple, [])
				rows.append(row)
				self.shelveddb[searchtuple] = rows
			del self.shelveddb

	def lookup(self, row, namemapping={}):
		namesinrow =[(namemapping.get(a) or a) for a in self.lookupatts]
		searchtuple = tuple(row[n] for n in namesinrow)
		rows = self.shelveddb.get(searchtuple)
		if rows:
			nrow = dict(zip(self.all, rows[0]))
			return nrow[self.key]
		else:
			return self.defaultidvalue


	def ensure(self, row, namemapping={}):
		res = self.lookup(row, namemapping)
		if res==self.defaultidvalue:
			res = self.insert(row, namemapping)
		return res

	def insert(self, row, namemapping={}):
		key = (namemapping.get(self.key) or self.key)
		if row.get(key) is None:
			keyval = self._getnextid()
			row[key] = keyval
			keyadded = True
		else:
			keyval = row[key]
			keyadded = False
		# Insert to shelve db
		searchtuple = tuple(row[(namemapping.get(a) or a)] for a in self.lookupatts)
		nrow = tuple(row[(namemapping.get(a) or a)] for a in self.all)
		self.shelveddb[searchtuple] = [nrow]

		if keyadded:
			del row[key]
		return keyval

	def shelved2onlinedb(self):
		if len(self.shelveddb)>0:
			csvfilepath = None
			curs = None
			try:
				fd, csvfilepath = tempfile.mkstemp(suffix='.csv', prefix=self.name)
				tmpfile = file(csvfilepath, 'w')
				csvwriter = UnicodeWriter(tmpfile, delimiter='\t')          
				for k in self.shelveddb:
					csvwriter.writerows(self.shelveddb[k])
				tmpfile.close()
				os.close(fd)
				curs = self.con.cursor()
				curs.execute('TRUNCATE %s' % self.name)
				curs.copy_from(file=file(csvfilepath), table=self.name, sep='\t', null='', columns=self.all)
			finally:
				if curs:
					curs.close()
				os.remove(csvfilepath)     

	def endload(self):
		if self.shelveddb:
			#self.shelved2onlinedb()
			del self.shelveddb
		self.shelveddb = None



class SlowlyChangingDimension(CachedDimension):

	def __init__(self, name, key, attributes, lookupatts, versionatt, 
		         fromatt=None, toatt=None, srcdateatt=None, srcdateparser=etlmr.ymdparser,
		         type1atts=(), defaultidvalue=None, targetconnection=None,shelvedpath=None, 
	             cachesize=2000, prefill=False, bigdim=False):
		
		CachedDimension.__init__(self, name, key, attributes, lookupatts, defaultidvalue, 
		                         targetconnection, shelvedpath, cachesize, prefill, bigdim)
		if not versionatt:
			raise ValueError, 'A version attribute must be given'

		self.versionatt = versionatt
		self.fromatt = fromatt

		if srcdateatt is not None: 
			self.fromfinder = etlmr.datereader(srcdateatt, srcdateparser)
		else:                     
			self.fromfinder = etlmr.today
		self.toatt = toatt
		self.tofinder = self.fromfinder
		self.srcdateatt = srcdateatt
		self.srcdateparser = srcdateparser
		self.type1atts = type1atts

		# Check that versionatt, fromatt and toatt are also declared as attributes
		for var in (versionatt, fromatt, toatt):
			if var and var not in attributes:
				raise ValueError, "%s not present in attributes argument" % (var,)

	def lookup(self, row, namemapping={}):
		searchtuple, rows = self._get_rows(row, namemapping)
		if rows is None:
			return  self.defaultidvalue
		else:
			if self.type1atts:
				nrow = dict(zip(self.all, rows[0]))
				return nrow[self.key]
			else:   
				size = len(rows)
				for i in xrange(1, size+1):
					nrow = dict(zip(self.all, rows[-i]))
					validfrom = nrow[self.fromatt]
					validto = nrow.get(self.toatt) or  '9999-12-31' 
					srcdate = row[namemapping.get(self.srcdateatt) or self.srcdateatt]
					if validfrom<=srcdate and srcdate < validto:
						return nrow[self.key]
				return self.defaultidvalue

	def _get_rows(self, row, namemapping={}):
		namesinrow =[(namemapping.get(a) or a) for a in self.lookupatts]
		searchtuple = tuple(row[n] for n in namesinrow)
		return (searchtuple, self.shelveddb.get(searchtuple, None))

	def _type1_ensure(self, row, namemapping={}):
		searchtuple, rows = self._get_rows(row, namemapping)
		if not rows:
			return self.insert(row, namemapping)
		else:
			rowaddeddict = dict(zip(self.all, rows[0]))
			type1updates = {}
			for att in self.type1atts:
				if row[att]!=rowaddeddict[att]:
					type1updates[att] = row[att]
			if type1updates: # Update in the shelved db
				rowaddeddict.update(type1updates)
				updatedrow = tuple(rowaddeddict[att] for att in self.all)
				self.shelveddb[searchtuple] = [updatedrow]
			return rowaddeddict[self.key]

	def _type2_ensure(self, row, namemapping={}):
		searchtuple, rows = self._get_rows(row, namemapping)
		key = (namemapping.get(self.key) or self.key)
		versionatt = (namemapping.get(self.versionatt) or self.versionatt)     
		fromatt = (namemapping.get(self.fromatt) or self.fromatt)
		toatt = (namemapping.get(self.toatt) or self.toatt)    
		if not rows: # Add the first version of row
			row[versionatt] = 1           
			if fromatt and fromatt not in row:
				row[fromatt] = str(self.fromfinder(self.con, row, namemapping)).strip("':date ")
				

			if toatt and toatt not in row:
				row[toatt] = None
			return self.insert(row, namemapping)
		else: # Add new version
			addnewversion = False
			other = dict(zip(self.all, rows[-1]))

			rowdate = None
			if self.srcdateatt is not None:
				rdt = self.srcdateparser(row[self.srcdateatt])
				modref = self.con.getunderlyingmodule()
				rowdate = modref.Date(rdt.year, rdt.month, rdt.day)
			else:
				rowdate = self.fromfinder(self.con, row, namemapping)

			if str(rowdate).strip("':date ")>other[self.fromatt]:
				for att in self.all:
					if att in (self.key, self.fromatt, self.versionatt, self.toatt):
						continue
					else:
						mapped = (namemapping.get(att) or att)
						if row[mapped] != other[att]:
							addnewversion = True
							break
			if addnewversion:
				row.pop(key, None)
				row[versionatt] = other[self.versionatt] + 1
				if fromatt:
					row[fromatt] = rowdate
				if toatt:
					row[toatt] = None
				row[key] = self._getnextid()

				if toatt:
					toattval = self.tofinder(self.con, row, namemapping)
					# Update the todate attribute in the old row version in the shelve DB.     
					other[self.toatt] = str(toattval).strip("':date ")
					rows.pop()
					rows.append(tuple(other[a] for a in self.all))
				if fromatt:
					row[fromatt] = str(row[fromatt]).strip("':date ")
				nrow = tuple(row[(namemapping.get(a) or a)] for a in self.all)
				rows.append(nrow)
				self.shelveddb[searchtuple] = rows
				return row[key]
			else:
				return other[self.key]

	def ensure(self, row, namemapping={}):
		if self.type1atts:
			return self._type1_ensure(row, namemapping)
		else:
			return self._type2_ensure(row, namemapping)


SCDimension = SlowlyChangingDimension




class BulkFactTable(object):
	"""Class for addition of facts to a fact table. Reads are not supported. """

	def __init__(self, name, keyrefs, measures, bulkloader, 
		         fieldsep='\t', rowsep='\n', nullsubst=None,
		         tempdest=None, bulksize=500000):
		"""Arguments:
		   - name: the name of the fact table in the DW
		   - keyrefs: a sequence of attribute names that constitute the
		     primary key of the fact tables (i.e., the dimension references)
		   - measures: a possibly empty sequence of measure names. Default: ()
		   - bulkloader: A method 
		     m(name, attributes, fieldsep, rowsep, nullsubst, tempdest)
		     that is called to load data from a temporary file into the DW.
		     The argument attributes is the combination of keyrefs and measures
		     and show the order in which the attribute values appear in the 
		     temporary file. The rest of the arguments are similar to those
		     described here.
		   - fieldsep: a string used to separate fields in the temporary 
		     file. Default: '\\t'
		   - rowsep: a string used to separate rows in the temporary file.
		     Default: '\\n'
		   - nullsubst: an optional string used to replace None values.
		     If nullsubst=None, no substitution takes place. Default: None
		   - tempdest: a file object or None. If None a named temporary file
		     is used.
		   - bulksize: an int deciding the number of rows to load in one
		     bulk operation.
		"""

		self.name = name
		self.all = [k for k in keyrefs] + [m for m in measures]
		self.__close = False
		if tempdest is None:
			self.__close = True
			self.__namedtempfile = tempfile.NamedTemporaryFile()
			tempdest = self.__namedtempfile.file
		self.fieldsep = fieldsep
		self.rowsep = rowsep
		self.nullsubst = nullsubst
		self.bulkloader = bulkloader
		self.tempdest = tempdest

		self.bulksize = bulksize
		self.__count = 0
		self.__ready = True

		if nullsubst is None:
			self.insert = self._insertwithoutnulls
		else:
			self.insert = self._insertwithnulls

		etlmr._alltables.append(self)



	def __preparetempfile(self):
		#self.__namedtempfile = tempfile.NamedTemporaryFile()
		#self.tempdest = self.__namedtempfile.file
		self.tempdest = open(self.tempdest,  'w')
		self.__ready = True

	def insert(self, row, namemapping={}):
		"""Insert a fact into the fact table.

		   Arguments:
		   - row: a dict at least containing values for the keys and measures.
		   - namemapping: an optional namemapping (see module's documentation)
		"""
		pass # Is set to _insertwithnulls or _inserwithoutnulls from __init__

	def _insertwithnulls(self, row, namemapping={}):
		"""Insert a fact into the fact table.

		   Arguments:
		   - row: a dict at least containing values for the keys and measures.
		   - namemapping: an optional namemapping (see module's documentation)
		"""
		if not self.__ready:
			self.__preparetempfile()
		rawdata = [row[namemapping.get(att) or att] for att in self.all]
		data = [etlmr.getstrornullvalue(val, self.nullsubst) \
				for val in rawdata]
		self.__count += 1
		self.tempdest.write("%s%s" % (self.fieldsep.join(data), self.rowsep))
		if self.__count == self.bulksize:
			self.__bulkloadnow()

	def _insertwithoutnulls(self, row, namemapping={}):
		"""Insert a fact into the fact table.

		   Arguments:
		   - row: a dict at least containing values for the keys and measures.
		   - namemapping: an optional namemapping (see module's documentation)
		"""
		if not self.__ready:
			self.__preparetempfile()
		#msg("row=%s"% str(row))
		data = [str(row[namemapping.get(att) or att]) for att in self.all]
		self.__count += 1
		self.tempdest.write("%s%s" % (self.fieldsep.join(data), self.rowsep))
		if self.__count == self.bulksize:
			self.__bulkloadnow()


	def __bulkloadnow(self):
		start = time.time()
		self.tempdest.flush()
		self.tempdest.seek(0)
		self.bulkloader(self.name, self.all, 
				        self.fieldsep, self.rowsep, self.nullsubst,
				        self.tempdest)
		self.tempdest.seek(0)
		self.tempdest.truncate(0)
		self.__count = 0
		end = time.time()

	def endload(self):
		"""Finalize the load."""
		if self.__count > 0:
			self.__bulkloadnow()
		if self.__close:
			self.__namedtempfile.close()
			self.__ready = False
			