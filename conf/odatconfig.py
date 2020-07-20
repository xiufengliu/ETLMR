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

import time, datetime, psycopg2
import pyetlmr
from pyetlmr.odattables import CachedDimension, \
     SnowflakedDimension,SlowlyChangingDimension, \
     BulkFactTable, FactTable
from pyetlmr import getint, getdate, \
     datereader, getvalue

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'


#---- Define the database setting -------
dbconfig = {
'module'    : psycopg2,
'hostname'  : 'localhost',
'port'      : '5432',
'database'  : 'demo',
'username'  : 'demouser',
'password'  : 'demouser'
}

#-- Define the UDFs  --------------------
def UDF_createConnection():
	if dbconfig['module']==psycopg2:
		conn = psycopg2.connect(host=dbconfig['hostname'], \
	                                port=dbconfig['port'], \
	                                database=dbconfig['database'], \
	                                user=dbconfig['username'], \
	                                password=dbconfig['password'])
	else:
		raise Exception('Cannot esbablish db connection!')
	wrappedconn = pyetlmr.ConnectionWrapper(conn)
	wrappedconn.execute('set search_path to etlmr')
	wrappedconn.setasdefault()
	return wrappedconn


connection = UDF_createConnection()

def UDF_datehandling(row, namemapping):
	# This method is called from ensure(row) when the lookup of a date fails.
	# We have to calculate all date related fields and add them to the row.
	date = getvalue(row, 'date', namemapping)
	(year, month, day, hour, minute, second, weekday, dayinyear, dst) = \
	        time.strptime(date, "%Y-%m-%d")
	(isoyear, isoweek, isoweekday) = \
	        datetime.date(year, month, day).isocalendar()
	row['day'] = day
	row['month'] = month
	row['year'] = year
	row['week'] = isoweek
	row['weekyear'] = isoyear
	return row

def UDF_pgcopy(name, atts, fieldsep, rowsep, nullval, filehandle):
	global connection
	curs = connection.cursor()
	curs.copy_from(file=filehandle, table=name, sep=fieldsep,
	               null=str(nullval), columns=atts)

# ----------------------------------------
topleveldomaindim = CachedDimension(
        name='topleveldomaindim', 
        key='topleveldomainid',
        attributes=['topleveldomain'])

domaindim = CachedDimension(
        name='domaindim', 
        key='domainid', 
        attributes=['domain', 'topleveldomainid'],
        lookupatts=['domain']
)

serverdim = CachedDimension(
        name='serverdim', 
        key='serverid',
        attributes=['server']
)

serverversiondim = CachedDimension(
        name='serverversiondim', 
        key='serverversionid',
        attributes=['serverversion', 'serverid']
)

pagedim = SlowlyChangingDimension(
        name='pagedim', 
        key='pageid',
        attributes=['url', 'size', 'validfrom', 'validto', 'version', 
                    'domainid', 'serverversionid'],
        lookupatts=['url'],
        versionatt='version', 
        fromatt='validfrom',
        toatt='validto', 
        srcdateatt='lastmoddate',                     
        cachesize=-1,
        defaultidvalue=-1)

references = [(pagedim, (serverversiondim, domaindim)),
              (serverversiondim, serverdim),
              (domaindim, topleveldomaindim)
              ]


pagesf = SnowflakedDimension(references)
# ----------------------------------------
testdim = CachedDimension(
        name='testdim', 
        key='testid',
        attributes=['testname'],
        lookupatts=['testname'], 
        prefill=True, 
        defaultidvalue=-1
)
# ----------------------------------------

datedim = CachedDimension(
        name='datedim', 
        key='dateid',
        attributes=['date', 'day', 'month', 'year', 'week', 'weekyear'],
        lookupatts=['date'], 
        defaultidvalue=-1
)
# ----------------------------------------    

testresults = BulkFactTable(
        name='testresultsfact', 
        keyrefs=['pageid', 'testid', 'dateid'],
        measures=['errors'], 
        bulkloader=UDF_pgcopy,
        bulksize=5000000)

# Configure the dimensions and their fields
def UDF_extractdomaininfo(row, namemapping):
	# Take the 'www.domain.org' part from 'http://www.domain.org/page.html'
	# We also the host name ('www') in the domain in this example.
	domaininfo = row['url'].split('/')[-2]
	row['domain'] = domaininfo
	# Take the top level which is the last part of the domain
	row['topleveldomain'] = domaininfo.split('.')[-1]

def UDF_extractserverinfo(row, namemapping):
	# Find the server name from a string like "ServerName/Version"
	row['server'] = row['serverversion'].split('/')[0]

def UDF_datehandling(row, namemapping):
	date = pyetlmr.getvalue(row, 'date', namemapping)
	(year, month, day, hour, minute, second, weekday, dayinyear, dst) = time.strptime(date, "%Y-%m-%d")
	(isoyear, isoweek, isoweekday) = datetime.date(year, month, day).isocalendar()
	row['day'] = day
	row['month'] = month
	row['year'] = year
	row['week'] = isoweek
	row['weekyear'] = isoyear



dimensions = {
        pagesf : {'srcfields'  : ('url', 'serverversion', 'domain', 'size', 'lastmoddate'),
                   'rowhandlers': (UDF_extractdomaininfo, UDF_extractserverinfo,),
                   'namemappings' : {}
                   },
        datedim : {'srcfields': ('downloaddate',),
                   'rowhandlers' :(UDF_datehandling,),
                   'namemappings' : {'date':'downloaddate'}
                   },
        testdim : {'srcfields': (),
                   'rowhandlers': (),
                   'namemappings' :{'testname':'test'}
                   },
}

# ------------------------------------------------------------
def UDF_fact_testresults(row):
	row['testid'] = testdim.lookup(row, {'testname':'test'})
	row['pageid'] = pagedim.scdlookup(row)
	row['dateid'] = datedim.lookup(row, {'date':'downloaddate'})
	row['errors'] = getint(row['errors'])


facts = {
        testresults:{'srcfields':('test', 'url', 'downloaddate', 'lastmoddate', 'errors'),
                     'rowhandlers':(UDF_fact_testresults,)
                     }
}

if __name__== "__main__":
	print('Hello World!')
