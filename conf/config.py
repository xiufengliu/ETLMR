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
import os, time, datetime 
import psycopg2
import pyetlmr as etlmr
from pyetlmr import getint, getdate, datereader
from pyetlmr.odottables import CachedDimension, \
     SlowlyChangingDimension, BulkFactTable

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
        wrappedconn = etlmr.ConnectionWrapper(conn)
        wrappedconn.execute('set search_path to etlmr')
        wrappedconn.setasdefault()
        return wrappedconn

connection = UDF_createConnection()

def UDF_idfinder(name):
        connection.execute("SELECT nextval('%s')" % name)
        return connection.fetchonetuple()[0]

def UDF_datehandling(row, namemapping):
        date = etlmr.getvalue(row, 'date', namemapping)
        (year, month, day, hour, minute, second, weekday, dayinyear, dst) = time.strptime(date, "%Y-%m-%d")
        (isoyear, isoweek, isoweekday) = datetime.date(year, month, day).isocalendar()
        row['day'] = day
        row['month'] = month
        row['year'] = year
        row['week'] = isoweek
        row['weekyear'] = isoyear

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

def UDF_convertstrtoint(row):
        row['errors'] = etlmr.getint(row['errors'])


def UDF_pgcopy(name, atts, fieldsep, rowsep, nullval, filehandle):
	global connection
	try:
		curs = connection.cursor()
	except Exception:
		connection = UDF_createConnection()
		curs = connection.cursor()
	curs.copy_from(file=filehandle, table=name, \
	               sep=fieldsep, null=str(nullval), columns=atts)
	connection.commit()

#-- Declare dimensions and their settings -------------------
topleveldomaindim = CachedDimension(
        name='topleveldomaindim',
        key='topleveldomainid',
        attributes=['topleveldomain'],
        lookupatts=['topleveldomain'])

domaindim = CachedDimension(
        name='domaindim',
        key='domainid',
        attributes=['domain', 'topleveldomainid'],
        lookupatts=['domain']
)

serverdim = CachedDimension(
        name='serverdim',
        key='serverid',
        attributes=['server'],
        lookupatts=['server'])

serverversiondim = CachedDimension(
        name='serverversiondim',
        key='serverversionid',
        attributes=['serverversion', 'serverid'],
        lookupatts=['serverversion']
)

pagedim = SlowlyChangingDimension(
        name='pagedim',
        key='pageid',
        lookupatts=['url'],
        attributes=['url', 'size', 'validfrom', 'validto', 'version', 'domainid', 'serverversionid'],
        versionatt='version',
        fromatt='validfrom',
        toatt='validto',
        srcdateatt='lastmoddate'
)

testdim = CachedDimension(
        name='testdim',
        key='testid',
        defaultidvalue = -1,
        attributes=['testname'],
        lookupatts=['testname']
)

datedim = CachedDimension(
        name='datedim',
        key='dateid',
        attributes=['date','day','month','year','week','weekyear'],
        lookupatts=['date']
)

dimensions = { # Settings of dimensions
               pagedim: {'srcfields' : ('url', 'serverversion', 'domain', 'size', 'lastmoddate'),
                         'rowhandlers' : (UDF_extractdomaininfo, UDF_extractserverinfo),
                         'namemappings' : {}},
               topleveldomaindim: {'srcfields' : ('url',),
                                   'rowhandlers' : (UDF_extractdomaininfo,),
                                   'namemappings' : {}},
               domaindim: {'srcfields' : ('url',),
                           'rowhandlers' : (UDF_extractdomaininfo,),
                           'namemappings' : {}},
               serverdim: {'srcfields' : ('serverversion',),
                           'rowhandlers' : (UDF_extractserverinfo, ),
                           'namemappings' : {}},
               serverversiondim: {'srcfields' : ('serverversion',),
                                  'rowhandlers' : (UDF_extractserverinfo, ),
                                  'namemappings' : {}},
               datedim: {'srcfields' : ('downloaddate',),
                         'rowhandlers' : (UDF_datehandling,),
                         'namemappings' : {'date':'downloaddate'}},
               testdim: {'srcfields' : ('test',),
                         'rowhandlers' : (),
                         'namemappings':{'testname':'test'}}
}

# Define the reference-ship of snowflaked dimension tables
references = [(pagedim, (serverversiondim, domaindim)),
              (serverversiondim, serverdim),
              (domaindim, topleveldomaindim)
              ]


# The loading order of  snowflake dimensions
order =  [(topleveldomaindim, serverdim), (domaindim, serverversiondim), (pagedim, testdim, datedim)]


# --- Define facts and their settings ---------
testresultsfact = BulkFactTable(
        name='testresultsfact',
        keyrefs=['pageid', 'testid', 'dateid'],
        measures=['errors'],
        bulkloader=UDF_pgcopy,
        bulksize=500000)

facts = { # Settings of facts
          testresultsfact: {
                  'refdims' : (testdim, pagedim, datedim),
                  'namemappings' : {'testname':'test', 'date':'downloaddate'},
                  'rowhandlers' : (UDF_convertstrtoint,),
                  },
}

#factdict = {'testresultsfact':testresultsfact}