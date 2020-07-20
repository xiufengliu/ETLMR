"""
   The package contains a number of classes for filling fact tables
   and dimensions (including snowflaked and slowly changing dimensions), 
   classes for extracting data from different sources, classes for defining
   'steps' in an ETL flow, and convenient functions for often-needed ETL
   functionality.

   The package's modules are:
   - datasources for access to different data sources
   - steps for defining steps in an ETL flow
   - tables for giving easy and abstracted access to dimension and fact tables
   - FIFODict for providing a dict with a limited size and where elements are 
     removed in first-in first-out order
"""
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
import copy as pcopy
import types
from datetime import date, datetime
from Queue import Queue
from sys import modules
from threading import Thread

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1.0'

__all__ = ['project', 'copy', 'rename', 'getint', 'getlong', 'getfloat', 
           'getstr', 'getstrippedstr', 'getstrornullvalue', 'getbool', 
           'getdate', 'gettimestamp', 'getvalue', 'getvalueor', 'setdefaults', 
           'rowfactory', 'endload', 'today', 'now', 'ymdparser', 'ymdhmsparser',
           'datereader', 'datetimereader', 'toupper', 'tolower', 'keepasis', 
           'ConnectionWrapper', 'BackgroundConnectionWrapper']


_alltables = []

def project(atts, row, renaming={}):
    """Create a new dictionary with a subset of the attributes.

       Arguments:
       - atts is a sequence of attributes in row that should be copied to the
         new result row.
       - row is the original dictionary to copy data from.
       - renaming is a mapping of names such that for each k in atts, 
         the following holds:
         - If k in renaming then result[k] = row[renaming[k]]. 
         - If k not in renaming then result[k] = row[k].
         renaming defauts to {}
         
    """
    res = {}
    for c in atts:
        if c in renaming:
            res[c] = row[renaming[c]]
        else:
            res[c] = row[c]
    return res


def copy(row, **renaming):
    """Create a copy of a dictionary, but allow renamings.

       Arguments:
       - row the dictionary to copy
       - **renaming allows renamings to be specified in the form
         newname=oldname meaning that in the result oldname will be
         renamed to newname.
    """
    if not renaming:
        return row.copy()

    tmp = row.copy()
    res = {}
    for k,v in renaming.items():
        res[k] = row[v]
        del tmp[v]
    res.update(tmp)
    return res


def rename(row, renaming):
    """Rename keys in a dictionary.

       For each (k,v) in renaming.items(): rename row[k] to row[v].
    """
    if not renaming:
        return row

    for k,v in renaming.items():
        row[v] = row[k]
        del row[k]
    

    
def getint(value, default=None):
    """getint(value[, default]) -> int(value) if possible, else default."""
    try:
        return int(value)
    except Exception:
        return default


def getlong(value, default=None):
    """getlong(value[, default]) -> long(value) if possible, else default."""
    try:
        return long(value)
    except Exception:
        return default

def getfloat(value, default=None):
    """getfloat(value[, default]) -> float(value) if possible, else default."""
    try:
        return float(value)
    except Exception:
        return default

def getstr(value, default=None):
    """getstr(value[, default]) -> str(value) if possible, else default."""
    try:
        return str(value)
    except Exception:
        return default

def getstrippedstr(value, default=None):
    """Convert given value to a string and use .strip() on the result.

       If the conversion fails, the given default value is returned.
    """
    try:
        s = str(value)
        return s.strip()
    except Exception:
        return default

def getstrornullvalue(value, nullvalue='None'):
    """Convert a given value different from None to a string.

       If the given value is None, nullvalue (default: 'None') is returned.
    """
    if value is None:
        return nullvalue
    else:
        return str(value)

def getbool(value, default=None, 
            truevalues=set((True, 1, '1', 't', 'true')), 
            falsevalues=set((False, 0, '0', 'f', 'false'))):
    """Convert a given value to True, False, or a default value.

       If the given value is in the given truevalues, True is retuned.
       If the given value is in the given falsevalues, False is returned.
       Otherwise, the default value is returned.
    """
    if value in truevalues:
        return True
    elif value in falsevalues:
        return False
    else:
        return default



def getdate(targetconnection, ymdstr, default=None):
    """Convert a string of the form 'yyyy-MM-dd' to a Date object.

       The returned Date is in the given targetconnection's format.
       Arguments:
       - targetconnection: a ConnectionWrapper whose underlying module's
         Date format is used
       - ymdstr: the string to convert
       - default: The value to return if the conversion fails
    """
    try:
        (year, month, day) = ymdstr.split('-')
        modref = targetconnection.getunderlyingmodule()
        return modref.Date(int(year), int(month), int(day))
    except Exception:
        return default

def gettimestamp(targetconnection, ymdhmsstr, default=None):
    """Converts a string of the form 'yyyy-MM-dd HH:mm:ss' to a Timestamp.
    
    The returned Timestamp is in the given targetconnection's format.
       Arguments:
       - targetconnection: a ConnectionWrapper whose underlying module's
         Timestamp format is used
       - ymdhmsstr: the string to convert
       - default: The value to return if the conversion fails
    """
    try:
        (datepart, timepart) = ymdhmsstr.strip().split(' ')
        (year, month, day) = datepart.split('-')
        (hour, minute, second) = timepart.split(':')
        modref = targetconnection.getunderlyingmodule()
        return modref.Timestamp(int(year), int(month), int(day),\
                                int(hour), int(minute), int(second))
    except Exception:
        return default

def getvalue(row, name, mapping={}):
    """If name in mapping, return row[mapping[name]], else return row[name]."""
    if name in mapping:
        return row[mapping[name]]
    else:
        return row[name]

def getvalueor(row, name, mapping={}, default=None):
    """Return the value of name from row using a mapping and a default value."""
    if name in mapping:
        return row.get(mapping[name], default)
    else:
        return row.get(name, default)

def setdefaults(row, attributes, defaults=None):
    """Set default values for attributes not present in a dictionary.

       Default values are set for "missing" values, existing values are not 
       updated. 

       Arguments:
       - row is the dictionary to set default values in
       - attributes is either
           A) a sequence of attribute names in which case defaults must
              be an equally long sequence of these attributes default values or
           B) a sequence of pairs of the form (attribute, defaultvalue) in 
              which case the defaults argument should be None
       - defaults is a sequence of default values (see above)
    """
    if defaults and len(defaults) != len(attributes):
        raise ValueError, "Lists differ in length"

    if defaults:
        seqlist = zip(attributes, defaults)
    else:
        seqlist = attributes

    for att, defval in seqlist:
        if att not in row:
            row[att] = defval


def rowfactory(source, names, close=True):
    """Generate dicts with key values from names and data values from source.
    
       The given source should provide either next() or fetchone() returning
       a tuple or fetchall() returning a sequence of tuples. For each tuple,
       a dict is constructed such that the i'th element in names maps to 
       the i'th value in the tuple.

       If close=True (the default), close will be called on source after
       fetching all tuples.
    """
    nextfunc = getattr(source, 'next', None)
    if nextfunc is None:
        nextfunc = getattr(source, 'fetchone', None)

    try:
        if nextfunc is not None:
            try:
                tmp = nextfunc()
                if tmp is None:
                    return
                else:
                    yield dict(zip(names, tmp))
            except StopIteration, IndexError:
                return
        else:
            for row in source.fetchall():
                yield dict(zip(names, row))
    finally:
        if close:
            try:
                source.close()
            except:
                return

def endload():
    """Signal to all Dimension and FactTable objects that all data is loaded."""
    global _alltables
    for t in _alltables:
        method = getattr(t, 'endload', None)
        if callable(method):
            method()

_today = None
def today(targetconnection, ignoredrow=None, ignorednamemapping=None):
    """Return the date of the first call this method as a Date object.

       The Date object is in a format suitable for the given targetconnection.
    """
    global _today
    if _today is not None:
        return _today
    to = datetime.now()
    modref = targetconnection.getunderlyingmodule()
    _today = modref.Date(to.year, to.month, to.day)
    return _today

_now = None
def now(targetconnection, ignoredrow=None, ignorednamemapping=None):
    """Return the tine of the first call this method as a Timestamp object.

       The Timestamp object is in a format suitable for the given 
       targetconnection.
    """
    global _now
    if _now is not None:
        return _now
    to = datetime.now()
    modref = targetconnection.getunderlyingmodule()
    _now = modref.Timestamp(to.year, to.month, to.day, to.hour, to.minute, 
                            to.second)
    return _now

def ymdparser(ymdstr):
    """Convert a string of the form 'yyyy-MM-dd' to a datetime.date. 

       If the input is None, the return value is also None.
    """
    if ymdstr is None:
        return None
    (year, month, day) = ymdstr.split('-')
    return date(int(year), int(month), int(day))

def ymdhmsparser(ymdhmsstr):
    """Convert a string 'yyyy-MM-dd HH:mm:ss' to a datetime.datetime. 

       If the input is None, the return value is also None.
    """
    if ymdstr is None:
        return None
    (datepart, timepart) = ymdhmsstr.strip().split(' ')
    (year, month, day) = datepart.split('-')
    (hour, minute, second) = timepart.split(':')
    return datetime(int(year), int(month), int(day),\
                    int(hour), int(minute), int(second))


def datereader(dateattribute, parsingfunction=ymdparser):
    """Return a function that converts a certain dictionary member to a Date.

       When setting, fromfinder for a tables.SlowlyChangingDimension, this
       method can be used for generating a function that picks the relevant
       dictionary member from each row and converts it.

       Arguments:
       - dateattribute: the attribute the generated function should read
       - parsingfunction: the parsing function that converts the string
         to a datetime.date
    """
    def readerfunction(targetconnection, row, ignorednamemapping = None):
        thedate = parsingfunction(row[dateattribute]) # a datetime.date
        modref = targetconnection.getunderlyingmodule()
        return modref.Date(thedate.year, thedate.month, thedate.day)
    
    return readerfunction
    

def datetimereader(datetimeattribute, parsingfunction=ymdhmsparser):
    """Return a function that converts a certain dictionary member to a Timestamp.

       When setting, fromfinder for a tables.SlowlyChangingDimension, this
       method can be used for generating a function that picks the relevant
       dictionary member from each row and converts it.

       Arguments:
       - datetimeattribute: the attribute the generated function should read
       - parsingfunction: the parsing function that converts the string
         to a datetime.datetime
    """
    def readerfunction(targetconnection, row, ignoredmapping = None):
        thedt = parsingfunction(row[datetimeattribute]) # a datetime.datetime
        modref = targetconnection.getunderlyingmodule()
        return modref.Timestamp(thedt.year, thedt.month, thedt.day, 
                                thedt.hour, thedt.minute, thedt.second)

    return readerfunction


toupper  = lambda s: s.upper()
tolower  = lambda s: s.lower()
keepasis = lambda s: s

_defaulttargetconnection = None

def getdefaulttargetconnection():
    """Return the default target connection"""
    global _defaulttargetconnection
    return _defaulttargetconnection

class ConnectionWrapper(object):
    """Provide a uniform representation of different database connection types.

       All Dimension and FactTable communicate with the data warehouse using 
       a ConnectionWrapper. In this way, the code for loading the DW does not
       have to care about which parameter format is used.  
       
       A ConnectionWrapper must be implemented for different kinds of drivers.
       The base implementation provided here, assumes 'pyformat' 
    """

    def __init__(self, connection):
        """Create a ConnectionWrapper around the given PEP 249 connection """
        self.__connection = connection
        self.__cursor = connection.cursor()
        self.__close = False
        self.nametranslator = lambda s: s

    def execute(self, stmt, arguments=None, namemapping=None):
        """Execute a statement.

           Arguments:
           - stmt: the statement to execute
           - arguments: a mapping with the arguments (default: None)
           - namemapping: a mapping of names such that if stmt uses %(arg)s
             and namemapping[arg]=arg2, the value arguments[arg2] is used 
             instead of arguments[arg]
        """
        if namemapping and arguments:
            arguments = copy(arguments, **namemapping)
        self.__cursor.execute(stmt, arguments)

    def executemany(self, stmt, params):
        """Execute a sequence of statements."""
        self.__cursor.executemany(stmt, params)

    def rowfactory(self, names=None):
        """Return a generator object returning result rows (i.e. dicts)."""
        rows = self.__cursor
        self.__cursor = self.__connection.cursor()
        if names is None:
            names = [self.nametranslator(t[0]) for t in rows.description]
        return rowfactory(rows, names, True)

    def fetchone(self, names=None):
        """Return one result row (i.e. dict)."""
        if self.__cursor.rowcount == -1:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) for t in cursor.description]
        values = self.__cursor.fetchone()
        if values is None:
            return dict([(n, None) for n in names])#A row with each att = None
        else:
            return dict(zip(names, values))

    def fetchonetuple(self):
        """Return one result tuple."""
        if self.__cursor.rowcount == -1:
            return ()
        values = self.__cursor.fetchone()
        if values is None:
            return (None, ) * len(self.__cursor.description)
        else:
            return values

    def fetchmanytuples(self, cnt):
        """Return cnt result tuples."""
        if self.__cursor.rowcount == -1:
            return []
        return self.__cursor.fetchmany(cnt)

    def fetchalltuples(self):
        """Return all result tuples"""
        if self.__cursor.rowcount == -1:
            return []
        return self.__cursor.fetchall()

    def rowcount(self):
        """Return the size of the result."""
        return self.__cursor.rowcount

    def getunderlyingmodule(self):
        """Return a reference to the underlying connection's module."""
        return modules[self.__connection.__class__.__module__]

    def commit(self):
        """Commit the transaction."""
        #endload()
        self.__connection.commit()
    
    #def commit_only(self):
    #    self.__connection.commit()

    def isclose(self):
        return self.__close
    
    def close(self):
        """Close the connection to the database,"""
        if not self.__close:
            self.__connection.commit()
            self.__cursor.close()
            self.__connection.close()
            self.__close = True
        

    def rollback(self):
        """Rollback the transaction."""
        self.__connection.rollback()

    def setasdefault(self):
        """Set this ConnectionWrapper as the default connection."""
        global _defaulttargetconnection
        _defaulttargetconnection = self

    def cursor(self):
        """Return a cursor object. Optional method."""
        return self.__connection.cursor()

    def __del__(self):
        self.close()
        _defaulttargetconnection = None

class BackgroundConnectionWrapper(object):
    """An alternative implementation of the ConnectionWrapper for experiments.
       This implementation communicates with the database by using a
       separate thread.

       This class offers the same methods as ConnectionWrapper. The 
       documentation is not repeated here.
    """
    _SINGLE = 1
    _MANY = 2
    def __init__(self, connection):
        self.__connection = connection
        self.__cursor = connection.cursor()
        self.__queue = Queue(5000)
        t = Thread(target=self.__worker)
        t.daemon = True
        t.start()
        self.nametranslator = lambda s: s

    def execute(self, stmt, arguments=None, namemapping=None):
        if namemapping and arguments:
            arguments = copy(arguments, **namemapping)
        elif arguments is not None:
            arguments = pcopy.copy(arguments)
        self.__queue.put((self._SINGLE, self.__cursor, stmt, arguments)) 
        

    def executemany(self, stmt, params):
        params = pcopy.copy(params)
        self.__queue.put((self._MANY, self.__cursor, stmt, params))

    def rowfactory(self, names=None):
        self.__queue.join()
        rows = self.__cursor
        self.__cursor = self.__connection.cursor()
        if names is None:
            names = [self.nametranslator(t[0]) for t in rows.description]
        return rowfactory(rows, names, True)

    def fetchone(self, names=None):
        self.__queue.join()
        if self.__cursor.rowcount == -1:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) for t in cursor.description]
        values = self.__cursor.fetchone()
        if values is None:
            return dict([(n, None) for n in names])#A row with each att = None
        else:
            return dict(zip(names, values))

    def fetchonetuple(self):
        self.__queue.join()
        if self.__cursor.rowcount == -1:
            return ()
        values = self.__cursor.fetchone()
        if values is None:
            return (None, ) * len(self.__cursor.description)
        else:
            return values

    def fetchmanytuples(self, cnt):
        self.__queue.join()
        if self.__cursor.rowcount == -1:
            return []
        return self.__cursor.fetchmany(cnt)

    def fetchalltuples(self):
        self.__queue.join()
        if self.__cursor.rowcount == -1:
            return []
        return self.__cursor.fetchall()

    def rowcount(self):
        self.__queue.join()
        return self.__cursor.rowcount

    def getunderlyingmodule(self):
        # No need to join the queue here
        return modules[self.__connection.__class__.__module__]

    def commit(self):
        endload()
        self.__queue.join()
        self.__connection.commit()

    def close(self):
        self.__queue.join()
        self.__connection.close()

    def rollback(self):
        self.__queue.join()
        self.__connection.rollback()

    def setasdefault(self):
        global _defaulttargetconnection
        _defaulttargetconnection = self

    def cursor(self):
        self.__queue.join()
        return self.__connection.cursor()

    def __worker(self):
        while True:
            (op, curs, stmt, args) = self.__queue.get()
            if op == self._SINGLE:
                curs.execute(stmt, args)
            elif op == self._MANY:
                curs.executemany(stmt, args)
            self.__queue.task_done()
