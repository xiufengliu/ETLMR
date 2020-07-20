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
import types, time
from conf import config

__author__ = "Xiufeng Liu"
__maintainer__ = "Xiufeng Liu"
__version__ = '0.1'


class Table(object):
    """A class for accessing a dimension. Does no caching."""

    def __init__(self, tablename, pkey, duplicateattrs=(), reftables=(), root=False, targetconnection=None):
        self.tablename = tablename
        self.targetconnection = targetconnection
        self.duplicateattrs = duplicateattrs
        self.pkey = pkey
        self.reftables = reftables
        self.root = root
        
    def get_pkey(self):
        return self.pkey
        
    
    def set_root(self, root=True):
        self.root = root
        
    def del_duplicate_rows(self):
        sql = 'delete from %s where %s not in (select max(%s) from %s group by %s)' % \
        (self.tablename, self.pkey, self.pkey, self.tablename, ','.join(self.duplicateattrs))
        self.targetconnection.execute(sql)
  
    def add_primarykey(self):
        sql = 'alter table %s add primary key(%s)' % (self.tablename, self.pkey)
        self.targetconnection.execute(sql)

    def get_fixedidlist(self):
        '''
        return the value as [[1, 3], [2, 4]] where the duplicate records are shown
        '''
        if not self.root:
            sql = 'select %s, %s from %s order by %s' %  \
            (self.pkey, ','.join(self.duplicateattrs), self.tablename,  ','.join(self.duplicateattrs)) 
            self.targetconnection.execute(sql)
            duplicatevalues = None
            dup_ids = {}
            while True:
                row = self.targetconnection.fetchonetuple()
                if not row[0]:
                    break
                idlist = dup_ids.get(row[1:], [])
                idlist.append(row[0])
                dup_ids[row[1:]] = idlist
                
            fixedidlist = []
            for idlist in dup_ids.values():
                if len(idlist)>1:
                    idlist.sort()
                    fixedidlist.append(idlist)
            return fixedidlist
                               
        
    def update_foreignref(self, fkey, fixedidlist):
        for idlist in fixedidlist:
            sql = 'update %s set %s=%d where %s in (%s)' % \
            (self.tablename, fkey, idlist[-1], fkey, ','.join(map(str,idlist[:-1])))
            self.targetconnection.execute(sql)
            
            
    def fix(self):
        fkey_fixidlist = {}
        for reftab in self.reftables:
            fixedidlist = reftab.fix()
            fkey_fixidlist[reftab.get_pkey()] = fixedidlist     
           
        for fkey, fixedidlist in fkey_fixidlist.iteritems():
            self.update_foreignref(fkey, fixedidlist)
            
        fidlist = self.get_fixedidlist()  
        if not self.root: # No duplicate rows to be deleted in the root dimension
	    self.del_duplicate_rows()
            #self.add_primarykey()
        self.targetconnection.commit()
        return fidlist


def __build_snowflake(snflkdim, startdim, connection):
    found = False
    for level in snflkdim:
        if level[0]==startdim:
            found = True
            refdims = level[1]
            if type(refdims) != types.TupleType:
                refdims = (refdims,)
            reftables = []
            duplicateattrs = startdim.lookupatts
            for refdim in refdims:
                table = __build_snowflake(snflkdim, refdim, connection)
                if table.pkey not in duplicateattrs:
                    duplicateattrs.append(table.pkey)
                reftables.append(table)
            return Table(startdim.name, startdim.key, duplicateattrs=tuple(duplicateattrs), reftables=tuple(reftables), targetconnection=connection)
            
    if not found: # leaf
        return Table(startdim.name, startdim.key, duplicateattrs=tuple(startdim.lookupatts), targetconnection=connection)
   
   
def post_fix_test(config):
    try:
	connection = config.connection
	postfix_starttime = time.time()
	dimensions = [config.datedim]#config.dimensions.keys()
	for dimension in dimensions:
	    snflkdim = dimension.get_referencedims()
	    rootdim = __build_snowflake(snflkdim, snflkdim[0][0], connection)
	    rootdim.set_root()
	    rootdim.fix()
	postfix_endtime = time.time()
	print "Time of post-fixing: %f" % (postfix_endtime-postfix_starttime)
    except Exception as ex:
	print ex
	return    
                        
def post_fix(config):
    try:
        snflkdim, connection = config.references, config.connection
    except Exception as ex:
        return
    if snflkdim:
        postfix_starttime = time.time()
        rootdim = __build_snowflake(snflkdim, snflkdim[0][0], connection)
        rootdim.set_root()
        rootdim.fix()
	postfix_endtime = time.time()
	print "Post-fixing time: %f" % (postfix_endtime-postfix_starttime)        

    
def test_fix_dim():    
    from config import connection            
            
    topleveldomain = Table('topleveldomain', 'topleveldomainid', duplicateattrs=('topleveldomain',), targetconnection=connection)   
    domain = Table('domain', 'domainid', duplicateattrs=('domain','topleveldomainid',), reftables=(topleveldomain,), targetconnection=connection) 
    server = Table('server', 'serverid', duplicateattrs=('server',), targetconnection=connection)             
    serverversion = Table('serverversion', 'serverversionid', duplicateattrs=('serverversion', 'serverid',), reftables=(server,), targetconnection=connection) 
    page = Table('page', 'pageid', duplicateattrs=('url', 'size', 'validfrom', 'domainid','serverversionid'), reftables=(serverversion,domain), root=True, targetconnection=connection)     
    page.fix()
    
if __name__== "__main__":
    post_fix(config)
    