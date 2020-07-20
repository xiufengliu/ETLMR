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

class _dlnode(object):
    def __init__(self):
        self.key = None


class lrucache(object):

    def __init__(self, size, callback=None):

        self.callback = callback
        # Initialize the hash table as empty.
        self.table = {}



        self.head = _dlnode()
        self.head.next = self.head
        self.head.prev = self.head

        self.listSize = 1

        # Adjust the size
        self.size(size)


    def __len__(self):
        return len(self.table)

    # Does not call callback to write any changes!
    def clear(self):
        self.table.clear()

        node = self.head
        for i in range(self.listSize):
            node.key = None
            node.obj = None
            node = node.next

    def __iter__(self):
        node = self.head
        for i in range(self.listSize):
            key, value = node.key, node.obj
            node = node.next
            yield key, value


    def __contains__(self, key):
        return key in self.table


    def peek(self, key):
        # Look up the node
        node = self.table[key]
        return node.obj

    def __getitem__(self, key):

        # Look up the node
        node = self.table[key]

        self.mtf(node)
        self.head = node

        # Return the object
        return node.obj


    def __setitem__(self, key, obj):
        if key in self.table:
            node = self.table[key]
            node.obj = obj
            self.mtf(node)
            self.head = node

            return

        node = self.head.prev

        if node.key is not None:
            if self.callback:
                self.callback(node.key, node.obj)
            del self.table[node.key]

        node.key = key
        node.obj = obj

        self.table[node.key] = node 

        self.head = node


    def __delitem__(self, key):
        node = self.table[key]
        del self.table[key]

        node.key = None
        node.obj = None

        self.mtf(node)
        self.head = node.next



    def size(self, size=None):
        if size is not None:
            assert size > 0
            if size > self.listSize:
                self.addTailNode(size - self.listSize)
            elif size < self.listSize:
                self.removeTailNode(self.listSize - size)

        return self.listSize


    def addTailNode(self, n):
        for i in range(n):
            node = _dlnode()
            node.next = self.head
            node.prev = self.head.prev

            self.head.prev.next = node
            self.head.prev = node

        self.listSize += n


    def removeTailNode(self, n):
        assert self.listSize > 1   
        for i in range(n):
            node = self.head.prev
            if node.key is not None:
                if self.callback:
                    self.callback(node.key, node.obj)
                del self.table[node.key]

            # Splice the tail node out of the list
            self.head.prev = node.prev
            node.prev.next = self.head

            # The next four lines are not strictly necessary.
            node.prev = None
            node.next = None

            node.key = None
            node.obj = None

        self.listSize -= n

    def __del__(self):

        tail = self.head.prev

        node = self.head
        while node.prev is not None:
            node = node.prev
            node.next.prev = None

        tail.next = None
        
    def mtf(self, node):

        node.prev.next = node.next
        node.next.prev = node.prev

        node.prev = self.head.prev
        node.next = self.head.prev.next

        node.next.prev = node
        node.prev.next = node
        
SEQ = 'seq'
class LRUWrap(object):
    def __init__(self, store, size, readonly=False):
        self.store = store
        self.dirty = set()
        self.readonly = readonly
        def callback(key, value):
            if key in self.dirty:
                self.store[key] = value
                self.dirty.remove(key)
        self.cache = lrucache(size, None if readonly else callback)

    def __len__(self):
        return len(self.store)


    def __contains__(self, key):
        if key in self.cache:
            return True
        if key in self.store:
            return True

        return False

    def __getitem__(self, key):
        try:
            return self.cache[key]
        except KeyError:
            pass
        value = self.store[key] 
        self.cache[key] = value
        return value
    

    def __setitem__(self, key, value):
        self.cache[key] = value
        if not self.readonly:
            self.dirty.add(key)


    def __delitem__(self, key):
        if not self.readonly:
            found = False
            try:
                del self.cache[key]
                found = True
                self.dirty.remove(key)
            except KeyError:
                pass
    
            try:
                del self.store[key]
                found = True
            except KeyError:
                pass
    
            if not found:  # If not found in cache or store, raise error.
                raise KeyError


    def incr(self):
        if not self.readonly:
            try:
                seq = self.cache[SEQ]
                self.cache[SEQ] = seq + 1
                return seq
            except KeyError:
                pass
    
            try:
                seq = self.store[SEQ]
                self.cache[SEQ] = seq + 1
                self.dirty.add(SEQ)
                return seq
            except KeyError:
                seq = 1
                self.cache[SEQ] = seq + 1
                self.dirty.add(SEQ)
                return seq
        else:
            raise Exception('Readonly cannot generate sequence!')


    def sync(self):
        if not self.readonly:
            for key in self.dirty:
                value = self.cache.peek(key)  # Doesn't change the cache's order
                self.store[key] = value
            self.dirty.clear()


class lrudecorator(object):
    def __init__(self, size):
        self.cache = lrucache(size)

    def __call__(self, func):
        def wrapped(*args):  
            try:
                return self.cache[args]
            except KeyError:
                pass

            value = func(*args)
            self.cache[args] = value
            return value
        return wrapped


class _SerializedShelve:

    def __init__(self, filepath, flag):
        self.db = shelve.open(filepath, flag)

    def __setitem__(self, key, value):
        self.db[str(key)] = value

    def get(self, key, default=None):
        return self.db[str(key)] 
    
    def __len__(self):
        return len(self.db)

    def __getitem__(self, key):
        return self.get(key, None)

    def iteritems(self):
        for k, v in self.db.items():
            if k != SEQ:
                yield (k, v)        
        
            
    def iterkeys(self):
        for k in self.db.iterkeys():
            if k != SEQ:
                yield k
    
    def iterms(self):
        return list(self.iteritems())
    
    def close(self):
        self.db.close()


class LRUShelve:

    def __init__(self, filepath, cachesize, temp=False, readonly=False):
        self.slowDict = _SerializedShelve(filepath, 'r' if readonly else 'c')
        self.cacheDict = LRUWrap(self.slowDict, cachesize, readonly)

    def get(self, key, default=None):
        try:
            return self.cacheDict[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        self.cacheDict[key] = value

    def __getitem__(self, key):
        return self.cacheDict[key]

    def __len__(self): # The len of the shelve store, not include the len of cache.
        return len(self.cacheDict)

    def get_nextid(self):
        return self.cacheDict.incr()

    def iteritems(self):
        self.cacheDict.sync()
        return self.slowDict.iteritems()
            
    def iterkeys(self):
        self.cacheDict.sync()
        return self.slowDict.iterkeys()
    
    def iterms(self):
        self.cacheDict.sync()
        return self.slowDict.iterms()
    
    
    def __del__(self):
        self.close()
        
    def close(self):
        self.cacheDict.sync()
        self.slowDict.close()        

if __name__== "__main__":
    db = LRUShelve('/home/demouser/disco/root/input/testdim', 2000, False, False)
    #db = LRUShelve('/tmp/pagedim_test', 2000, False, False)
    #db[('a', 'a')] = {'a':1, 'a':2}
    #db[('a', 'b')] = {'a':1, 'b':2}
    #db[('a', 'c')] = [{'a':1, 'c':2},{'d':4, 'c':6}]
    #db[('a', 'd')] = {'a':1, 'd':2}
  
    for k, v in   db.iterms():
        print k, v

    

