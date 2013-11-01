import os
import sys
import struct

Nof = None
DBf = None
name = ""
N = 0
offset = 0

ENTRY_SIZE = struct.calcsize("!Q")


def create(file_name):
    global Nof
    global DBf
    global name
    global N
    global offset
    if Nof or DBf:
        print "Attempted to initialize %s when %s was still open!" % (file_name, name)
        sys.exit(-1)
    #if os.path.exists(file_name+".No") or os.path.exists(file_name+".DB"):
    #    print "Attempted to recreate %s!!!" % file_name
    #    sys.exit(-1)
    Nof = open(file_name+".No", 'wb')
    DBf = open(file_name+".DB", 'wb')
    N = 0
    offset = 0
    name = file_name
    return N


def resume(file_name):
    global Nof
    global DBf
    global name
    global N
    global offset
    if Nof or DBf:
        print "Attempted to initialize %s when %s was still open!" % (file_name, name)
        sys.exit(-1)
    if not os.path.exists(file_name+".No") or not os.path.exists(file_name+".DB"):
        print "Failed to resume %s!!!" % file_name
        sys.exit(-1)
    N = os.path.getsize(file_name+".No") / ENTRY_SIZE
    offset = os.path.getsize(file_name+".DB")
    Nof = open(file_name+".No", 'ab')
    DBf = open(file_name+".DB", 'ab')
    name = file_name
    return N


def close():
    global Nof
    global DBf
    global name
    global N
    global offset
    if not Nof or not DBf:
        print "Attempted to close non-existent NoDB store!"
        sys.exit(-1)
    else:
        Nof.close()
        Nof = None
        DBf.close()
        DBf = None
        name = ""
        offset = 0
        size = N
        N = 0
        return size


def store(byte_string):
    global Nof
    global DBf
    global name
    global N
    global offset

    if not Nof or not DBf:
        print "Attempted to use NoDB store without initialization!"
        sys.exit(-1)
    DBf.write(byte_string)
    N += 1
    offset += len(byte_string)
    Nof.write(struct.pack("!Q", offset))
    assert Nof.tell() == N*ENTRY_SIZE
    return N


def lookup(file_name, n):
    # These are private by design!
    N = os.path.getsize(file_name+".No") / ENTRY_SIZE
    if n > N:
        print "Attempted to lookup entry #%d from a DB of cardinality #%d!" % (n, N)
        sys.exit(-1)
    if n == 0:
        print "Keys to NoDB must be natural numbers, starting from 1!"
        sys.exit(-1)
    Nof = open(file_name + ".No", 'rb')
    start = 0
    if n > 1:
        Nof.seek(ENTRY_SIZE*(n-2))
        start = struct.unpack("!Q", Nof.read(ENTRY_SIZE))[0]
    stop = struct.unpack("!Q", Nof.read(ENTRY_SIZE))[0]
    Nof.close()
    DBf = open(file_name + ".DB", 'rb')
    DBf.seek(start)
    out = DBf.read(stop-start)
    DBf.close()
    return out
