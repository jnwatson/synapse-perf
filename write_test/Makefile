
# This should point to a C LMDB distribution.  Conveniently, there's one in py-lmdb.
LMDB_DIR=~/proj/py-lmdb/lib

CFILES=lmdb_write_test.c $(LMDB_DIR)/mdb.c $(LMDB_DIR)/midl.c
CPPFLAGS=-O3 -std=c11 -Wall -I $(LMDB_DIR)
LDLIBS=-lpthread

c_lmdb_write_test_exe: $(CFILES)
	gcc $(CPPFLAGS) $(CFILES) $(LDLIBS) -o $@

.PHONY : clean
clean:
	rm -f c_lmdb_write_test_exe
