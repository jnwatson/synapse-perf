import time
import os
import struct
import sqlite3
from binascii import hexlify
from typing import Union
import base64

import psutil  # type: ignore
import lmdb  # type: ignore
from numpy import random  # type: ignore
import psycopg2  # type: ignore

import synapse.cortex as s_cortex  # type: ignore

_SIZET_ST = struct.Struct('@Q')


class SqliteWriter:
    def __init__(self, filename: str, already_exists: bool, use_sqlite_wal: bool) -> None:
        self.conn = sqlite3.connect(filename)
        if not already_exists:
            self.conn.execute('CREATE TABLE t(key INTEGER PRIMARY KEY ASC, val BLOB);')
        self.conn.commit()
        if use_sqlite_wal:
            self.conn.execute('PRAGMA journal_mode=WAL;')
            self.label = 'sqlite_wal'
        else:
            self.label = 'sqlite'

    def write(self, data: bytes, batch_size: int) -> None:
            self.conn.executemany('INSERT INTO t VALUES (?, ?)',
                                  ((_SIZET_ST.unpack(random.bytes(7)+b'\0')[0], data) for j in range(batch_size)))
            self.conn.commit()

    def close(self):
        self.conn.close()


class PostgresqlWriter(SqliteWriter):
    def __init__(self, delete_first: bool) -> None:
        self.conn = psycopg2.connect("host='localhost' dbname='db' user='synapse' password='synapse'")
        self.curs = self.conn.cursor()
        if delete_first:
            self.curs.execute('DROP TABLE IF EXISTS t;')
            self.curs.execute('DROP TABLE IF EXISTS synapsetable;')
            self.curs.execute('DROP TABLE IF EXISTS synapsetable_blob;')
        self.curs.execute('CREATE TABLE t(key BIGINT PRIMARY KEY, val BYTEA);')
        self.conn.commit()
        self.label = 'postgres'

    def write(self, data: bytes, batch_size: int) -> None:
            self.curs.executemany('INSERT INTO t VALUES (%s, %s)',
                                  ((_SIZET_ST.unpack(random.bytes(7)+b'\0')[0], data) for j in range(batch_size)))
            self.conn.commit()

    def close(self):
        self.curs.close()
        self.conn.close()


class LmdbWriter:
    def __init__(self, filename: str) -> None:
        MAP_SIZE = 20 * 1024 * 1024 * 1024
        self.env = lmdb.Environment(filename, map_size=MAP_SIZE, subdir=False, metasync=False, sync=True,
                                    readahead=False, max_dbs=4, writemap=True, meminit=False, lock=True)
        self.db = self.env.open_db(key=b'data', integerkey=True)
        self.label = 'lmdb'

    def write(self, data: bytes, batch_size: int) -> None:
        with self.env.begin(write=True, buffers=True, db=self.db) as txn:
            for j in range(batch_size):
                # rv = txn.put(key_enc, data, dupdata=False, overwrite=False, db=db)
                rv = txn.put(random.bytes(8), data, dupdata=False, overwrite=False, db=self.db)
                assert rv

    def close(self) -> None:
        self.env.close()


class SynapseWriter:
    def __init__(self, filename: str, *, use_sqlite: bool, use_postgres: bool, delete_first: bool,
                 use_sqlite_wal=False) -> None:
        print(self)
        if use_sqlite or use_sqlite_wal:
            url = 'sqlite:///' + filename
            if use_sqlite_wal:
                self.label = 'syn_sqlite_wal'
            else:
                self.label = 'syn_sqlite'
        elif use_postgres:
            url = 'postgres://synapse:synapse@localhost/db/synapsetable'
            self.label = 'syn_postgres'
            if delete_first:
                self._drop_synapse_table()
        else:
            url = 'lmdb:///' + filename
            self.label = 'syn_lmdb'
        self.core = s_cortex.openurl(url)
        if use_sqlite_wal:
            db = self.core.store.dbpool.get()
            db.execute('PRAGMA journal_mode=WAL;')
            self.core.store.dbpool.put(db)

    def _drop_synapse_table(self):
        conn = psycopg2.connect("host='localhost' dbname='db' user='synapse' password='synapse'")
        curs = conn.cursor()
        curs.execute('DROP TABLE IF EXISTS synapsetable;')
        curs.execute('DROP TABLE IF EXISTS synapsetable_blob;')
        curs.execute('DROP TABLE IF EXISTS t;')
        conn.commit()
        curs.close()
        conn.close()

    def write(self, data: bytes, batch_size: int) -> None:
        rows = []
        val = base64.a85encode(data).decode('ascii')
        prop = 'propname'
        for i in range(batch_size):
            iden = hexlify(random.bytes(16)).decode('utf8')
            timestamp = random.randint(1, 2 ** 63)
            rows.append((iden, prop, val, timestamp))
        self.core.addRows(rows)

    def close(self) -> None:
        pass


def make_db(*, size_in_mb: int, delete_first: bool, filename: str, use_sqlite=False, use_postgresql=False,
            use_synapse=False, use_sqlite_wal=False):
    me = psutil.Process()
    if (use_sqlite and use_postgresql) or (use_sqlite_wal and use_postgresql):
        raise Exception('Invalid parameters.')
    if delete_first and not use_postgresql:
        try:
            os.unlink(filename)
        except OSError:
            pass
        already_exists = os.path.exists(filename)
    else:
        already_exists = False
    if use_synapse:
        writer: Union[SynapseWriter, SqliteWriter, LmdbWriter] = \
                SynapseWriter(filename, use_sqlite=use_sqlite, use_sqlite_wal=use_sqlite_wal, use_postgres=use_postgresql, 
                              delete_first=delete_first)
    elif use_sqlite:
        writer = SqliteWriter(filename, already_exists, use_sqlite_wal=use_sqlite_wal)
    elif use_postgresql:
        writer = PostgresqlWriter(delete_first)
    else:
        writer = LmdbWriter(filename)

    print("Starting write test for %s" % writer.label)
    if already_exists:
        print("Using existing DB")
    total_size = size_in_mb * 1024 * 1024

    DATA_SIZE = 1024
    BATCH_SIZE = 128
    data = b'\xa5' * DATA_SIZE
    key = last_key = first_key = random.randint(0, 2**63-1)
    last_now = start_time = time.time()
    for i in range(total_size // DATA_SIZE // BATCH_SIZE):
        if i % 512 == 0:
            print('uss=%dMiB' % (me.memory_full_info().uss // 1024 // 1024))
            now = time.time()
            if i > 0:
                mib = DATA_SIZE * (key - first_key) / 1024 / 1024
                mib_s = (DATA_SIZE * (key - last_key) / 1024 / 1024)/(now - last_now)
                print('MiB=%.1f, MiB/s=%.3f' % (mib, mib_s))
                print('> {"%s": {"mib": %d, "mib_s": %.3f}}' % (writer.label, mib, mib_s))
            last_key = key
            last_now = now
        writer.write(data, BATCH_SIZE)
        key += BATCH_SIZE

    writer.close()
    now = time.time()
    mib = DATA_SIZE * (key - first_key) / 1024 / 1024
    mib_s = mib/(now - start_time)
    print('Cum MiB=%.2f, MiB/s=%.2f' % (mib, mib_s))
    print('> {"%s cum": {"mib": %d, "mib_s": %.3f}}' % (writer.label, mib, mib_s))


def main():

    parser = argparse.ArgumentParser(description="Write a database")
    parser.add_argument("--delete-first", action='store_true')
    parser.add_argument("--use-sqlite", action='store_true')
    parser.add_argument("--use-sqlite-wal", action='store_true')
    parser.add_argument("--use-postgresql", action='store_true')
    parser.add_argument("--use-synapse", action='store_true')
    parser.add_argument("filename")
    parser.add_argument("size_in_mb", type=int)
    args = parser.parse_args()
    make_db(**vars(args))

if __name__ == '__main__':
    import argparse
    main()
