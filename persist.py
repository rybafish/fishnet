#
# Persist the given data into sqlite database.
# Table structure intended to persist cloudfront access logs
#
# FishNet, 2022-10-21, EVN
#

import sqlite3
from sqlite3 import Error

'''

TODO

1. возвращать true/false чтобы понимать надо удалять файл или нет
2. поставить обработку исключений на всю работу с базой (и на основе этого пункт -1)

'''

from utils import log, profiler, cfg

config_dbfile = cfg('db_file', mandatory=True)
config_table = cfg('cf_table', mandatory=True)

def createTable(cur, table):
    '''
        creatse table structure to store cloudFront access logs
        
        cur:    open sqlite cursor
        table:  table name
    '''
    create = f'''create table {table} (
        id                                  integer primary key autoincrement,
        version                             varchar(64),
        timestamp                           timestamp,
        "date"                              char(10),
        "time"                              char(8),
        "x-edge-location"                   varchar(32),
        "sc-bytes"                          int,
        "c-ip"                              char(64),
        "cs-method"                         varchar(16),
        "cs(Host)"                          varchar(255),
        "cs-uri-stem"                       varchar(255),
        "sc-status"                         int,
        "cs(Referer)"                       varchar(255),
        "cs(User-Agent)"                    varchar(255),
        "cs-uri-query"                      varchar(8),
        "cs(Cookie)"                        varchar(8),
        "x-edge-result-type"                varchar(8),
        "x-edge-request-id"                 varchar(64),
        "x-host-header"                     varchar(64),
        "cs-protocol"                       varchar(8),
        "cs-bytes"                          int,
        "time-taken"                        real,
        "x-forwarded-for"                   varchar(64),
        "ssl-protocol"                      varchar(16),
        "ssl-cipher"                        varchar(16),
        "x-edge-response-result-type"       varchar(8),
        "cs-protocol-version"               varchar(8),
        "fle-status"                        varchar(8),
        "fle-encrypted-fields"              varchar(8),
        "c-port"                            int,
        "time-to-first-byte"                real,
        "x-edge-detailed-result-type"       varchar(8),
        "sc-content-type"                   varchar(16),
        "sc-content-len"                    int,
        "sc-range-start"                    varchar(8),
        "sc-range-end"                      varchar(8)
    );
    '''
    
    log(f'Creating the table {table}...')
    try:
        cur.execute(create)
    except Error as e:
        log(f'[E] {e}')
        raise Exception('Cannot create table, stop')
    
    return
    
    
def checkTable(cur, table):
    '''Checks if the table exists, creates one if not'''
    cur.execute(f"select name from sqlite_master where type='table' AND name='{table}'")
    
    rows = cur.fetchall()
    
    if not rows:
        createTable(cur, table)
        
        
def persistFile(db_file, table, headers, rows):
    ''''
        Pesrists rows into a given sqlite table
        
        Parameters:
        db_file:    sqlite database file
        table:      name of the table to persist data in
        headers:    list of headers to compose an insert statement
        rows:       list of rows - data to be stored, each row also is a list of values
        
        if the structure of headers does not match the structure of the created table
        hopefully, it will fail and somehow manifest the error. This is expected
        to happen if/when the access log structure change.
        
        returns the number of rows inserted or None in case of error.
    '''
    conn = None
    
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
    except Error as e:
        log(str(e), 2)
        return None

    checkTable(cur, table)
    
    headersStr = '"' + '", "'.join(headers) + '"'
    
    c = 0

    if not rows:
        log(f'[ W  ] {file} is empty, skip')
        return 0
    
    # check if already loaded
    
    with profiler('persist check existance'):
        file = rows[0][0]
        cur.execute(f"select count(*) from {table} where version=?", [file])
        cnt = cur.fetchall()
        if cnt and cnt[0][0] > 0:
            log(f'[ W  ] {file} already exists, will skip')
            return 0

    valuesStr = ('?, '*len(rows[0]))[:-2]

    with profiler('persist inserts'):
        for r in rows:
        
            try:
                sql = f'insert into {table} ({headersStr}) values ({valuesStr});'
                cur.execute(sql, r)
                
                c += 1
            except Error as e:
                log(f'[E] cannot insert {valuesStr}, :{e}, skipping')
                return None
        
    with profiler('persist inserts commit'):
        log(f'[ ok ] {file} done, {c} records')
        conn.commit()   
        
    return c
    
if __name__ == '__main__':
    persist(config_dbfile, config_table, None, None)
