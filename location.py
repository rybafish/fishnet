#
# The script extracts distinct ips from access log sqlite table and requests geo-data on new ips
# geodata cached in ipcache table, only new ips are requested.
#
# FishNet, 2022-10-11, EVN
#

import geocoder
import sqlite3
from sqlite3 import Error

from utils import profiler, log, cfg

cfg_dbfile = cfg('db_file', mandatory=True)
cfg_cf_table = cfg('cf_table', mandatory=True)
cfg_ip_column = cfg('ip_column', mandatory=True)

def createTable(cur):
    '''Creates a table structure.'''
    create = '''create table ipcache(
        ip              varchar(64) primary key,
        country         char(2),
        city            varchar(32),
        state           varchar(32)
    );
    '''

    log('Creating the table...')
    try:
        cur.execute(create)
    except Error as e:
        log(f'[E] cannot create the table {e}')
        raise Exception('Cannot create table, stop')

    return
    
def checkTable(cur):
    '''Checks if the table exist, creates one if not.'''
    cur.execute("select name from sqlite_master where type='table' AND name='ipcache'")
    
    rows = cur.fetchall()
    
    if not rows:
        createTable(cur)


@profiler
def getIPs(cur, table, ipColumn):
    '''Extracts list if distintct IPs from the access table not existing in ipcache table'''

    cur.execute(f'select distinct "{ipColumn}" from {table} where "{ipColumn}" not in (select ip from ipcache)')
    
    ips = []
    
    rows = cur.fetchall()
    
    for r in rows:
        ips.append(r[0])
        
    return ips

def enrichIPs(db_file, table, ipColumn):

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
    except Error as e:
        log(str(e), 2)

    checkTable(cur)
    
    with profiler('Extract Geo'):
        ips = getIPs(cur, table, ipColumn)
    
    for ip in ips:
        g = geocoder.ipinfo(ip)
        
        try:
            cur.execute('insert into ipcache (ip, country, state, city) values(?, ?, ?, ?)', [ip, g.country, g.state, g.city])
        except Error as e:
            log(f'[E] cannot insert {[ip, g.country, g.state, g.city]}: {e}, skip')
            raise(e)
            
        log(f'geodata for {ip:>20}: {g.address}')
                
    conn.commit()
    
    return len(ips)

if __name__ == '__main__':
    count = enrichIPs(cfg_dbfile, cfg_cf_table, cfg_ip_column)
    
    if count > 0:
        profiler.report()