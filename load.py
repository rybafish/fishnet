#
# Script loads given folder with Amazon S3 access logs into sqlite database.
# currently only CloudFront access logs supported.
#
# FishNet, 2022-10-11, EVN
#


import gzip
import os
import re
from datetime import datetime

import persist
from utils import profiler, log, cfg

cfg_dbfile = cfg('db_file', mandatory=True)
cfg_cf_table = cfg('cf_table', mandatory=True)
cfg_s3_table = cfg('s3_table', mandatory=False)

cfg_path = cfg('logs_folder', mandatory=True)
cfg_bkp = cfg('bkp_folder', mandatory=True)

def moveToBackup(src, dst, file):
    '''
        moves file from folder to cfg_bkp folder
    '''
    srcFile = os.path.join(src, file)
    dstFile = os.path.join(dst, file)

    if not os.path.isdir(dst):
        os.mkdir(dst)
    
    os.rename(srcFile, dstFile)

@profiler
def processS3File(db, ts, table, srcFolder, file, bkpFolder, count):
    # should read and parse s3 access log
    # prepare the headers
    # hopefully use the same persist call to store parsed data
    # support on the persist part will be required... (at least due to the different table structure)
    
    headers = ['VERSION', 'TIMESTAMP'] + ['bucketowner',
        'bucket',
        'request_ts',
        'ip',
        'requester',
        'requestid',
        'operation',
        'key',
        'request_uri',
        'httpstatus',
        'errorcode',
        'bytessent',
        'objectsize',
        'totaltime',
        'turnaroundtime',
        'referrer',
        'useragent',
        'versionid',
        'hostid',
        'sigv',
        'ciphersuite',
        'authtype',
        'endpoint',
        'tlsversion']
        
    rows = []
        
    with profiler('compile regexp'):
        # https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/
        rowReg = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"\"?[^\"]*\"\"?|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$')
    
    filename = os.path.join(srcFolder, file)
    
    with profiler('Open/Parse S3'):
        with open(filename, mode='rt') as f:
            
            i = 0
            
            filerows = f.read().splitlines()
            
            for row in filerows:
                i += 1
        
                m = rowReg.match(row)
                
                if m:
                    rows.append([file, ts] + list(m.groups()))
                else:
                    log(f'[E] parsing error: {file}/{i}')
                    return None
                
    if persist.persistFile(db, table, headers, rows, 'S3', count):
        moveToBackup(srcFolder, bkpFolder, file)
    
@profiler    
def processCFFile(db, ts, table, srcFolder, file, bkpFolder):
    '''
        Opens a gzipped file and does headers/rows parsing
        Executes immidiate insertion of the results into the database (using persist.py)
        Processed file moved to bkpFolder
        
        parameters
        db:         sqlite database file to connect to
        ts:         load timestamp
        table:      target table to store parsed values
        srcFolder:  source folder with files
        file:       filename with cloudfront logs
        bkpFolder:  folder to move processed file
        
        Returns None anyway
    '''
    
    headers = []
    rows = []
    
    #file = os.path.basename(filename)
    filename = os.path.join(srcFolder, file)
    
    # ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        with gzip.open(filename, mode='rt') as f:
            
            i = 0
            
            filerows = f.read().splitlines()
            
            for row in filerows:
                i += 1
                if i == 1:
                    if row.strip() != '#Version: 1.0':
                        raise Exception(f'Invalid file version, abort ({row})')
                    continue
                if i == 2:
                    if row[:9] != '#Fields: ':
                        raise Exception(f'Invalid headers line, abort: {row[:9]}')
                    headers = row[9:].split(' ')
                    continue
                    
                rows.append([file, ts] + row.split('\t'))
    except Exception as e:
        log(f'[E] Failed reading {filename}, {e}')
        raise(f'[E] Failed reading {filename}, {e}')

    headers = ['VERSION', 'TIMESTAMP'] + headers
    
    c = persist.persistFile(db, table, headers, rows, 'CF')
    if c:
        moveToBackup(srcFolder, bkpFolder, file)


    return c

def processFolder(db, table_cf, table_s3, srcFolder, bkpFolder):
    '''Loads CF files from the specified folder using persist.py
        Only current folder, without subfolders
        Only .gz files considered
        
        returns number of files tried to process, even failed ones count
    '''
    
    # 2022-11-11-16-18-18-E54A0E42471A1A08
    #fileMask = re.compile(r'^\d\d\d\d-\d\d-\d\d-d\d-\d\d-\d\d-\w+$')
    fileMask = re.compile(r'^\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d-[0-9A-F]+$')
    
    def isS3File(testname):
        # very basich check if the file name seems to be a regular s3 access log file
        
        #print(testname)
        
        if fileMask.match(testname):
            return True
        else:
            return False
            
    def isCFFile(testname):
        return testname[-3:] == '.gz' and not os.path.isdir(testname)
    
    with profiler('list files'):
        files = os.listdir(srcFolder)

    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    i = 0
    c = 0
    for f in files:
        filename = os.path.join(srcFolder, f)

        if isCFFile(filename):
            i += 1
            c += processCFFile(db, ts, table_cf, srcFolder, f, bkpFolder)
        elif isS3File(f):
            i += 1 # not yet
            if table_s3:
                processS3File(db, ts, table_s3, srcFolder, f, bkpFolder, i)
            else:
                raise Exception('no table_s3 defined in config file')
        else:
            if not os.path.isdir(filename):
                log(f'[W] not a CF log file: {filename}, skipped', 2)
                
    if i == 0:
        log('Nothing to process.')
        return 0
    else:
        log(f'Files processed: {i}, records created: {c}')
        
    return i
            
if __name__ == '__main__':
    try:
        if processFolder(cfg_dbfile, cfg_cf_table, cfg_s3_table, cfg_path, cfg_bkp):
            profiler.report()
    except KeyboardInterrupt:
        log('[ .. ] Keyboard interrupt')
        profiler.report()
