#
# Script loads given folder with Amazon S3 access logs into sqlite database.
# currently only CloudFront access logs supported.
#
# FishNet, 2022-10-11, EVN
#


import gzip
import os
from datetime import datetime

import persist
from utils import profiler, log, cfg

cfg_dbfile = cfg('db_file', mandatory=True)
cfg_cf_table = cfg('cf_table', mandatory=True)

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

def processCFFile(db, table, srcFolder, file, bkpFolder):
    # should read and parse s3 access log
    # prepare the headers
    # hopefully use the same persist call to store parsed data
    # support on the persist part will be required... (at least due to the different table structure)
    pass
    
def processCFFile(db, table, srcFolder, file, bkpFolder):
    '''
        Opens a gzipped file and does headers/rows parsing
        Executes immidiate insertion of the results into the database (using persist.py)
        Processed file moved to 
        
        parameters
        db:         sqlite database file to connect to
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
    
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
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
    
    if persist.persistFile(db, table, headers, rows):
        moveToBackup(srcFolder, bkpFolder, file)

def processFolder(db, table, srcFolder, bkpFolder):
    '''Loads CF files from the specified folder using persist.py
        Only current folder, without subfolders
        Only .gz files considered
        
        returns number of files tried to process, even failed ones count
    '''
    
    def isS3File(testname):
        # very basich check if the file name seems to be a regular s3 access log file
        pass
    
    def isCFFile(testname):
        return testname[-3:] == '.gz' and not os.path.isdir(testname)
    
    files = os.listdir(srcFolder)
    
    i = 0
    for f in files:
        filename = os.path.join(srcFolder, f)
        
        if isCFFile(filename):
            i += 1
            processCFFile(db, table, srcFolder, f, bkpFolder)
        elif isS3File:
            i += 1*0 # not yet
            log(f'[W] not yet... {f}')
            processS3File(db, table, srcFolder, f, bkpFolder)
        else:
            if not os.path.isdir(filename):
                log(f'[W] not a CF log file: {filename}, skipped', 2)
                
    if i == 0:
        log('Nothing to process.')
        return 0
        
    return i
            
if __name__ == '__main__':
    if processFolder(cfg_dbfile, cfg_cf_table, cfg_path, cfg_bkp):
        profiler.report()