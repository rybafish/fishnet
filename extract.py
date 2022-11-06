#
# Script to download stuff from specified S3 bucket.
# 
# Access keys are to be maintained in ~\.aws\credentials file
# in case of windows usage: users\<username>\.aws\credentials
#
# Rest of configuration maintained in config.yaml
#
# FishNet, 2022-10-10, EVN
#



import boto3
from botocore.exceptions import ClientError
import os
from utils import intToStr, profiler, log, fileBytes, cfg

conf_bucket = cfg('s3_bucket')
conf_folder = cfg('download_folder')
conf_delete = cfg('delete_remote_files')

@profiler
def downloadFile(s3client, bucketName, key, targetFolder, modifyTime=None):
    '''
        The procedure downloads and stores file from the AWS s3 bucket
        Supports subfolders (will create local subfolders if not exist)
    
        Files stored with local modify time in local time zone
        
        input: already connected s3client
        bucketName - ...
        key - file name (same input and output)
        targetFolder - base of the target folder (without subfolders)
        modifyTime - time to be set for the file created (usually taken from S3 key metadata ouside this call)
        
        returns True if okay,  False if not
    '''
    
    def understandFolder(fname):
        path = fname.split('/')
        folders = path[:-1]
        fname = path[-1]
        return folders, fname
    
    folders, filename = understandFolder(key)
    
    targetFolderFull = os.path.join(targetFolder, bucketName)
    
    for folder in folders:
        targetFolderFull = os.path.join(targetFolderFull, folder)
        
    with profiler('Check Folder'):
        if not os.path.isdir(targetFolderFull):
            os.mkdir(targetFolderFull)
        
    output = os.path.join(targetFolderFull, filename)

    with profiler('Object Download'):
        try:
            with open(output, 'wb') as f:
                s3client.download_fileobj(bucketName, key, f)
        except Exception as e:
            log(f'[E] issue downloading/storing {output}: {e}')
            return False

    if modifyTime:
        with profiler('Modify Create Time'):
            os.utime(output, (modifyTime, modifyTime))
            
    return True
        
@profiler
def extract(bucketName, folder, delFlag=False):
    '''
        Script itterates through the complete S3 bucket and downloads
        the content (using the downloadFile call)
        
        bucketName - name of the S3 bucket
        folder - target folder
        delFlag - when set, file will be deleted on the S3 side (otherwise kept intact)
        
        Returns total numbet of processed files.
    '''

    #boto3.set_stream_logger('botocore', level='DEBUG')

    log('Connecting to S3')
    s3 = boto3.resource('s3')
    s3client = boto3.client('s3')
    #s3 = boto3.resource('s3', aws_access_key_id='...', aws_secret_access_key='...')
    
    log('Connected, init page iterrator...')
        
    paginator = s3client.get_paginator('list_objects')
    pageIterrator = paginator.paginate(Bucket=bucketName)
    
    files = []
    pageNumber = 0
    
    totalItems = 0
    totalSize = 0
    
    deleteFlag = ' [D]' if delFlag else ''
    
    with profiler('Main Loop'):
        try: 
            for page in pageIterrator: #this iterration potenrially results in S3 exception
            
                if 'Contents' not in page:
                    log('The bucket is empty, exiting')
                    break
            
                items = page['Contents']
                pageSize = 0
                i = 0
                
                try:
                    with profiler('Iterate Items'):
                        for f in items:
                            pageSize += f['Size']
                            
                            log(f"{pageNumber:3} {f['Key']} {f['LastModified']} {f['Size']}{deleteFlag}")
                            
                            downloadFile(s3client, bucketName, f['Key'], folder, f['LastModified'].timestamp())

                            if deleteFlag:
                                with profiler('Object delete'):
                                    s3.Object(bucketName, f['Key']).delete()
                            
                            i+=1
                except KeyboardInterrupt:
                    log('[!] KeyboardInterrupt')
                    log('will report partically incorrect data.')
                    
                    totalItems += i
                    totalSize += pageSize
                    break

                pageNumber += 1
                totalItems += len(items)
                totalSize += pageSize

                log(f'Page: {pageNumber} Items: {len(items)}, size: {fileBytes(totalSize)} ({pageSize})')
                
        except ClientError as e:
            log(f'[E] S3 Exception: {e}' )
        
    if totalItems > 0:
        log('-')
        log(f'Total pages: {intToStr(pageNumber)}, items: {intToStr(totalItems)}, size: {fileBytes(totalSize)}')
    
    return totalItems
    
if __name__ == '__main__':
    
    itemsProcessed = extract(conf_bucket, conf_folder, conf_delete)
    
    if itemsProcessed >= 1:
        profiler.report()