import re, os, sys

def compare(s, verbose = False):
    #s = '9509a447d0e5ddbc577482add16849867e0d6911702516267e5ce02f763a17f1 files.dugwin.net [22/Jul/2015:07:27:16 +0000] 92.248.208.59 - 7C3E2A969D98883F REST.GET.OBJECT photo/2012/dbd/img_5719.jpg "GET /photo/2012/dbd/img_5719.jpg HTTP/1.1" 200 - 107230 107230 46 45 "-" "Mozilla/5.0 (Linux; Android 4.4.2; PocketBook SURFpad 3 (7,85") Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Safari/537.36 GSA/4.8.12.19.arm" -'

    #s = '9509a447d0e5ddbc577482add16849867e0d6911702516267e5ce02f763a17f1 files.dugwin.net [04/Jan/2016:16:51:18 +0000] 95.30.237.5 - C2642C4DBA8B6864 REST.GET.OBJECT photo/daybyday/img_6712.jpg "GET /photo/daybyday/img_6712.jpg HTTP/1.1" 200 - 163353 163353 56 54 "https://www.google.ru/" "Mozilla/5.0 (Linux; Android 4.2.2; PocketBook SURFpad 3 (10,1") Build/JDQ39) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Safari/537.36" -'

    #s = '9509a447d0e5ddbc577482add16849867e0d6911702516267e5ce02f763a17f1 files.dugwin.net [11/May/2015:02:21:50 +0000] 37.235.215.118 - ED5DB9C761C29115 REST.GET.OBJECT thursday/thursday72.mp3 "GET /thursday/thursday72.mp3 HTTP/1.1" 200 - 31853237 32018511 8986 52 "http://www.dugwin.net/podcast/thursday" "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36" -'

    rowReg = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"\"?[^\"]*\"\"?|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$')

    rowRegM = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"\"?[^\"]*\"?[^\"]*\"\"?|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$')

    # 10 groups
    #rowReg = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*).*$')

    # 15 groups
    #rowReg = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"\"?[^\"]*\"\"?|-) ([^ ]*).*$')

    #rowReg = re.compile('([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"\"?[^\"]*\"?[^\"]*\"\"?|-).*$')

    headers = ['bucketowner', 'bucket', 'request_ts', 'ip', 'requester', 'requestid', 'operation', 'key', 'request_uri', 'httpstatus',
        'errorcode', 'bytessent', 'objectsize', 'totaltime', 'turnaroundtime', 'referrer', 'useragent', 'versionid', 'hostid', 'sigv',
        'ciphersuite', 'authtype', 'endpoint', 'tlsversion']

    m = rowReg.match(s)
    mm = rowRegM.match(s)

    if m and mm:

        i = 0
        
        g = m.groups()
        gm = m.groups()
        
        for i in range(len(headers)):
            if g[i] == gm[i]:
                st = 'ok'
            else:
                st = '!!'
                
            if verbose:
                print(f'{headers[i]:32} {st}  {g[i]}')
            
            if st != 'ok':
                break
    else:
        print('Error')
        return False
        
    if st != 'ok':
        print('Error')
        return False
        
    return True
    

if __name__ == '__main__':

    srcFolder = r'C:\home\dug\fishnet\stage\test.logs\.processed'
    srcFolder = r'C:\home\dug\dugwin\s3\stage\dgwn.logs\part2'
    srcFolder = r'C:\home\dug\dugwin\s3\stage\dgwn.logs\part1'
    srcFolder = r'C:\home\dug\dugwin\s3\stage\dgwn.logs\part3'

    files = os.listdir(srcFolder)
    
    i = 0
    for fn in files:
        filename = os.path.join(srcFolder, fn)
    
        with open(filename, mode='rt') as f:
            filerows = f.read().splitlines()
            
            for row in filerows:
                if compare(row):
                    print(f'{i:3} {fn}: ok')
                else:
                    print(f'!!! {fn}' )
                    compare(row, True)
                    sys.exit(1)
            
        i += 1
        
        if i >= 10000 and False:
            break
            