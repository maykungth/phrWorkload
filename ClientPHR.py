import csv,glob,json, os, json
import random, timeit, requests, logging
from time import sleep
from requests.packages import urllib3
urllib3.disable_warnings()

## Setup Parameter ##
WORKLOAD = '/home/hduser/workspace/phr_client/workload/workloadx_0.75_uniform.csv'
UPLOAD_DIR = '/home/hduser/workspace/phr_client/enc_files'
UPLOAD_LOG = 'upload_log.csv'

user_pass={'user':'system1@example.com','pass':'system1'}
cert_path='/home/hduser/workspace/sslcert/ssl_DSePHR.crt' 
DSePHR_SERV='www.phrapi.com'
DSePHR_PORT='50000'
MAX_TRY = 10
SLEEP_TIME = 5
## End Setup Parameter ##

# Set Log to stderr and file
rootLogger = logging.getLogger(__name__)
rootLogger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(asctime)s,%(levelname)s,%(message)s")

fileHandler = logging.FileHandler(UPLOAD_LOG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

listfile = glob.glob("{}/*.*".format(UPLOAD_DIR))
len_listfile = len(listfile)
dict_file = {}
if len_listfile == 0:
    #rootLogger.warn("UPLOAD_DIR Not Found")
    exit()
else:
    #rootLogger.info("{} files found in UPLOAD_DIR".format(len_listfile))
    for i in listfile:
        dict_file[i.rsplit('/',1)[1]] = i 

# Client Login to the system and get Token_key

r = requests.post("https://{}:{}/login".format(DSePHR_SERV,DSePHR_PORT), data=json.dumps({'email':user_pass['user'],
'password':user_pass['pass']}), headers={'content-type': 'application/json'},verify=False)
tokenkey = r.json()['response']['user']['authentication_token']

# read files from WORKLOAD and upload or download
csvfile =  open(WORKLOAD, 'rb')
wkreader = csv.DictReader(csvfile,delimiter=',')

count=0
for row in wkreader:
    if count == 6000:
        # How many read row from workload files
        break
    #print "{},{},{}".format(row['ops'],row['uid_sysid'],dict_file[row['name']])
    
    if row['ops'] == 'write':
        ## Write the data to the system
        #print "ROW => {}".format(count)
        userid = row['uid_sysid'].split('-')[0]
        sysid = row['uid_sysid'].split('-')[1]
        payload = {'sysid':sysid,'userid':userid}
        f = open(dict_file[row['name']], 'rb')
        files = {'file': f}
        numtry = 0
        while numtry <= MAX_TRY:
            try:
                start = timeit.default_timer()
                res = requests.post('https://{}:{}/upload'.format(DSePHR_SERV,DSePHR_PORT),files=files,data=payload,
                                   headers={'Authentication-Token':tokenkey},verify=False)
                uptime = timeit.default_timer() - start
                break
            except requests.exceptions.RequestException as e:
                rootLogger.error("{} File#{}".format(e,row['#number']))
                sleep(SLEEP_TIME)
            except Exception as e:
                rootLogger.error("{} File#{}".format(e,row['#number']))
                sleep(SLEEP_TIME)
            except:
                rootLogger.error("Unknow Error ! File#{}".format(row['#number']))
                sleep(SLEEP_TIME)
            numtry+=1
            if numtry >= 10:
                rootLogger.error("Error more than 10 times File#{}".format(row['#number']))
        f.close()
        result = dict(res.json())
        rootLogger.info("write,{},{},sec:{:.3f},size:{},#{}".format(result['filename'],result['rowkey'],uptime,row['size(kb)'],row['#number']))
    elif row['ops'] == 'read':
        f = open(UPLOAD_LOG,"r")
        reader=list(csv.reader(f,delimiter=','))
        f.close()
        len_reader = len(reader)
        if len_reader == 0:
            continue
        rand_num = random.randint(1,len_reader)
        for i in xrange(len_reader):
            ir = (rand_num+i)%len_reader
            if len(reader[ir]) < 5:
                continue
            if reader[ir][4] == row['name'] and reader[ir][3] == 'write':
                # Download the file from DSePHR
                numtry = 0
                while numtry <= MAX_TRY:
                    try:
                        start = timeit.default_timer()
                        res = requests.get("https://{}:{}/download/{}".format(DSePHR_SERV,DSePHR_PORT,reader[ir][5]),headers={'Authentication-Token':tokenkey},verify=False)
                        if res.headers['Content-Type'] == 'application/json':
                            rootLogger.warn(dict(res.json()))
                            sleep(SLEEP_TIME)  # wait for downloading again
                            continue
                        filename= str(res.headers['content-disposition']).rsplit('"')[1]
                        with open (filename,'wb') as fdw:
                            for i in res.iter_content(1024):
                                fdw.write(i)
                        uptime = timeit.default_timer() - start
                        rootLogger.info("read,{},{},sec:{:.3f},size:{},#{}".format(filename,reader[ir][5],uptime,row['size(kb)'],row['#number']))
                        os.remove(filename)
                        break
                    except requests.exceptions.RequestException as e:
                        rootLogger.error("{} File#{}".format(e,row['#number']))
                        sleep(SLEEP_TIME)
                    except Exception as e:
                        rootLogger.error("{} File#{}".format(e,row['#number']))
                        sleep(SLEEP_TIME)
                    except:
                        rootLogger.error("Unknow Error ! File#{}".format(row['#number']))
                        sleep(SLEEP_TIME)
                    numtry+=1
                    if numtry >= 10:
                        rootLogger.error("Error more than 10 times File#{}".format(row['#number']))
                break
            else:
                if i >= len_reader-1 :
                    rootLogger.error("Cannot find {} to download,#{}".format(row['name'],row['#number']))
                continue
    count +=1
