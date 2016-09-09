# workload Generator #
import csv, glob, random
from os import path, mkdir
from sys import argv

## Setup Parameter ##
UPLOAD_DIR = '/home/hduser/workspace/phr_client/enc_files'
WORKLOAD_UNI = 'uniform.csv'
WORKLOAD_DIS = 'dis.csv'
WRITE_POS = 0.5   # write possibility
NUMBER_OPS = 10000
range_userid={'start':1,'end':5000}
range_sysid={'start':0,'end':9}

# get directory workload from first argv
if len(argv) == 1:
    if path.isdir('workload') == False:
        mkdir('workload')
    WORKLOAD_UNI = "{}/workload_{}_{}".format('workload',str(WRITE_POS),WORKLOAD_UNI)
    WORKLOAD_DIS = "{}/workload_{}_{}".format('workload',str(WRITE_POS),WORKLOAD_DIS)
else:
    if path.isdir(argv[1]) == False:
        mkdir(str(argv[1]))
    WORKLOAD_UNI = "{}/{}_{}_{}".format(str(argv[1]),format(str(argv[1])),str(WRITE_POS),WORKLOAD_UNI)
    WORKLOAD_DIS = "{}/{}_{}_{}".format(str(argv[1]),format(str(argv[1])),str(WRITE_POS),WORKLOAD_DIS)
            
def rand_wr(write_poss):
    if random.random() <= write_poss:
        return(True) # return True = write
    else:
        return(False) # return False = read
f = open(WORKLOAD_UNI,'w')

# Scan files in UPLOAD_DIR
listfile = glob.glob("{}/*.*".format(UPLOAD_DIR))
file_list = []
file_uni_total = {}
file_dis_total = {}
file_dis_list1 = []
file_dis_list2 = []
file_dis_list3 = []
file_dis_list4 = []
for i in range(0,len(listfile)):
    file = {"name":listfile[i].rsplit('/',1)[1],"size":path.getsize(listfile[i])/1024} # convert byte to KB
    #file_list.append(file)
    file_uni_total[file['name']] = 0
    file_dis_total[file['name']] = 0

    if file['size'] <= 100:    # less than 100kb
        file_dis_list1.append(file)   
    elif file['size'] <= 1024:   # less than 1 Mb
        file_dis_list2.append(file)
    elif file['size'] <= 10240:   # less than 10 Mb
        file_dis_list3.append(file)
    else:                           # more than 10 Mb
        file_dis_list4.append(file)

write_total = 0
read_total = 0
NUMBER_OPS += 1

f.write("#number,ops,uid_sid,name,size(kb)\n")
# uniform distribution
for i in range(1,NUMBER_OPS):
    # write possibility
    if rand_wr(WRITE_POS):
        ops = 'write'
        write_total += 1
    else:
        ops = 'read'
        read_total += 1
    
    # random userid and sysid
    userid = random.randint(range_userid['start'],range_userid['end'])
    sysid = random.randint(range_sysid['start'],range_sysid['end'])

    # random range of file size
    randpos = random.random()
    if randpos <= 0.25:
        rand_file = random.choice(file_dis_list1)
    elif randpos <= 0.50:
        rand_file = random.choice(file_dis_list2)
    elif randpos <= 0.75:
        rand_file = random.choice(file_dis_list3)
    else:
        rand_file = random.choice(file_dis_list4)
    f.write("{},{},u{}-s{},{},{}\n".format(i,ops,userid,sysid,rand_file['name'],rand_file['size']))
    file_uni_total[rand_file['name']] += 1

f.write("#,ops,write_total,read_total\n")
f.write("#,w_r,{},{}\n".format(write_total,read_total))
f.write("#,name,total\n")
for x in file_uni_total:
    f.write("#,{},{}\n".format(x,file_uni_total[x]))

f.close()

write_total = 0
read_total = 0

# size distribution
f = open(WORKLOAD_DIS,'w')
f.write("#number,ops,uid_sysid,name,size(kb)\n")

for i in range(1,NUMBER_OPS):
    if rand_wr(WRITE_POS):
        ops = 'write'
        write_total += 1
    else:
        ops = 'read'
        read_total += 1
    randpos = random.random()

    # random userid and sysid
    userid = random.randint(range_userid['start'],range_userid['end'])
    sysid = random.randint(range_sysid['start'],range_sysid['end'])

    if randpos <= 0.27:
        # Case < 100 kb 
        rand_file = random.choice(file_dis_list1)
        f.write("{},{},u{}-s{},{},{}\n".format(i,ops,userid,sysid,rand_file['name'],rand_file['size']))
    elif randpos <= 0.73:
        # Case < 1024 kb (1MB)
        rand_file = random.choice(file_dis_list2)
        f.write("{},{},u{}-s{},{},{}\n".format(i,ops,userid,sysid,rand_file['name'],rand_file['size']))
    elif randpos <= 0.91:
        # Case < 10240 kb (10MB)
        rand_file = random.choice(file_dis_list3)
        f.write("{},{},u{}-s{},{},{}\n".format(i,ops,userid,sysid,rand_file['name'],rand_file['size']))
    else:
        # Case >= 10241 kb
        rand_file = random.choice(file_dis_list4)
        f.write("{},{},u{}-s{},{},{}\n".format(i,ops,userid,sysid,rand_file['name'],rand_file['size']))
    file_dis_total[rand_file['name']] += 1


f.write("#,ops,write_total,read_total\n")
f.write("#,w_r,{},{}\n".format(write_total,read_total))
f.write("#,name,total\n")
for x in file_uni_total:
    f.write("#,{},{}\n".format(x,file_dis_total[x]))
