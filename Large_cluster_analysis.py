# Find start and end time of sequence 500-5500
# Use those time to find max and min of sum (bytes_in of DS1-6)
# 
import csv,glob
from datetime import datetime
from datetime import timedelta
import numpy
start_seq = 500
num_seq = 5000
thres_time = 1 # minute

WORK_DIR = "D:\DATA_lab\LAB_res_lar\Serv\L_1.0_uniform"
CLIENT_DIR = "D:\DATA_lab\LAB_res_lar\L_1.0_uniform"
OUT_FILE = "Large_Cluster_{}.csv".format(WORK_DIR.split("\\")[-1])

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
# calc start_time and end_time from client
def get_time_of_row(row):
    # 2016-10-26T12:09:00+07:00
    return datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00")
def find_start_end_time():
    list_client_file = glob.glob("{}/*.*".format(CLIENT_DIR))
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=    

    # to find first #500 and last #5500
    start_time = None
    end_time = None
    for csv_file in list_client_file:
        f = open(csv_file,"r")
        reader= csv.reader(f,delimiter=',')
        cur = 0
        for row in reader:
            if cur == start_seq+1:
                # to find first sequnece 500 from all files
                temp = datetime.strptime(row[0],"%Y-%m-%d %H:%M:%S")
                if start_time == None:
                    start_time = temp
                elif temp < start_time:   # if there is start_time from other client less than the old one, change it
                    start_time = temp
                    #print "start ==> ", csv_file
            elif cur == start_seq + num_seq + 1:
                temp = datetime.strptime(row[0],"%Y-%m-%d %H:%M:%S")
                if end_time == None:
                    end_time = temp
                elif temp > end_time:
                    end_time = temp
                    #print "end ==>", csv_file
            if len(row) == 9:
                cur += 1
        f.close()
    #print start_time, end_time
    return {'start_time':start_time,'end_time':end_time}

def add_item(dict_item, row,csv_file):
    n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
    if row[2] == 'INFO':
        if row[4] in dict_item:
            dict_item[row[4]]["{}_{}".format(n_client,row[0].split(' ')[1])]= float(row[6].split(":")[1])
        else:
            dict_item[row[4]] = {}
            dict_item[row[4]]["{}_{}".format(n_client,row[0].split(' ')[1])]= float(row[6].split(":")[1])
    return dict_item

def create_table(dict_item,f_out,label,type_ops=""):
    f_out.write("{}\n".format(label))
    f_out.write("{},FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n".format(type_ops))
    for k,v in dict_item.items():
            f_out.write("{},{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(type_ops,k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
            numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))

f_out = open(OUT_FILE,"w")
used_time = find_start_end_time()
print "START : {}\nEND : {}".format(used_time['start_time'],used_time['end_time'])
f_out.write("Unit,bytes : Mb/s\n")
f_out.write("START : {}\nEND : {}\n".format(used_time['start_time'],used_time['end_time']))

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
work_file = glob.glob("{}/*.*".format(WORK_DIR))

for csv_file in work_file:
    if 'bytes' not in csv_file:
        continue
    f = open(csv_file,"r")
    f_out.write("FILE : {}\n".format(csv_file))
    reader = csv.DictReader(f,delimiter=',')
    dict_table={}
    # for k in iter(reader.next().keys()):
    #     dict_table[str(k)]=[]
    for row in reader:
        time = get_time_of_row(row)
        if used_time['start_time'] <= time <= used_time['end_time']:
            sum_byte=0.0
            for k in row:
                if row[k] == "NaN":
                    break
                if k == 'Timestamp':
                    time = datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00")
                elif 'LAB_DS' or 'LAB_Rserver' in k:
                    # sum bytes
                    sum_byte += float(row[k])/1024/1024
            dict_table[time] = sum_byte
                    
    # 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
    if "HDFS" in csv_file:
        f_out.write("LOG,HDFS_")
    elif "DS" in csv_file:
        f_out.write("LOG,WebService_")
    if "bytes_in" in csv_file:
        f_out.write("Bytes_IN (MB/s)\n")
    elif "bytes_out" in csv_file:
        f_out.write("Bytes_OUT (MB/s)\n")

    max_time = max(dict_table,key=dict_table.get)
    min_time = min(dict_table,key=dict_table.get)
    f_out.write("MAX,{},{}\n".format(max_time,dict_table[max_time]))
    f_out.write("MIN,{},{}\n".format(min_time,dict_table[min_time]))

    # At this point we get max time and min time and we need to see what client do in this time


    list_client_file = glob.glob("{}/*.*".format(CLIENT_DIR))
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=    

    dict_files_write_all_max_time = {}
    dict_files_read_all_max_time = {}

    dict_files_write_all_min_time = {}
    dict_files_read_all_min_time = {}

    for csv_file in list_client_file:
        f = open(csv_file,"r")
        #f_out.write("LOG,{}\n".format(csv_file.split("\\")[-1]))
        n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
        reader= csv.reader(f,delimiter=',')
        for row in reader:
            time=datetime.strptime(row[0],"%Y-%m-%d %H:%M:%S")
            # max time period
            if max_time - timedelta(minutes=thres_time) <= time <= max_time + timedelta(minutes=thres_time):
                #f_out.write("{},{},{},{},{},{}\n".format(row[0],row[2],row[3],row[4],row[6],row[8]))
                if row[2] == 'INFO' and row[3] == 'write':
                    dict_files_write_all_max_time = add_item(dict_item=dict_files_write_all_max_time,row=row,csv_file=csv_file)
                if row[2] == 'INFO' and row[3] == 'read':
                    dict_files_read_all_max_time = add_item(dict_item=dict_files_read_all_max_time,row=row,csv_file=csv_file)

            # min time period
            elif min_time - timedelta(minutes=thres_time) <= time <= min_time + timedelta(minutes=thres_time):
                #f_out.write("{},{},{},{},{},{}\n".format(row[0],row[2],row[3],row[4],row[6],row[8]))
                if row[2] == 'INFO' and row[3] == 'write':
                    dict_files_write_all_min_time = add_item(dict_item=dict_files_write_all_min_time,row=row,csv_file=csv_file)

                if row[2] == 'INFO' and row[3] == 'read':
                    dict_files_read_all_min_time = add_item(dict_item=dict_files_read_all_min_time,row=row,csv_file=csv_file)
        f.close()
    
    # print "lens of all write {}\n lens of all read {}\n".format(len(dict_files_write_all_max_time),len(dict_files_read_all_max_time))
    # def create_table(dict_item,f_out,label,type_ops=""):
    create_table(dict_item=dict_files_write_all_max_time,f_out=f_out,
                label="LOG: At Max time ALL Client {} +- {} minutes".format(max_time,thres_time),
                type_ops = "write"
                )

    create_table(dict_item=dict_files_read_all_max_time,f_out=f_out,
                label="",
                type_ops = "read"
                )

    create_table(dict_item=dict_files_write_all_min_time,f_out=f_out,
                label="LOG: At Min time ALL Client {} +- {} minutes".format(min_time,thres_time),
                type_ops = "write"
                )
    create_table(dict_item=dict_files_read_all_min_time,f_out=f_out,
                label="",
                type_ops = "read"
                )






