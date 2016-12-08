import csv,glob
from datetime import datetime
from datetime import timedelta
import numpy
start_seq = 500
num_seq = 5000

WORK_DIR = "C:\Users\mayku\Documents\LAB_res_lar\Serv\L_0.5_dis"
CLIENT_DIR = "C:\Users\mayku\Documents\LAB_res_lar\L_0.5_dis"
OUT_FILE = "Con_Serv_side_{}.csv".format(WORK_DIR.split("\\")[-1])

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
                elif temp < start_time:
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
    #print start_time, end_time
    return {'start_time':start_time,'end_time':end_time}
f_out = open(OUT_FILE,"w")
used_time = find_start_end_time()
print "START : {}\nEND : {}".format(used_time['start_time'],used_time['end_time'])
f_out.write("Unit,bytes : Mb's,cpu_usage : %,mem_usage : GB,disk_usage : GB\n")
f_out.write("START : {}\nEND : {}\n".format(used_time['start_time'],used_time['end_time']))

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
work_file = glob.glob("{}/*.*".format(WORK_DIR))

for csv_file in work_file:
    f = open(csv_file,"r")
    f_out.write("FILE : {}\n".format(csv_file))
    reader = csv.DictReader(f,delimiter=',')
    dict_table={}
    for k in iter(reader.next().keys()):
        dict_table[str(k)]=[]
    for row in reader:
        time = get_time_of_row(row)
        if used_time['start_time'] <= time <= used_time['end_time']:
            for k in row:
                if row[k] == "NaN":
                    continue
                if k != 'Timestamp':
                    # for bytes_in and bytes_out
                    if "bytes" in csv_file:
                        dict_table[k].append(float(row[k])/1024/1024) # convert to Mb/s
                    elif "cpu_idle" in csv_file:
                        dict_table[k].append(100.0-float(row[k])) # convert to cpu usage
                    elif "mem_free" in csv_file:
                        dict_table[k].append((8.0-float(row[k])/1024/1024)) # convert to memory usage
                    elif "disk_free" in csv_file:
                        disk_usage = 468.0-float(row[k])
                        if disk_usage >= 0.0:
                            dict_table[k].append(disk_usage) # convert to disk usage
                        else:
                            dict_table[k].append(0.0)
                else:
                    dict_table[k].append(datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00"))
    # 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
    if "HDFS" in csv_file:
        f_out.write("LOG,HDFS_")
    elif "DS" in csv_file:
        f_out.write("LOG,WebService_")
    if "bytes_in" in csv_file:
        f_out.write("Bytes_IN (MB/s)\n")
    elif "bytes_out" in csv_file:
        f_out.write("Bytes_OUT (MB/s)\n")
    elif "cpu_idle" in csv_file: 
        f_out.write("CPU_USAGE (%)\n")
    elif "mem_free" in csv_file:
        f_out.write("Memory_Usage (GB)\n")
    elif "disk_free" in csv_file:
        f_out.write("Disk_Usage (GB)\n")

    f_out.write("SERV,NUM,MAX,AVG,MIN,STD,VAR,95th,99th\n")
    for serv in sorted(dict_table):
        if serv != 'Timestamp':
            v = dict_table[serv]
            f_out.write("{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f}\n".format(serv,len(v),numpy.max(v),numpy.mean(v),numpy.min(v),numpy.std(v),numpy.var(v),
            numpy.percentile(v,95),numpy.percentile(v,99)))






