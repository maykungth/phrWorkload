# process data before and after adding serv 30 minutes
import csv,glob
from datetime import datetime
from datetime import timedelta
import numpy

WORK_DIR = "C:\Users\mayku\Documents\LAB_small\Serv_small\L_1.0_dis"
OUT_FILE = "Con_Serv_small_side_{}.csv".format(WORK_DIR.split("\\")[-1])

ADD_SERV_TIME = datetime.strptime("2016-10-26 15:30:00","%Y-%m-%d %H:%M:%S")
RANGE_TIME = timedelta(minutes=30)

def get_time_of_row(row):
    # 2016-10-26T12:09:00+07:00
    return datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00")
f_out = open(OUT_FILE,"w")

before_time = {'start_time':ADD_SERV_TIME-timedelta(minutes=31),'end_time':ADD_SERV_TIME-timedelta(minutes=1)}
after_time = {'start_time':ADD_SERV_TIME+timedelta(minutes=1),'end_time':ADD_SERV_TIME+timedelta(minutes=31)}

f_out.write("Unit,bytes : Mb's,cpu_usage : %,mem_usage : GB,disk_usage : GB\n")
f_out.write("ADD_SERV_TIME: {}\n".format(ADD_SERV_TIME))

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
work_file = glob.glob("{}/*.*".format(WORK_DIR))


for csv_file in work_file:
    f = open(csv_file,"r")
    f_out.write("FILE : {}\n".format(csv_file))
    reader = csv.DictReader(f,delimiter=',')
    dict_table_before={}
    dict_table_after={}
    for k in iter(reader.next().keys()):
        dict_table_before[str(k)]=[]
        dict_table_after[str(k)]=[]

    for row in reader:
        time = get_time_of_row(row)
        if before_time['start_time'] <= time <= before_time['end_time']:
            for k in row:
                if row[k] == "NaN":
                    continue
                if k != 'Timestamp':
                    # for bytes_in and bytes_out
                    if "bytes" in csv_file:
                        dict_table_before[k].append(float(row[k])/1024/1024) # convert to Mb/s
                    elif "cpu_idle" in csv_file:
                        dict_table_before[k].append(100.0-float(row[k])) # convert to cpu usage
                    elif "mem_free" in csv_file:
                        dict_table_before[k].append((8.0-float(row[k])/1024/1024)) # convert to memory usage
                    elif "disk_free" in csv_file:
                        disk_usage = 468.8-float(row[k])
                        if disk_usage >= 0.0:
                            dict_table_before[k].append(disk_usage) # convert to disk usage
                        else:
                            dict_table_before[k].append(0.0)
                else:
                    dict_table_before[k].append(datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00"))

        elif after_time['start_time'] <= time <= after_time['end_time']:
            for k in row:
                if row[k] == "NaN":
                    continue
                if k != 'Timestamp':
                    # for bytes_in and bytes_out
                    if "bytes" in csv_file:
                        dict_table_after[k].append(float(row[k])/1024/1024) # convert to Mb/s
                    elif "cpu_idle" in csv_file:
                        dict_table_after[k].append(100.0-float(row[k])) # convert to cpu usage
                    elif "mem_free" in csv_file:
                        dict_table_after[k].append((8.0-float(row[k])/1024/1024)) # convert to memory usage
                    elif "disk_free" in csv_file:
                        disk_usage = 468.8-float(row[k])
                        if disk_usage >= 0.0:
                            dict_table_after[k].append(disk_usage) # convert to disk usage
                        else:
                            dict_table_after[k].append(0.0)
                else:
                    dict_table_after[k].append(datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00"))
    
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
    f_out.write("TIME,{} => {}\n".format(before_time['start_time'],before_time['end_time']))
    f_out.write("SERV,NUM,MAX,AVG,MIN,STD,VAR,95th,99th\n")
    for serv in sorted(dict_table_before):
        if serv != 'Timestamp':
            v = dict_table_before[serv]
            f_out.write("{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f}\n".format(serv.replace("LAB_Rserver","LAB_RS"),len(v),numpy.max(v),numpy.mean(v),numpy.min(v),numpy.std(v),numpy.var(v),
            numpy.percentile(v,95),numpy.percentile(v,99)))

    f_out.write("TIME,{} => {}\n".format(after_time['start_time'],after_time['end_time']))
    f_out.write("SERV,NUM,MAX,AVG,MIN,STD,VAR,95th,99th\n")
    for serv in sorted(dict_table_after):
        if serv != 'Timestamp':
            v = dict_table_after[serv]
            f_out.write("{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f}\n".format(serv.replace("LAB_Rserver","LAB_RS"),len(v),numpy.max(v),numpy.mean(v),numpy.min(v),numpy.std(v),numpy.var(v),
            numpy.percentile(v,95),numpy.percentile(v,99)))