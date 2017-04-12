# process data before and after adding serv 30 minutes
import csv,glob
from datetime import datetime
from datetime import timedelta
from Client_func import *
import collections
import numpy

CSV_FILE = "D:\DATA_lab\LAB_small\Serv_small\L_0.5_dis\HDFS_disk_free.csv"
CLIENT_DIR =CSV_FILE.replace('Serv_small\\','').rsplit('\\',1)[0]

OUT_FILE = "Small_percent_time_{}.csv".format(CSV_FILE.split("\\")[-2])

list_ADD_time = {'1.0_uniform':datetime.strptime("2016-10-26 13:02:00","%Y-%m-%d %H:%M:%S"),
                '1.0_dis':datetime.strptime("2016-10-26 15:30:00","%Y-%m-%d %H:%M:%S"),
                '0.75_uniform':datetime.strptime("2016-10-25 14:16:00","%Y-%m-%d %H:%M:%S"),
                '0.75_dis':datetime.strptime("2016-10-25 16:27:00","%Y-%m-%d %H:%M:%S"),
                '0.5_uniform':datetime.strptime("2016-10-25 23:54:00","%Y-%m-%d %H:%M:%S"),
                '0.5_dis':datetime.strptime("2016-10-26 02:25:00","%Y-%m-%d %H:%M:%S")
}
for k in list_ADD_time:
    if k in CSV_FILE:
        ADD_SERV_TIME = list_ADD_time[k]
print "ADD_SERV_TIME: {}".format(ADD_SERV_TIME)
MAX_CAPA = {}
NON_DFS_USED = 21.5 + 2.5
MAX_HDD = 60
thres_time = 1 # minute


f_out = open(OUT_FILE,"w")
#before_time = {'start_time':ADD_SERV_TIME-timedelta(minutes=31),'end_time':ADD_SERV_TIME-timedelta(minutes=1)}
#after_time = {'start_time':ADD_SERV_TIME+timedelta(minutes=1),'end_time':ADD_SERV_TIME+timedelta(minutes=31)}

f_out.write("ADD_SERV_TIME: {}\n".format(ADD_SERV_TIME))

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
#work_file = glob.glob("{}/*.*".format(WORK_DIR))

f = open(CSV_FILE,"r")
f_out.write("FILE : {}\n".format(CSV_FILE))
reader = csv.DictReader(f,delimiter=',')

# Timestamp,LAB_Rserver1,LAB_Rserver2,LAB_Rserver3,LAB_Rserver4,LAB_Rserver5,LAB_Rserver6
# 2016-10-26T12:09:00+07:00,468.3288,468.25273333,468.39126667,468.549,467.95,468.552

# Collect the data to table row
table_all = collections.OrderedDict()
for row in reader:
    time = get_time_of_row(row)
    for k in row:
        if 'Time' not in k:
            if k in table_all:
                table_all[k][time] = float(row[k])
            else:
                table_all[k] = collections.OrderedDict()
                table_all[k][time] = float(row[k])

# Find maximum remaining on each machine
for Rserver in table_all:
    MAX_CAPA[Rserver] = max(table_all[Rserver].values())
    min_hdd = min(table_all[Rserver].values())
    #print "{} {} {} {}".format(Rserver,MAX_CAPA[Rserver],min_hdd,MAX_CAPA[Rserver]-min_hdd)
# Adjust the remaining hdd to disk usage
for Rserver in table_all:
    for k,v in table_all[Rserver].iteritems():
        # %remaining  = MAX_CAPA[Rserver] - v
        table_all[Rserver][k] = (MAX_CAPA[Rserver] - v + NON_DFS_USED)/MAX_HDD*100
        # check percent
        # if table_all[Rserver][k] <0:
        #     print "HDD CAPACITY WRONG"
        #     print Rserver, k, table_all[Rserver][k]

# Find 50%, 75%, 80%, 85%, 90%, 95%
def add_measure_time(time_measure,time_to_add):
    if time_measure == None:
        time_measure = time_to_add
    elif time_measure > time_to_add:
        time_measure = time_to_add
    return time_measure

# for Rserver in table_all:
#     print Rserver, table_all[Rserver][ADD_SERV_TIME]
time_measure = collections.OrderedDict()
time_measure = {50:None,75:None,80:None,85:None,90:None,95:None}
for Rserver in table_all:
    if Rserver in ['LAB_Rserver4','LAB_Rserver5','LAB_Rserver6']:
        continue
    for k in time_measure:
        for time in table_all[Rserver]:
            if abs(float(k)-table_all[Rserver][time])<=1.1:
                #print table_all[Rserver][time], time_measure[k]
                time_measure[k] = add_measure_time(time_measure[k],time)
time_measure[95] = ADD_SERV_TIME
for k,v in sorted(time_measure.iteritems()):
    print "Remain:{}% {}".format(k,v)

# Listing Client file
dict_files_measure_time = { 'write':{50:{},75:{},80:{},85:{},90:{},95:{}},
                            'read':{50:{},75:{},80:{},85:{},90:{},95:{}}}

list_client_file = glob.glob("{}/*.*".format(CLIENT_DIR))

for per in time_measure:
    for csv_file in list_client_file:
            f = open(csv_file,"r")
            #f_out.write("LOG,{}\n".format(csv_file.split("\\")[-1]))
            n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
            reader= csv.reader(f,delimiter=',')
            for row in reader:
                if len(row)==9:
                    time_finish = datetime.strptime("{}.{}".format(row[0],row[1]),"%Y-%m-%d %H:%M:%S.%f")
                    time_start = time_finish - timedelta(seconds=float(row[6].split(":")[1]))
                    ops = str(row[3])
                
                    #if ops == 'write' or ops == 'read':
                    lower_time = time_measure[per] - timedelta(minutes=thres_time)
                    upper_time = time_measure[per] + timedelta(minutes=thres_time)
                    if lower_time <= time_start <= upper_time:
                        # type1 = item in peak time
                        dict_files_measure_time[ops][per] = add_item(dict_item=dict_files_measure_time[ops][per],row=row,csv_file=csv_file)
            f.close()
    
# Get data of 50%, 75%, 80%, 85%, 90%, 95% of each file
file_measure = {'8excel.xlsx':[],'6ccd.pdf':[],'10sound.jpg':[],'12videobig.mp4':[]}
for per in sorted(time_measure):
    f_out.write("###,AT Used {}%, Time: {}\n".format(per,time_measure[per]))
    create_table(dict_item=dict_files_measure_time['write'][per],f_out=f_out,
                        label="Write OPS at Used {}%".format(per),
                        type_ops = "write"
                        )
    create_table(dict_item=dict_files_measure_time['read'][per],f_out=f_out,
                        label="Read OPS at Used {}%".format(per),
                        type_ops = "read"
                        )
    for file_name in dict_files_measure_time['read'][per]:
        if file_name in file_measure:
            file_measure[file_name].append(numpy.mean(dict_files_measure_time['read'][per][file_name].values()))
# y_axis_data = {'Type1':[3,4,5,6,5,3],'Type2':[5,5,6,5,4,6],'Type3':[1,2,3,5,4,6],'Type4':[6,5,4,3,2,3]}
print file_measure
gen_graph(name_graph=CSV_FILE.rsplit('\\',2)[1],y_axis_data=file_measure)