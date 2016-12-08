# This script to find few things 
# - a number of files in HDFS at CONSIDER_TIME
# - the volume size of queue that persist in DSePHR webservice 

import csv,glob
from datetime import datetime
from datetime import timedelta

CSV_DIR = "C:\Users\mayku\Documents\LAB_small\LAB_small_ex7"
CSV_GANGLIA = "C:\Users\mayku\Documents\LAB_small\L1.0_uniform_disk_free_DS1-DS6.csv"
START_TIME = datetime.strptime("2016-10-26 12:08:00","%Y-%m-%d %H:%M:%S")   #determine start time in ""
#END_TIME = datetime.strptime("2016-10-26 14:14:00","%Y-%m-%d %H:%M:%S")    #determine end time in ""
CONSIDER_TIME = datetime.strptime("2016-10-26 13:02:00","%Y-%m-%d %H:%M:%S")
CSV_OUT = "Total_files_HDFS.csv"
# start_seq = 500
# num_seq = 5000
def in_range_time(s_time,e_time,c_time):
    if s_time <= c_time <= e_time :
        return True
    else:
        return False
def is_info_write(row):
    if len(row) != 9:
        return False
    if row[2] == 'INFO' and row[3] == 'write':
        return True
    else:
        return False 
def get_time_of_row(row,ganglia=False):
    if ganglia==False:
        # 2016-10-22 20:00:27
        return datetime.strptime(str(row[0]),"%Y-%m-%d %H:%M:%S")
    else:
        # 2016-10-26T12:09:00+07:00
        return datetime.strptime(str(row[0]),"%Y-%m-%dT%H:%M:%S+07:00")


# This part to get actual size from upload_log.csv that Client upload to DSePHR webservice
sum_size_all = []
n_files_all = []
cur_time_all = []
listfile = glob.glob("{}/*.*".format(CSV_DIR))
f_out = open(CSV_OUT,"w")
f_out.write("{}\n{}\n".format(CSV_DIR,CSV_GANGLIA))
f_out.write("\nThe data from clients at the time adding server \n")
f_out.write("###,last_time,n_files,total_size(mb)\n")
for idx, csv_file in enumerate(listfile):
    f = open(csv_file,"r")
    reader=list(csv.reader(f,delimiter=','))
    sum_size_all.append(0)
    n_files_all.append(0)
    cur_time_all.append(START_TIME)
    for irow in reader:
        cur_time = get_time_of_row(irow)
        if cur_time > CONSIDER_TIME:
            #f_out.write("{},{},{},{}\n".format(csv_file.split('\\')[-1],cur_time,n_files,sum_size_all[idx]/1024))
            break
        if is_info_write(irow) and in_range_time(START_TIME,CONSIDER_TIME,cur_time):
            sum_size_all[idx] += int(irow[7].split(":")[1])
            n_files_all[idx] += 1
            cur_time_all[idx] = cur_time
    f_out.write("{},{},{},{}\n".format(csv_file.split('\\')[-1],cur_time_all[idx],n_files_all[idx],sum_size_all[idx]/1024))
#f_out.write("{},{},{},{}\n".format(csv_file.split('\\')[-1],cur_time,n_files,sum_size_all[idx]/1024))

f_out.write("All clients uploaded size(Mb),,, {}\n".format(sum(sum_size_all)/1024))

# This part to get HDFS size from GANGLIA FILE

f_gang = open(CSV_GANGLIA,"r")
reader=list(csv.reader(f_gang,delimiter=','))
has_max_row = False
for idx, row in enumerate(reader):
    # Skip first row
    # Timestamp,LAB_Rserver1,LAB_Rserver2,LAB_Rserver3,LAB_Rserver4,LAB_Rserver5,LAB_Rserver6
    # 2016-10-26T12:09:00+07:00,468.3288,468.25273333,468.39126667,468.549,467.95,468.552
    if idx == 0:
        first_row = row
        f_out.write("Time,{}\n".format("(GB),".join(first_row[1:])))
        continue
    elif has_max_row == False:
        if (row[1] or row[2] or row[3]) == "NaN":
            print "{} {} {}".format(idx,row[1],row[2])
            continue
        else:
            max_free_row = row
            has_max_row = True

    time_row = get_time_of_row(row,ganglia=True)
    #print time_row
    if time_row >= CONSIDER_TIME:
        f_out.write("{},{}\n".format(get_time_of_row(max_free_row,ganglia=True),",".join(max_free_row[1:])))

        f_out.write("{},{}\n".format(time_row,",".join(row[1:])))

        total_use_gb = [float(max_free_row[i+1])-float(v) for i,v in enumerate(row[1:])]
        f_out.write("Used (GB),{}\n".format(",".join(format(x,".3f") for x in total_use_gb)))
        f_out.write("Total Use(GB),{}\n".format(sum(total_use_gb)))
        f_out.write("Total Use(GB)/3 replicas,{}\n".format(sum(total_use_gb)/3))
        break



