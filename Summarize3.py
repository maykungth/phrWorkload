# This script to find few things 
# - a number of files in HDFS at CONSIDER_TIME
# - the volume size of queue that persist in DSePHR webservice 

import csv,glob
from datetime import datetime
from datetime import timedelta

CSV_DIR = "C:\Users\mayku\Documents\LAB_small\LAB_small_ex7"
CSV_GANGLIA = "C:\Users\mayku\Documents\LAB_small\L1.0_uniform_disk_free_R1-R6.csv"
START_TIME = datetime.strptime("2016-10-26 12:08:00","%Y-%m-%d %H:%M:%S")   #determine start time in ""
END_TIME = datetime.strptime("2016-10-26 14:14:00","%Y-%m-%d %H:%M:%S")    #determine end time in ""
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
listfile = glob.glob("{}/*.*".format(CSV_DIR))
f_out = open(CSV_OUT,"w")
f_out.write("{}\n{}\n".format(CSV_DIR,CSV_GANGLIA))
f_out.write("\nThe data from clients at the time adding server \n")
f_out.write("###,last_time,n_files,total_size(mb)\n")
for idx, csv_file in enumerate(listfile):
    f = open(csv_file,"r")
    reader=list(csv.reader(f,delimiter=','))
    sum_size_all.append(0)
    n_files = 0
    for irow in reader:
        cur_time = get_time_of_row(irow)
        if cur_time > CONSIDER_TIME:
            f_out.write("{},{},{},{}\n".format(csv_file.split('\\')[-1],cur_time,n_files,sum_size_all[idx]/1024))
            break
        if is_info_write(irow) and in_range_time(START_TIME,CONSIDER_TIME,cur_time):
            sum_size_all[idx] += int(irow[7].split(":")[1])
            n_files += 1
f_out.write("All clients uploaded size(Mb),,, {}\n".format(sum(sum_size_all)/1024))

# This part to get HDFS size from GANGLIA FILE

f_gang = open(CSV_GANGLIA,"r")
reader=list(csv.reader(f_gang,delimiter=','))
is_first_row = False
for idx, row in enumerate(reader):
    # Skip first row
    # Timestamp,LAB_Rserver1,LAB_Rserver2,LAB_Rserver3,LAB_Rserver4,LAB_Rserver5,LAB_Rserver6
    # 2016-10-26T12:09:00+07:00,468.3288,468.25273333,468.39126667,468.549,467.95,468.552
    if is_first_row == False:
        is_first_row = True
        first_row = row
        continue
    time_row = get_time_of_row(row,ganglia=True)
    print time_row
    if time_row > CONSIDER_TIME:
        f_out.write("Time,")
        f_out.write("(GB),".join(first_row[1:]))
        #f_out.write("\n")
        f_out.write("\n{},".format(time_row))
        f_out.write(",".join(row[1:]))
        f_out.write("\n")
        break



    # in while loop
    # cur_time += timedelta(seconds=60)

    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=   
    # cur=0 
    # for idx, row in enumerate(reader):
    #     if is_info(row):
    #         cur += 1
    #     if not in_range_row(cur):
    #         continue
    #     print row
    #     break
    


