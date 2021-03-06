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

WORK_DIR = "D:\DATA_lab\LAB_res_lar\Serv\L_0.75_uniform"
CLIENT_DIR = "D:\DATA_lab\LAB_res_lar\L_0.75_uniform"
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
            dict_item[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
        else:
            dict_item[row[4]] = {}
            dict_item[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
    return dict_item

def create_table(dict_item,f_out,label,type_ops=""):
    f_out.write("{}\n".format(label))
    f_out.write("{},FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n".format(type_ops))
    for k,v in sorted(dict_item.items()):
        if "sound.jpg" in k:
            kname = str(k).replace('sound.jpg','sound.mp3')
        # elif "word.pdf" in k:
        #     kname = str(k).replace('word.pdf','word.docx')
        else:
            kname = str(k)
        f_out.write("{},{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(type_ops,kname,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
            numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))

f_out = open(OUT_FILE,"w")
used_time = find_start_end_time()
print "START : {}\nEND : {}".format(used_time['start_time'],used_time['end_time'])
f_out.write("Unit,bytes : Mb/s\n")
f_out.write("START : {}\nEND : {}\n".format(used_time['start_time'],used_time['end_time']))

# 2 cate of files HDFS,DS ==> bytes_in, bytes_out, cpu_idle, mem_free, disk_free
work_file = glob.glob("{}/*.*".format(WORK_DIR))

for csv_file_data in work_file:
    if 'DS_bytes' not in csv_file_data:
        continue
    f = open(csv_file_data,"r")
    f_out.write("########################################################################\n")
    f_out.write("FILE : {}\n".format(csv_file_data))
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
    if "HDFS" in csv_file_data:
        f_out.write("LOG,HDFS_")
    elif "DS" in csv_file_data:
        f_out.write("LOG,WebService_")
    if "bytes_in" in csv_file_data:
        f_out.write("Bytes_IN (MB/s)\n")
    elif "bytes_out" in csv_file_data:
        f_out.write("Bytes_OUT (MB/s)\n")

    max_time = max(dict_table,key=dict_table.get)
    upper_max_time = max_time + timedelta(minutes=thres_time)
    lower_max_time = max_time - timedelta(minutes=thres_time)
    
    min_time = min(dict_table,key=dict_table.get)
    upper_min_time = min_time + timedelta(minutes=thres_time)
    lower_min_time = min_time - timedelta(minutes=thres_time)
    
    f_out.write("MAX,{},{:.3f}\n".format(max_time,dict_table[max_time]))
    f_out.write("MIN,{},{:.3f}\n".format(min_time,dict_table[min_time]))

    # At this point we get max time and min time and we need to see what client do in this time


    list_client_file = glob.glob("{}/*.*".format(CLIENT_DIR))
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=    

    dict_files_max_time = { 'write':{'type1':{},'type2':{},'type3':{}},
                            'read':{'type1':{},'type2':{},'type3':{}}}

    dict_files_min_time = { 'write':{'type1':{},'type2':{},'type3':{}},
                            'read':{'type1':{},'type2':{},'type3':{}}}
    
    for csv_file in list_client_file:
        f = open(csv_file,"r")
        #f_out.write("LOG,{}\n".format(csv_file.split("\\")[-1]))
        n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
        reader= csv.reader(f,delimiter=',')
        for row in reader:
            if row[2] == 'INFO':
                time_finish = datetime.strptime("{}.{}".format(row[0],row[1]),"%Y-%m-%d %H:%M:%S.%f")
                time_start = time_finish - timedelta(seconds=float(row[6].split(":")[1]))
                ops = str(row[3])

            # max time period
           
            if row[2] == 'INFO' and (ops == 'write' or ops == 'read'):
                if time_start < lower_max_time <= time_finish <= upper_max_time :
                    # type1 = item start ops before lower_max_time and finish in range of considering time
                    dict_files_max_time[ops]['type1'] = add_item(dict_item=dict_files_max_time[ops]['type1'],row=row,csv_file=csv_file)
                elif lower_max_time <= time_start <= upper_max_time:
                    #print time_start,time_finish
                    # type2 = item start ops in range of considering time
                    dict_files_max_time[ops]['type2'] = add_item(dict_item=dict_files_max_time[ops]['type2'],row=row,csv_file=csv_file)
                elif upper_max_time <= time_start <= upper_max_time + timedelta(minutes=thres_time):
                    # type3 = item start ops after upper_max_time and continue thres_time
                    dict_files_max_time[ops]['type3'] = add_item(dict_item=dict_files_max_time[ops]['type3'],row=row,csv_file=csv_file)

            # min time period
            
            # if row[2] == 'INFO' and (ops == 'write' or ops == 'read'):
            #     if time_start < lower_min_time <= time_finish <= upper_min_time :
            #         # type1 = item start ops before lower_min_time and finish in range of considering time
            #         dict_files_min_time[ops]['type1'] = add_item(dict_item=dict_files_min_time[ops]['type1'],row=row,csv_file=csv_file)
            #     elif lower_min_time <= time_start <= upper_min_time:
            #         #print time_start,time_finish
            #         # type2 = item start ops in range of considering time
            #         dict_files_min_time[ops]['type2'] = add_item(dict_item=dict_files_min_time[ops]['type2'],row=row,csv_file=csv_file)
            #     elif upper_min_time <= time_start <= upper_min_time + timedelta(minutes=thres_time):
            #         # type3 = item start ops after upper_min_time and continue thres_time
            #         dict_files_min_time[ops]['type3'] = add_item(dict_item=dict_files_min_time[ops]['type3'],row=row,csv_file=csv_file)

        f.close()
    
    # print "lens of all write {}\n lens of all read {}\n".format(len(dict_files_max_time),len(dict_files_read_all_max_time))
    
    ####### MAX TIME #######
    
    #WRITE
    
    if "bytes_in" in csv_file_data:
        f_out.write("#### Client to DSePHR PEAK#### \n")
        f_out.write("LOG: At MAX time ALL Client {} +- {} minutes\n".format(max_time,thres_time))
        create_table(dict_item=dict_files_max_time['write']['type1'],f_out=f_out,
                    label="TYPE 1: time_start < lower_time <= time_finish <= upper_time ",
                    type_ops = "write"
                    )
        create_table(dict_item=dict_files_max_time['write']['type2'],f_out=f_out,
                    label="TYPE 2: lower_time <= time_start <= upper_time",
                    type_ops = "write"
                    )
        create_table(dict_item=dict_files_max_time['write']['type3'],f_out=f_out,
                    label="TYPE 3: upper_time <= time_start <= upper_time + thres_time (1 Min)",
                    type_ops = "write"
                    )
    if "bytes_out" in csv_file_data:
        f_out.write("#### DSePHR to Storage PEAK #### \n")
    
    # READ
    if "bytes_in" in csv_file_data:
        f_out.write("FILE : {}\n".format(csv_file_data))
        f_out.write("#### Client to DSePHR PEAK#### \n")

    create_table(dict_item=dict_files_max_time['read']['type1'],f_out=f_out,
                label="TYPE 1: time_start < lower_time <= time_finish <= upper_time ",
                type_ops = "read"
                )
    create_table(dict_item=dict_files_max_time['read']['type2'],f_out=f_out,
                label="TYPE 2: lower_time <= time_start <= upper_time",
                type_ops = "read"
                )
    create_table(dict_item=dict_files_max_time['read']['type3'],f_out=f_out,
                label="TYPE 3: upper_time <= time_start <= upper_time + thres_time (1 Min)",
                type_ops = "read"
                )
    
    ####### MIN TIME #######
    # f_out.write("\nLOG: At MIN time ALL Client {} +- {} minutes\n".format(min_time,thres_time))
    # # WRITE
    # create_table(dict_item=dict_files_min_time['write']['type1'],f_out=f_out,
    #             label="TYPE 1: time_start < lower_time <= time_finish <= upper_time ",
    #             type_ops = "write"
    #             )
    # create_table(dict_item=dict_files_min_time['write']['type2'],f_out=f_out,
    #             label="TYPE 2: lower_time <= time_start <= upper_time",
    #             type_ops = "write"
    #             )
    # create_table(dict_item=dict_files_min_time['write']['type3'],f_out=f_out,
    #             label="TYPE 3: upper_time <= time_start <= upper_time + thres_time",
    #             type_ops = "write"
    #             )
    # # READ
    # create_table(dict_item=dict_files_min_time['read']['type1'],f_out=f_out,
    #             label="TYPE 1: time_start < lower_time <= time_finish <= upper_time ",
    #             type_ops = "read"
    #             )
    # create_table(dict_item=dict_files_min_time['read']['type2'],f_out=f_out,
    #             label="TYPE 2: lower_time <= time_start <= upper_time",
    #             type_ops = "read"
    #             )
    # create_table(dict_item=dict_files_min_time['read']['type3'],f_out=f_out,
    #             label="TYPE 3: upper_time <= time_start <= upper_time + thres_time",
    #             type_ops = "read"
    #             )