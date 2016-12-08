# Client ---> Time to per file
CSV_DIR = "C:\Users\mayku\Documents\LAB_res_lar\L_1.0_uniform"
f_out = open("Con_time_1.0_uniform.csv","w")
import csv,glob
import numpy
start_seq = 500
num_seq = 5000

def get_uid_sysid(rowkey):
    return "{}-{}".format(rowkey.split("-")[0],rowkey.split("-")[1])

dict_files_write_all = {}
dict_files_read_all = {}

listfile = glob.glob("{}/*.*".format(CSV_DIR))
cur_all= 0
for csv_file in listfile:
    f = open(csv_file,"r")
    reader=list(csv.reader(f,delimiter=','))
    cur=0
    table_sum = {}  # reset table_sum
    dict_files_write = {}  # reset list_size
    dict_files_read = {}
    n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=    
    for idx, row in enumerate(reader):
        if len(row) == 9:       # determine INFO row
            # skip to start_seq (500)
            # if cur == start_seq:
            #     print row
            if cur <= start_seq:
                cur += 1
                continue
            if cur > num_seq + start_seq:
                #print row
                break
            if row[2] == 'INFO' and row[3] == 'write':
                if row[4] in dict_files_write:
                    dict_files_write[row[4]][row[8]]= float(row[6].split(":")[1])
                else:
                    dict_files_write[row[4]] = {}
                    dict_files_write[row[4]][row[8]]= float(row[6].split(":")[1])

                ### for dict_files_write_all ###
                if row[4] in dict_files_write_all:
                    dict_files_write_all[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
                else:
                    dict_files_write_all[row[4]] = {}
                    dict_files_write_all[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])

            if row[2] == 'INFO' and row[3] == 'read':
                if row[4] in dict_files_read:
                    dict_files_read[row[4]][row[8]]= float(row[6].split(":")[1])
                else:
                    dict_files_read[row[4]] = {}
                    dict_files_read[row[4]][row[8]]= float(row[6].split(":")[1])

                ### for dict_files_read_all ###
                if row[4] in dict_files_read_all:
                    dict_files_read_all[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
                else:
                    dict_files_read_all[row[4]] = {}
                    dict_files_read_all[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
            
            # count sequence
            cur += 1
    # for each csv_file
    #print dict_files['3xray.png']
    f_out.write("LOG: {}\{}\n".format(csv_file.split("\\")[-2],csv_file.split("\\")[-1]))
    f_out.write("write,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n")
    for k,v in dict_files_write.items():
        f_out.write("write,{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))
    
    f_out.write("read,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n")
    for k,v in dict_files_read.items():
        f_out.write("read,{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))
 
    f_out.write("-----END of FILE------\n")

f_out.write("LOG: ALL files in DIR\n")
f_out.write("write,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n")
# print "lens of all write {}\n lens of all read {}\n".format(len(dict_files_write_all),len(dict_files_read_all))
for k,v in dict_files_write_all.items():
        f_out.write("write,{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))

f_out.write("read,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n")
for k,v in dict_files_read_all.items():
        f_out.write("read,{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))
