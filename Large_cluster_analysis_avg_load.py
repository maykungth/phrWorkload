
CSV_DIR = "D:\DATA_lab\LAB_res_lar\L_0.5_real"

f_out = open("Large_avg_load_{}.csv".format(CSV_DIR.split("\\")[-1]),"w")
import csv,glob
import numpy
from datetime import datetime
from datetime import timedelta
start_seq = 500
num_seq = 5000

def get_uid_sysid(rowkey):
    return "{}-{}".format(rowkey.split("-")[0],rowkey.split("-")[1])
    
dict_files_write_all = {}
dict_files_read_all = {}

listfile = glob.glob("{}/*.*".format(CSV_DIR))
cur_all= 0
sum_file_size = {'write':0.0,'read':0.0}
sum_time = {'write':0.0,'read':0.0}
sum_num = {'write':0,'read':0}

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
    flag = 0
    for idx, row in enumerate(reader):
        if len(row) != 9 and start_seq <= cur <= num_seq + start_seq:
            print row

        if len(row) == 9:       # determine INFO row
            # skip to start_seq (500)
            # if cur == start_seq:
            #     print row
            if cur <= start_seq:
                cur += 1
                continue
            if cur > num_seq + start_seq:
                print row[8]
                print "-----"
                break
            if flag == 0:
                print row[8]
                flag = 1
            if row[2] == 'INFO' and row[3] == 'write':
                ### for dict_files_write_all ###
                if row[4] in dict_files_write_all:
                    dict_files_write_all[row[4]]["{}_{}:{}".format(n_client,row[0].split(' ')[1],row[1])]= float(row[6].split(":")[1])
                else:
                    dict_files_write_all[row[4]] = {}
                    dict_files_write_all[row[4]]["{}_{}:{}".format(n_client,row[0].split(' ')[1],row[1])]= float(row[6].split(":")[1])
                
                sum_num['write'] += 1
                sum_time['write'] += float(row[6].split(":")[1])
                sum_file_size['write'] += float(row[7].split(":")[1])/1024

            if row[2] == 'INFO' and row[3] == 'read':

                ### for dict_files_read_all ###
                if row[4] in dict_files_read_all:
                    dict_files_read_all[row[4]]["{}_{}:{}".format(n_client,row[0].split(' ')[1],row[1])]= float(row[6].split(":")[1])
                else:
                    dict_files_read_all[row[4]] = {}
                    dict_files_read_all[row[4]]["{}_{}:{}".format(n_client,row[0].split(' ')[1],row[1])]= float(row[6].split(":")[1])
                
                sum_num['read'] += 1
                sum_time['read'] += float(row[6].split(":")[1])
                sum_file_size['read'] += float(row[7].split(":")[1])/1024

            # count sequence
            cur += 1

text_write={1:'',2:'',3:'',4:''}
text_read={1:'',2:'',3:'',4:''}
f_out.write("LOG:, ALL Files {}\n".format(CSV_DIR.split("\\")[-1]))
f_out.write("write,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX_start,#MAX_finish\n")
# print "lens of all write {}\n lens of all read {}\n".format(len(dict_files_write_all),len(dict_files_read_all))
for k,v in sorted(dict_files_write_all.items()):
    if "sound.jpg" in k:
        kname = str(k).replace('sound.jpg','sound.mp3')
    else:
        kname = str(k)
    max_tmp = str(max(v,key=v.get))
    time_start = datetime.strptime(max_tmp.split('_')[1],"%H:%M:%S:%f") - timedelta(seconds=v[max_tmp])
    time_start = max_tmp.split('_')[0] + "_" + time_start.strftime("%H:%M:%S:") + str(int(time_start.strftime("%f"))/1000)
    txt_tmp = ",{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(kname,len(v),v[max_tmp],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),time_start,max_tmp)
    if any(x in kname for x in ['2mri','4ccd','8excel']):
        text_write[1] += 'very_small' + txt_tmp
    elif any(x in kname for x in ['1ecg','5ccd','6ccd','7word','9sound']):
        text_write[2] += 'small' + txt_tmp
    elif any(x in kname for x in ['10sound','3xray']):
        text_write[3] += 'moderate' + txt_tmp
    elif any(x in kname for x in ['11videosmall','12videobig']):
        text_write[4] += 'large' + txt_tmp
for i in xrange(1,5):
    f_out.write(text_write[i])

f_out.write("read,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX_start,#MAX_finish\n")
for k,v in sorted(dict_files_read_all.items()):
    if "sound.jpg" in k:
        kname = str(k).replace('sound.jpg','sound.mp3')
    else:
        kname = str(k)
    max_tmp = str(max(v,key=v.get))
    time_start = datetime.strptime(max_tmp.split('_')[1],"%H:%M:%S:%f") - timedelta(seconds=v[max_tmp])
    time_start = max_tmp.split('_')[0] + "_" + time_start.strftime("%H:%M:%S:") + str(int(time_start.strftime("%f"))/1000)
    txt_tmp = ",{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(kname,len(v),v[max_tmp],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),time_start,max_tmp)
    if any(x in kname for x in ['2mri','4ccd','8excel']):
        text_read[1] += 'very_small' + txt_tmp
    elif any(x in kname for x in ['1ecg','5ccd','6ccd','7word','9sound']):
        text_read[2] += 'small' + txt_tmp
    elif any(x in kname for x in ['10sound','3xray']):
        text_read[3] += 'moderate' + txt_tmp
    elif any(x in kname for x in ['11videosmall','12videobig']):
        text_read[4] += 'large' + txt_tmp
for i in xrange(1,5):
    f_out.write(text_read[i])

f_out.write("######### AVG WRITE LOAD ########\n")
f_out.write("Write,N_files,T_time (s),T_size (MB)\n")
f_out.write("#,{},{},{}\n".format(sum_num['write'],sum_time['write'],sum_file_size['write']))
f_out.write("AVG,T_size/N_files: {:.3f} MB\n".format(sum_file_size['write']/sum_num['write']))
f_out.write("AVG,T_size/T_time: {:.3f} MB/s\n".format(sum_file_size['write']/sum_time['write']))

if sum_num['read'] > 0:
    f_out.write("######### AVG READ LOAD ########\n")
    f_out.write("Read,N_files,T_time (s),T_size (MB)\n")
    f_out.write("#,{},{},{}\n".format(sum_num['read'],sum_time['read'],sum_file_size['read']))
    f_out.write("AVG,T_size/N_files: {:.3f} MB\n".format(sum_file_size['read']/sum_num['read']))
    f_out.write("AVG,T_size/T_time: {:.3f} MB/s\n".format(sum_file_size['read']/sum_time['read']))
