from datetime import datetime
from datetime import timedelta
import csv,glob
import numpy
from matplotlib import pyplot as plt

# Setup param
CSV_DIR = "C:\Users\mayku\Documents\LAB_res_lar\L_0.5_dis"
CSV_OUTPUT = "table_summarize.csv"
CSV_CONCLUS = "conclus_all.csv"
CSV_FILE_SIZE = "file_size_analyze.csv"
start_seq = 500
num_seq = 5000
# End setup param

def get_uid_sysid(rowkey):
    return "{}-{}".format(rowkey.split("-")[0],rowkey.split("-")[1])
def add_to_table(row, table):
    uid_sysid = get_uid_sysid(row[5])
    if  uid_sysid in table:
        # add accumulate 
        table[uid_sysid]['n_file'] += 1
        table[uid_sysid]['total_size'] += int(row[7].split(':')[1])
    else:
        # add a new one
        table[uid_sysid] = {'n_file':1,'total_size':int(row[7].split(':')[1])}
    return table
def gen_graph(name_graph,list_sum):
    len_sum=len(list_sum)
    plt.figure(num=None, figsize=(20, 3), dpi=300)
    plt.bar(xrange(len_sum),numpy.log10(list_sum))
    plt.axis([0,5000,0,6])
    plt.xlabel('Sequences')
    plt.ylabel('log10(size)')
    plt.title("File size #{}".format(name_graph))
    plt.savefig("{}.png".format(name_graph))





table_sum_all = {}
col = {'n_file':None}
f_out = open(CSV_OUTPUT,"w")
f_all = open(CSV_CONCLUS,"w")
f_size = open(CSV_FILE_SIZE,"a")
f_out.write("###,CSV_DIR,{}\n".format(CSV_DIR))
f_all.write("###,CSV_DIR,{}\n".format(CSV_DIR))
f_all.write("###,Per User,n_file,t_size(kb),avg_size(kb)\n")
f_size.write("###,CSV_DIR,{}\n".format(CSV_DIR))
f_size.write("###,NUM,MAX(kb),MIN(kb),AVG(kb),99th,95th,VAR,STD\n")
listfile = glob.glob("{}/*.*".format(CSV_DIR))

for csv_file in listfile:
    f = open(csv_file,"r")
    f_out.write("###,CSV_FILE,{}\n".format(csv_file.split('\\')[-1]))
    #f_all.write("###,CSV_FILE,{}\n".format(csv_file.split('\\')[-1]))
    
    reader=list(csv.reader(f,delimiter=','))
    cur=0
    table_sum = {}  # reset table_sum
    list_size = []  # reset list_size
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
                table_sum = add_to_table(row,table_sum)
                table_sum_all = add_to_table(row,table_sum_all)
                list_size.append(int(row[7].split(':')[1]))
            cur += 1 # count the complete operation
    # loop for csv_file in listfile:  (for each files in DIR)
    num=1
    f_out.write("num,uid_sysid,n_file,total_size(kb),avg_size(kb)\n")
    all_col = {'n_file':[],'total_size':[],'avg_size':[]}
    for key,ir in table_sum.iteritems():
        avg_row= float(ir['total_size']/ir['n_file'])
        # write sequence for each user
        f_out.write("{},{},{},{},{}\n".format(num,key,ir['n_file'],ir['total_size'],avg_row))
        all_col['n_file'].append(ir['n_file'])
        all_col['total_size'].append(ir['total_size'])
        all_col['avg_size'].append(avg_row)
        num += 1
    f_out.write("###,end of file\n")
    # write to conclusion file for each client
    
    # Header
    # f_all.write("###,Per User,n_file,t_size(kb),avg_size(kb)\n")
    f_all.write("###,CSV_FILE,{},,,USR: {}\n".format(csv_file.split('\\')[-1],len(table_sum)))

    f_all.write("###,AVG,{},{},{}\n".format(numpy.mean(all_col['n_file']),numpy.mean(all_col['total_size']),numpy.mean(all_col['avg_size'])))
    f_all.write("###,MAX,{},{},{}\n".format(numpy.max(all_col['n_file']),numpy.max(all_col['total_size']),numpy.max(all_col['avg_size'])))
    f_all.write("###,MIN,{},{},{}\n".format(numpy.min(all_col['n_file']),numpy.min(all_col['total_size']),numpy.min(all_col['avg_size'])))
    f_all.write("###,SUM,{},{},{}\n".format(numpy.sum(all_col['n_file']),numpy.sum(all_col['total_size']),numpy.sum(all_col['avg_size'])))
    #f_all.write("###,N_USR,{}\n".format(len(table_sum)))

    # write conclusion for file in DIR to conclusion file
    # f_size.write("###(name),NUM,MAX,MIN,AVG,99th,95th,VAR,STD\n")
    f_size.write("{},{},{},{},{},{},{},{},{}\n".format(csv_file.split('\\')[-1],len(list_size),numpy.max(list_size),numpy.min(list_size),numpy.mean(list_size),
    numpy.percentile(list_size,99),numpy.percentile(list_size,95),numpy.var(list_size),numpy.std(list_size)))

    # draw a graph for each file (each client)

    # print ("Generating Graph for {}".format(csv_file.split('\\')[-1]))
    gen_graph("{},{}".format(csv_file.split('\\')[-2],csv_file.split('\\')[-1]),list_size)


# FOR ALL FILES TO CSV_CONCLUS
all_col = {'n_file':[],'total_size':[],'avg_size':[]}
for key,ir in table_sum_all.iteritems():
    avg_row= float(ir['total_size']/ir['n_file'])
    #f_out.write("{},{},{},{},{}\n".format(num,key,ir['n_file'],ir['total_size'],avg_row))
    all_col['n_file'].append(ir['n_file'])
    all_col['total_size'].append(ir['total_size'])
    all_col['avg_size'].append(avg_row)

f_all.write("###,all clients,n_file(kb),t_size(kb),avg_size(kb)\n")
f_all.write("###,AVG,{},{},{}\n".format(numpy.mean(all_col['n_file']),numpy.mean(all_col['total_size']),numpy.mean(all_col['avg_size'])))
f_all.write("###,MAX,{},{},{}\n".format(numpy.max(all_col['n_file']),numpy.max(all_col['total_size']),numpy.max(all_col['avg_size'])))
f_all.write("###,MIN,{},{},{}\n".format(numpy.min(all_col['n_file']),numpy.min(all_col['total_size']),numpy.min(all_col['avg_size'])))
f_all.write("###,SUM,{},{},{}\n".format(numpy.sum(all_col['n_file']),numpy.sum(all_col['total_size']),numpy.sum(all_col['avg_size'])))
f_all.write("###,N_USR,{}\n".format(len(table_sum_all)))



    
        
