# Client ---> webserv / per file
CSV_FILE = ["C:\Users\mayku\Documents\LAB_res_lar\L_1.0_uniform\upload_log_client2.csv",
            "C:\Users\mayku\Documents\LAB_res_lar\L_1.0_uniform\upload_log_client4.csv"
]
f_out = open("Table#2#4.csv","w")
import csv
import numpy
start_seq = 500
num_seq = 5000

def get_uid_sysid(rowkey):
    return "{}-{}".format(rowkey.split("-")[0],rowkey.split("-")[1])

for csv_file in CSV_FILE:
    f = open(csv_file,"r")
    reader=list(csv.reader(f,delimiter=','))
    cur=0
    table_sum = {}  # reset table_sum
    dict_files = {}  # reset list_size
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
                if row[4] in dict_files:
                    dict_files[row[4]][row[8]]= float(row[6].split(":")[1])
                else:
                    dict_files[row[4]] = {}
                    dict_files[row[4]][row[8]]= float(row[6].split(":")[1])
            cur += 1
    # for each csv_file
    #print dict_files['3xray.png']
    f_out.write("###,FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,#MAX,#MIN\n")
    f_out.write("LOG: {}\{}\n".format(csv_file.split("\\")[-2],csv_file.split("\\")[-1]))
    for k,v in dict_files.items():
        f_out.write("###,{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(k,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
        numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get)))
        # f_out.write("MAX: {:5} | {} sec\n".format(max(v,key=v.get),v[max(v,key=v.get)]))
        # f_out.write("MIN: {:5} | {} sec\n".format(min(v,key=v.get),v[min(v,key=v.get)]))
        # f_out.write("AVG: {:13.3f} sec\n".format(numpy.mean(v.values())))
        # f_out.write("STD: {:13.3f}\n".format(numpy.std(v.values())))
        # f_out.write("VAR: {:13.3f}\n".format(numpy.var(v.values())))
        # f_out.write("95th: {:12.3f}\n".format(numpy.percentile(v.values(),95)))
        # f_out.write("99th: {:12.3f}\n".format(numpy.percentile(v.values(),99)))
    f_out.write("-----END of FILE------\n")
