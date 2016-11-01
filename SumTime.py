from datetime import datetime
from datetime import timedelta
import csv,glob

# Setup param
#start_time = "2016-10-18 00:00:00,000"
#end_time = "2016-10-25 00:00:00,000"
CSV_DIR = "D:\LAB_result_0.5_uniform"
#CSV_LOG = "upload_log_client5.csv"
start_seq = 500
num_seq = 5000
category={
    '0kb':{'low':0,'high':100},  # 0-100 kb
    '100kb':{'low':101,'high':1000},  # 100 kb - 1 MB
    '1mb':{'low':1001,'high':10000},  # 1 MB  - 10 MB
    '10mb':{'low':10001,'high':999999}  # 10 MB 
}
#start_time = datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S,%f")
#end_time = datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S,%f")
# Respond time, Download time, upload time
# sperate by size category, client1-8, for each 1000 files
# End setup Param
def reset_sum():
    return {'0kb':{'size':0,'total':0,'time':0.0,'res':0.0},
            '100kb':{'size':0,'total':0,'time':0.0,'res':0.0},
            '1mb':{'size':0,'total':0,'time':0.0,'res':0.0},
            '10mb':{'size':0,'total':0,'time':0.0,'res':0.0}}
def cate_size(size):
    size = int(size)
    if category['0kb']['low'] <= size <= category['0kb']['high']:
        return '0kb'
    elif category['100kb']['low'] <= size <= category['100kb']['high']:
        return '100kb'
    elif category['1mb']['low'] <= size <= category['1mb']['high']:
        return '1mb'
    elif size >= category['10mb']['low']:
        return '10mb'
def calc_res_time(idx, cur_row,prev_row):  
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=
    cur_time = datetime.strptime("{},{}".format(cur_row[0],cur_row[1]),"%Y-%m-%d %H:%M:%S,%f")
    prev_time = datetime.strptime("{},{}".format(prev_row[0],prev_row[1]),"%Y-%m-%d %H:%M:%S,%f")
    str_time = cur_row[6].rsplit(':')[1]
    op_time = timedelta(seconds=int(str_time.rsplit('.')[0]),microseconds=int(str_time.rsplit('.')[1])*1000)
    if prev_row[2] == 'INFO' or prev_row[2] == 'ERROR':
        diff_time = cur_time - prev_time - op_time #diff_time is in datetime.timedelta form
    elif prev_row[2] == 'WARNING':
        diff_time = cur_time - prev_time - timedelta(seconds=5) - op_time # del with time to download#
    if diff_time.days == -1:
        return 0.0
    else:
        return float("{}.{}".format(diff_time.seconds,diff_time.microseconds/1000))

listfile = glob.glob("{}/*.*".format(CSV_DIR))
for csv_file in listfile:
    f = open(csv_file,"r")
    print "===== {} =====".format(csv_file)
    reader=list(csv.reader(f,delimiter=','))
    sum_read = reset_sum()
    sum_write = reset_sum()
    cur=0
    
    # 2016-10-22 20:00:27,142,WARNING,{u'Message': u'File not found'}
    # =========0=========,=1=,=2=====,===============3===============
    # 2016-10-22 20:00:32,995,INFO,write,3xray.png,u1093-s2-1477141232-13640f45-2b20-40e4-b2ce-93199117e0bd,sec:0.847,size:4099,#69
    # =========0=========,=1=,=2==,==3==,====4====,=====================5==================================,===6=====,====7====,=8=    
    for idx, row in enumerate(reader):
        if len(row) == 9:
            # skip to start_seq (500)
            if cur <= start_seq:
                cur += 1
                continue
            if cur > 5000 + start_seq:
                break
            cur += 1 # count the complete operation
            #print cur
            cate = cate_size(row[7].rsplit(':')[1])   # determine size
            res_time = calc_res_time(idx,row,reader[idx-1]) # send index, current row and previous row to calc
            if res_time > 5.0:
                print row
                print reader[idx-1]
                print res_time
                break
            if row[2] == 'INFO' and row[3] == 'write':
                sum_write[cate]['time'] += float(row[6].rsplit(':')[1])
                sum_write[cate]['res'] += res_time
                sum_write[cate]['size'] += int(row[7].rsplit(':')[1])
                sum_write[cate]['total'] += 1
            elif row[2] == 'INFO' and row[3] == 'read':
                sum_read[cate]['time'] += float(row[6].rsplit(':')[1])
                sum_read[cate]['res'] += res_time
                sum_read[cate]['size'] += int(row[7].rsplit(':')[1])
                sum_read[cate]['total'] += 1
    # print the result for each file
    sum_wr = 0
    print "=== Sum Write of {} ===".format(csv_file)
    for idx in sum_write:
        print idx
        print sum_write[idx]
        print "Avg upload {} mb/s".format(float(sum_write[idx] ['size']/1000/sum_write[idx]['time']))
        print "Avg Respond Time {} sec".format(float(sum_write[idx]['res']/sum_write[idx]['total']))
        sum_wr += sum_write[idx]['total']
    print "=== Sum Read of {} ===".format(csv_file)
    for idx in sum_read:
        print idx
        print sum_read[idx]
        print "Avg upload {} mb/s".format(float(sum_read[idx] ['size']/1000/sum_read[idx]['time']))
        print "Avg Respond Time {} sec".format(float(sum_read[idx]['res']/sum_read[idx]['total']))
        sum_wr += sum_read[idx]['total']
    print "Total : {}".format(sum_wr)
    # see only first result
    #break

# Pattern
# 2016-10-22 19:54:43,009,INFO,write,10sound.jpg,u4769-s3-1477140883-e780c1ab-7879-475a-9a07-ecf6909664bc,sec:0.886,size:8851,#50
# 2016-10-22 19:55:05,542,INFO,write,12videobig.mp4,u969-s0-1477140905-191d09b1-ce7b-492f-8bdd-67984c6a0df3,sec:22.533,size:237981,#51
# 2016-10-22 19:55:06,077,INFO,write,3xray.png,u1897-s2-1477140906-041a869e-c109-4bd3-9601-042e30fb8752,sec:0.534,size:4099,#52
# 2016-10-22 19:55:09,241,INFO,write,11videosmall.mp4,u4116-s4-1477140909-1cf2736a-3cad-41c4-8ff5-31b4335bf249,sec:3.164,size:27725,#53
# 2016-10-22 19:55:09,274,WARNING,{u'Message': u'File not found'}
# 2016-10-22 19:55:14,298,WARNING,{u'Message': u'File not found'}
# 2016-10-22 19:55:19,358,WARNING,{u'Message': u'File not found'}
# 2016-10-22 19:55:38,370,INFO,read,11videosmall.mp4,u4116-s4-1477140909-1cf2736a-3cad-41c4-8ff5-31b4335bf249,sec:14.006,size:27725,#54
# 2016-10-22 19:55:38,427,INFO,write,1ecg.jpg,u1960-s8-1477140938-050c97a0-477a-408c-9fdf-e1354d8bb858,sec:0.052,size:394,#55
# 2016-10-22 19:55:38,504,INFO,write,2mri.jpg,u1134-s2-1477140938-d86e8c79-f925-452b-8ef5-e3b5254b94b9,sec:0.076,size:20,#56
# 2016-10-22 19:55:41,739,INFO,write,11videosmall.mp4,u4803-s4-1477140941-f0182573-c97c-4434-861b-300236d7cff8,sec:3.235,size:27725,#57
# 2016-10-22 19:55:41,767,INFO,read,4ccd.xml,u4879-s2-1477140707-b1da4ea3-c911-45e8-a210-669d2deeb8c7,sec:0.028,size:27,#58
# 2016-10-22 19:55:41,768,ERROR,Cannot find 8excel.xlsx to download,#59
# 2016-10-22 19:55:41,768,ERROR,Cannot find 8excel.xlsx to download,#60
# 2016-10-22 19:55:41,785,WARNING,{u'Message': u'File not found'}
# 2016-10-22 19:58:19,557,INFO,read,12videobig.mp4,u3966-s9-1477140879-876bf267-861d-4ea0-8c75-7bfdc336fd0a,sec:152.767,size:237981,#61
# 2016-10-22 19:58:19,672,INFO,read,4ccd.xml,u4647-s0-1477140882-05ce90f0-2d0a-4ed4-acc9-fffe9c6a2bd0,sec:0.088,size:27,#62

# current_time = datetime.strptime("{},{}".format(row[0],row[1]),"%Y-%m-%d %H:%M:%S,%f")