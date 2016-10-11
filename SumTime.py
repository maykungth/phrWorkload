from datetime import datetime
import csv

# Setup param
start_time = "2016-09-15 00:00:00,000"
end_time = "2016-09-16 00:00:00,000"
CSV_LOG = "/home/hduser/workspace/phr_client/upload_log.csv"
# End setup Param

start_time = datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S,%f")
end_time = datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S,%f")

f = open(CSV_LOG,"r")
reader=list(csv.reader(f,delimiter=','))
# Pattern
# 2016-09-15 11:20:57,099,INFO,read,1ecg.jpg,u99-s6-1473757835-3b1f6895-4983-427e-9ee3-1d7f9ad4932d,sec:0.247,size:394,#11
# 2016-09-15 11:20:57,194,INFO,write,9sound.ogg,u4144-s5-1473913257-dde7af40-e2de-493e-9f6e-0ae90020d7af,sec:0.095,size:154,#12

sum_read = {'size':0,'total':0,'time':0}
sum_write = {'size':0,'total':0,'time':0}
print "Sumarize data {} to {}".format(start_time,end_time)
for row in reader:
    current_time = datetime.strptime("{},{}".format(row[0],row[1]),"%Y-%m-%d %H:%M:%S,%f")
    if current_time > start_time :
        if row[3] == 'write' : 
            sum_write['size'] += int(row[7].split(':')[1])
            sum_write['total'] += 1
            sum_write['time'] += float(row[6].split(':')[1])
        elif row[3] == 'read' :
            sum_read['size'] += int(row[7].split(':')[1])
            sum_read['total'] += 1
            sum_read['time'] += float(row[6].split(':')[1])
    if current_time > end_time :
        break
print "=== write ==="
print "Total : {}\nSize : {:.2f} MB\nTime : {} sec".format(sum_write['total'],sum_write['size']/1024,sum_write['time'])
print "Avg : {:.2f} MB/s".format((sum_write['size']/1024)/sum_write['time'])
print "=== read ==="
print "Total : {}\nSize : {:.2f} MB\nTime : {} sec".format(sum_read['total'],sum_read['size']/1024,sum_read['time'])
print "Avg : {:.2f} MB/s".format((sum_read['size']/1024)/sum_read['time'])