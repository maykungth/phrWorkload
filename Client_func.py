import csv,glob
from datetime import datetime
from datetime import timedelta
import collections
import numpy
from matplotlib import pyplot as plt

def gen_graph(name_graph,y_axis_data,show=False):
    x_axis = [50,75,80,85,90,95]
    fig, ax = plt.subplots(figsize=(11,9))
    for lab in y_axis_data:
        if 'sound.jpg' in lab:
            label = lab.replace('sound.jpg','sound.mp3')
        else:
            label = lab
        line, = ax.plot(x_axis, y_axis_data[lab],label=label)
        for xy in zip(x_axis, y_axis_data[lab]):                                
            ax.annotate("{:.2f}".format(xy[1]),xy=xy, textcoords='data')
    plt.xlabel('Left %')
    plt.ylabel('Time (s)')
    plt.title("{}".format(name_graph))
    #plt.axis([50,95,0,100])
    #ax.legend(loc='lower right')
    plt.legend(ncol=4, mode="expand",bbox_to_anchor=(0., -0.15, 1., .102))
    
    if show == False:
        plt.savefig("Graph_Retrieval_{}.png".format(name_graph))
    else:
        plt.show()
    
def get_time_of_row(row):
    # 2016-10-26T12:09:00+07:00
    return datetime.strptime(str(row['Timestamp']),"%Y-%m-%dT%H:%M:%S+07:00")
def create_table(dict_item,f_out,label,type_ops=""):
    f_out.write("{}\n".format(label))
    f_out.write("{},FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th\n".format(type_ops))
    for k,v in sorted(dict_item.items()):
        if "sound.jpg" in k:
            kname = str(k).replace('sound.jpg','sound.mp3')
        else:
            kname = str(k)
        f_out.write("{},{},{},{},{:.3f},{},{:.3f},{:.3f},{:.3f},{:.3f}\n".format(type_ops,kname,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
            numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99)))
def add_item(dict_item, row,csv_file):
    n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
    if row[2] == 'INFO':
        if row[4] in dict_item:
            dict_item[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
        else:
            dict_item[row[4]] = {}
            dict_item[row[4]]["{}_{}".format(n_client,row[8])]= float(row[6].split(":")[1])
    return dict_item



