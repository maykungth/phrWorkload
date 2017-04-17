import csv,glob
import pandas as pd
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
import numpy
from matplotlib import pyplot as plt
import itertools
from StringIO import StringIO
def gen_box_graph(dict_to_plot,filename):
    #dict_to_plot['write'][75]['12videobig.mp4']['client3_#123']
    d = OrderedDict({'AVG':pd.Series(dict_to_plot['AVG'][filename].values())})

    collabel = ['AVG']
    for per in dict_to_plot:
        # skip 50%
        if (per == 50) or per == 'AVG':
            continue
        collabel.append("{}%".format(per))
        d[per] = pd.Series(dict_to_plot[per][filename].values())

    df = pd.DataFrame(d)
    result_graph = df.plot.box(return_type='dict',showmeans=True,showfliers=False,widths=0.0,whis='range',whiskerprops={'linestyle':'-','linewidth':1}, capprops={'marker':'o'},grid=True)
    mean_data = [ "{:.2f}".format(result_graph['means'][i].get_ydata()[0])  for i in xrange(len(result_graph['means'])) ]
    min_data = [ "{:.2f}".format(result_graph['caps'][i].get_ydata()[0]) for i in xrange(0,len(result_graph['caps']),2) ]
    max_data = [ "{:.2f}".format(result_graph['caps'][i].get_ydata()[0]) for i in xrange(1,len(result_graph['caps']),2) ]

    plt.title("{}".format(filename))
    plt.ylim(0,140)
    plt.xlabel('% Resouce Usage')
    plt.ylabel('Time (s)')
    plt.subplots_adjust( bottom=0.25)
    cell_text = [max_data,mean_data,min_data]
    the_table = plt.table(cellText=cell_text,
                        rowLabels=['Max','Mean','Min'],
                        colLabels=collabel,
                        loc='bottom',bbox=[0.05,-0.32,0.95,0.2])
    
    plt.show()

def gen_graph(name_graph,y_axis_data,show=False):
    x_axis = [50,75,80,85,90,95]
    fig, ax = plt.subplots(figsize=(11,9))
    marker = itertools.cycle(('^', '+', '.', 'o', '*')) 
    for lab in y_axis_data:
        if 'sound.jpg' in lab:
            label = lab.replace('sound.jpg','sound.mp3')
        else:
            label = lab
        line, = ax.plot(x_axis, y_axis_data[lab],label=label,marker = marker.next(),markersize = 10)
        for xy in zip(x_axis, y_axis_data[lab]):                                
            ax.annotate("{:.2f}".format(xy[1]),xy=xy, textcoords='data')
    plt.xlabel('Used %')
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
def create_table(dict_item,f_out,label,type_ops="",list_file_show=None):
    f_out.write("{}\n".format(label))
    #f_out.write("{},FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,MAX_CLIENT_TIME\n".format(type_ops))
    text_csv ="{},FILE,NUM,MAX,AVG,MIN,STD,VAR,95th,99th,MAX_CLIENT_TIME,MIN_CLIENT_TIME\n".format(type_ops)
    for k,v in sorted(dict_item.items()):
        if k==None or k == {} or v == None or v =={}:
            continue
        if "sound.jpg" in k:
            kname = str(k).replace('sound.jpg','sound.mp3')
        else:
            kname = str(k)
        if list_file_show != None:
            if k in list_file_show:
                text_csv += "{},{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(type_ops,kname,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
                numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get))
                
        else:
            text_csv += "{},{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{},{}\n".format(type_ops,kname,len(v),v[max(v,key=v.get)],numpy.mean(v.values()),v[min(v,key=v.get)],
            numpy.std(v.values()),numpy.var(v.values()),numpy.percentile(v.values(),95),numpy.percentile(v.values(),99),max(v,key=v.get),min(v,key=v.get))
    f_out.write(text_csv)
    df = pd.read_csv(StringIO(text_csv),na_filter=False)
    return df.to_html(index=False)

def add_item(dict_item, row,csv_file):
    n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
    if row[2] == 'INFO':
        if row[4] in dict_item:
            dict_item[row[4]]["{}_{}_{}".format(n_client,row[8],row[0].split(' ')[1])]= float(row[6].split(":")[1])
        else:
            dict_item[row[4]] = {}
            dict_item[row[4]]["{}_{}_{}".format(n_client,row[8],row[0].split(' ')[1])]= float(row[6].split(":")[1])
    return dict_item

def add_item_not_group_name(dict_item, row,csv_file):
    n_client = csv_file.split('\\')[-1].split('_')[-1].split('.')[0]
    if row[2] == 'INFO':
        dict_item["{}_{}_{}".format(n_client,row[8],row[0].split(' ')[1])]= float(row[6].split(":")[1])
    return dict_item
def check_4type(file_name):
    if any(x in file_name for x in ['2mri','4ccd','8excel']):
        return 'very_small'
    elif any(x in file_name for x in ['1ecg','5ccd','6ccd','7word','9sound']):
        return 'small'
    elif any(x in file_name for x in ['10sound','3xray']):
        return 'moderate'
    elif any(x in file_name for x in ['11videosmall','12videobig']):
        return 'large'
def check_2type(file_name):
    if any(x in file_name for x in ['11videosmall','12videobig']):
        return 'HDFS'
    else:
        return 'HBase'



list_ADD_time = {'1.0_uniform':datetime.strptime("2016-10-26 13:02:00","%Y-%m-%d %H:%M:%S"),
                '1.0_dis':datetime.strptime("2016-10-26 15:30:00","%Y-%m-%d %H:%M:%S"),
                '0.75_uniform':datetime.strptime("2016-10-25 14:16:00","%Y-%m-%d %H:%M:%S"),
                '0.75_dis':datetime.strptime("2016-10-25 16:27:00","%Y-%m-%d %H:%M:%S"),
                '0.5_uniform':datetime.strptime("2016-10-25 23:54:00","%Y-%m-%d %H:%M:%S"),
                '0.5_dis':datetime.strptime("2016-10-26 02:25:00","%Y-%m-%d %H:%M:%S")
}

list_END_time = {'1.0_uniform':datetime.strptime("2016-10-26 14:13:00","%Y-%m-%d %H:%M:%S"),
                '1.0_dis':datetime.strptime("2016-10-26 16:52:00","%Y-%m-%d %H:%M:%S"),
                '0.75_uniform':datetime.strptime("2016-10-25 15:14:00","%Y-%m-%d %H:%M:%S"),
                '0.75_dis':datetime.strptime("2016-10-25 17:55:00","%Y-%m-%d %H:%M:%S"),
                '0.5_uniform':datetime.strptime("2016-10-26 00:54:00","%Y-%m-%d %H:%M:%S"),
                '0.5_dis':datetime.strptime("2016-10-26 04:07:00","%Y-%m-%d %H:%M:%S")
}
