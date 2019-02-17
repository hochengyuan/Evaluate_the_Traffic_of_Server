from __future__ import print_function
from argparse import ArgumentParser
import os
import pandas as pd
from datetime import datetime
import operator
import numpy as np

def mkSubFile(lines,head,srcName,sub):
    # writelines to the new csv file
    [des_filename, extname] = os.path.splitext(srcName)
    filename  = des_filename + '_' + str(sub) + extname
    print( 'make file: %s' %filename)
    fout = open(filename,'w')
    try:
        fout.writelines([head])
        fout.writelines(lines)
        return (sub + 1 , filename)
    finally:
        fout.close()

def splitByLineCount(filename,count):
    # split source csv file to chunks
    file_list = []
    fin = open(filename,'r')
    try:
        head = fin.readline()
        buf = []
        sub = 1
        for line in fin:
            buf.append(line)
            if len(buf) == count:
                sub_file = mkSubFile(buf,head,filename,sub)
                sub = sub_file[0]
                buf = []
                file_list.append(sub_file[1])
        if len(buf) != 0:
            sub_file = mkSubFile(buf,head,filename,sub)
            sub = sub_file[0]
            file_list.append(sub_file[1])
    finally:
        fin.close()
    
    return file_list

def list_subfile(filePath):
    # output the list of all subfiles 
    file_list = []
    file_size = os.path.getsize(filePath) # bytes
    if file_size >= 1073741824: # more than 1GB -> split
        file_list += splitByLineCount(filePath , 100000)
    else: # no file splitted
        file_list = [filePath]
    return file_list

def initialize_df(filePath):
    # initialize the dataframe of a csvfile, including mapping start timestamp of each connection
    df = pd.read_csv(filePath).dropna()
    df["endTs_datetime"] = pd.to_datetime(df['endTs'])
    df["startTs_datetime"] = pd.to_datetime(df['endTs_datetime']) - pd.to_timedelta(df['timeTaken'], unit='s')
    
    return df

def isActive(timestamp , df):
    # check the specified timestamp is in the time range
    if type(timestamp) == str:
        return df[(df["endTs_datetime"] > datetime.strptime(timestamp , "%Y-%m-%d %H:%M:%S.%f")) &\
         (datetime.strptime(timestamp , "%Y-%m-%d %H:%M:%S.%f") >= df["startTs_datetime"])]
    return df[(df["endTs_datetime"] > timestamp) & (timestamp >= df["startTs_datetime"])]

def number_of_active(timestamp , dataframe):
    # return number of active connection when specified timestamp
    return len(isActive(timestamp , dataframe))

def dict_timestamp_active(input_list , dataframe): 
    # return dictionary of timestamp and number of open connection
    output_dict = {}
    for timestamp in input_list:
        output_dict[timestamp] = number_of_active(timestamp , dataframe)
    return output_dict

def form_all_timeframe(list_subfile_all):
    # form up time_frame combining all sub_files
    first_df = initialize_df(list_subfile_all[0])
    min_start = min(first_df["startTs_datetime"])
    max_end = max(first_df["endTs_datetime"])

    if len(list_subfile_all) == 1:
        return pd.date_range(start=min_start , end=max_end , closed='left' , freq='s')
    else:
        for subfile in list_subfile_all[1:]:
            cur_df = initialize_df(subfile)
            cur_start = min(cur_df["startTs_datetime"])
            cur_end = max(cur_df["endTs_datetime"])
            min_start = min(min_start , cur_start)
            max_end = max(max_end , cur_end)
    return pd.date_range(start=min_start , end=max_end , closed='left' , freq='s')

def statistics_dataframe(dataframe , all_timestamp):
    # generate statistic dataframe for insert log file
    stat_df = pd.DataFrame({"Timestamp" : all_timestamp})
    stat_df["Volume"] = stat_df["Timestamp"].apply(lambda x : number_of_active(x , dataframe))
    return stat_df

def combine_dicts(a, b, op=operator.add):
    # union join two dictionary, and aggregate values with the common keys(timestamp)
    return dict(list(a.items()) + list(b.items()) + [(k, op(a[k], b[k])) for k in set(b) & set(a)])

def generate_query_result(list_subfile_all , input_list):
    # generate dictionary {timestamp : number of open connections} for query
    all_df = pd.DataFrame(columns=["ip" , "endTs" , "timeTaken" , "endTs_datetime" , "startTs_datetime"])
    output_dict = {}
    for subfile in list_subfile_all:
        cur_df = initialize_df(subfile)
        cur_dict = dict_timestamp_active(input_list , cur_df)    
        output_dict = combine_dicts(output_dict , cur_dict , op=operator.add)

    return output_dict

def generate_statistics(list_subfile_all):
    # generate statistic result for all subfiles
    timeframe = form_all_timeframe(list_subfile_all)
    all_stats_df = pd.DataFrame(columns=['Timestamp','Volume'])
    all_stats_df['Timestamp'] = timeframe
    all_stats_df['Volume'] = all_stats_df['Volume'].fillna(0)
    all_stats_df = all_stats_df.set_index('Timestamp')
    for subfile in list_subfile_all:
        cur_df = initialize_df(subfile)
        cur_stats_df = statistics_dataframe(cur_df , timeframe).set_index('Timestamp')
        all_stats_df += cur_stats_df

    return all_stats_df

def generate_timestamp_highest_volume(stats_df):
    # return timestamp with highest volume
    ts_highest_volume_df = stats_df[stats_df["Volume"] == max(stats_df["Volume"])]
    return pd.DataFrame(ts_highest_volume_df.index.tolist() , columns=['Timestamp'])
    
def generate_max_volume(stats_df):
    # return the maximum volume
    return max(stats_df["Volume"])
    
def generate_min_volume(stats_df):    
    # return the minimum volume
    return min(stats_df["Volume"])

def generate_average_volume(stats_df):
    # return the average volume
    return np.mean(stats_df["Volume"])
    


parser = ArgumentParser()
parser.add_argument('-f','--filePath', help=' Set flag' , required=True)
parser.add_argument('-l','--list', action='append', help=' Set flag', required=True) # add "-l" before adding a timestamp
args = parser.parse_args()  
file_size = os.path.getsize(args.filePath)
if file_size >= 1073741824:
    print('SPLIT THE BIG FILE TO CHUNKS')
else:
    print('NO NEED TO SPLIT THE FILE TO CHUNKS')

list_subfile_all = list_subfile(args.filePath)

print("Size of File:" , file_size , "byte(s)")
print("List of Timestamp: {}".format(args.list))
print("ALL File Path:" , list_subfile_all)

# Required
print("---Required Function---")
print("Open Connection:" , generate_query_result(list_subfile_all , args.list))


statistics = generate_statistics(list_subfile_all)
timestamp_highest_volume = generate_timestamp_highest_volume(statistics)
max_volume = generate_max_volume(statistics)
min_volume = generate_min_volume(statistics)
average_volume = generate_average_volume(statistics)

print('\n')
print("---Optional Function: Statisitcs of Log File---")
print('Profiling of' , args.filePath , '\n')
print("timestamp_highest_volume")
print(timestamp_highest_volume,'\n')
print("max_volume of Open Connection within a second:" , max_volume)
print("min_volume of Open Connection within a second:" , min_volume)
print("average_volume of Open Connection within a second:" , average_volume)