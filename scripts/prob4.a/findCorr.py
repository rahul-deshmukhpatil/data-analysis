#!/usr/bin/python

# Global imports
import os
import sys
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as st
import getopt
from threading import Thread
from threading import Lock 

from sys import argv
from os.path import exists

#local imports
sys.path.insert(0, '../common')
from error_print import eprint

# calculate the mean and std deviation 
###############################################################
def cal_mean_std(index):
    mean = 0.0
    stddev = 0.0

    with sum_lock:
		mean = total_sum[index] / total_samples[index]
		variance_num = total_square_sum[index] + (total_samples[index]*(mean * mean)) - (2 * mean * total_sum[index])
		variance = variance_num / (total_samples[index] - 1)
		stddev = variance ** 0.5
    
    #eprint("ts: %d, total_sum %f, total_samples %d, total_square_sum %f" %(index, total_sum[index], total_samples[index], variance_num))

    with print_lock:
        print "ts: %d, mean: %f, stddev: %f" %(index, mean, stddev)

# Function definitions
###############################################################
def calculate_stats(ts, index):
    length = ts.shape[0] 
    for i in range(0, length):
        if(not np.isnan(float(ts.iloc[i]))):
            total_sum[index] += ts.iloc[i] 
            total_square_sum[index] += (ts.iloc[i] * ts.iloc[i])
    total_samples[index] += ts.count()
    #eprint("total_sum %f, total_square_sum %f" %(total_sum[index], total_square_sum[index]))

# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" %s -f <data file> [-m] -s 2,3" %(argv[0]))
    eprint(" eg %s -f dataLarge")
    eprint(" -f --file  : data file containing time series with 0th coloumn as index")
    eprint(" -m --mt    : run in multithreaded mode")
    eprint(" -a --aggr  : num of records to aggr and represent as single record, default is 100000")
    eprint(" -r --rows  : count of rows to load at a time from time series")
    eprint(" -s --series : , saperated two series indices to compare ")
    eprint(" -h --help  : help")
    eprint(" ****************************************************************************")
    
# No of records  
###############################################################
def num_records(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# Main iterator over each series
###############################################################
def main_iterator():
    global num_rows
    global revised_values
   
    #df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0, usecols=[0, index1, index2], skiprows=2500000, nrows=2500000)
    df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0, usecols=[0, index1, index2])
    total_rows = df.shape[0]
    rindex1 = [0.0] * ((total_rows/aggr_records) + 1)
    rindex2 = [0.0] * ((total_rows/aggr_records) + 1)
    df = df.replace(to_replace='NaN', value =0.0).cumsum()
    
    i = 0
    rows_read = 0
    while(rows_read < total_rows):
        # Read only frame within the df
        read_till = rows_read + aggr_records
        if(read_till > total_rows):
            read_till = total_rows

        #eprint("i %d, read_till %d" %(i, read_till))
        #rindex1[i] = df[[index1]].iloc[rows_read:read_till].sum();
        #rindex2[i] = df[[index2]].iloc[rows_read:read_till].sum();
        rindex1[i] = df[[index1]].iloc[read_till-1]
        rindex2[i] = df[[index2]].iloc[read_till-1]
        eprint(" %f : %f" %(rindex1[i], rindex2[i]))
        i += 1
        rows_read += aggr_records

    #Now calculte the rho  and p-value by spearman ranking for correlation
    rho, pvalue = st.spearmanr(rindex1, rindex2)
    print "For index %d and %d, spearman rho: %f, pvalue %f" %(index1, index2, rho, pvalue)

# main function: 
# initialize locks and global variables
# get the command line options
# check if input file exists
# start the main iterator function
######################################################################
def main():
    # to synchronize the output
    global print_lock
    print_lock = Lock()

    # to synchronize the summations
    global sum_lock
    sum_lock = Lock()

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ha:f:r:s:m", ["help", "aggr=", "file=", "rows=", "series=", "mt"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

   
    global num_rows
    global dataLarge
    global multi_threaded
    global index1
    global index2
    global aggr_records 

    num_rows = 0
    dataLarge = 'None'
    multi_threaded = False
    index1 = 0
    index2 = 0
    aggr_records = 100000 

    for o, a in opts:
        if o in ("-a", "--aggr"):
            aggr_records = int(a)
        elif o in ("-m", "--mt"):
            multi_threaded = True
        elif o in ("-f", "--file"):
            dataLarge = a
        elif o in ("-r", "--rows"):
            num_rows = int(a)
        elif o in ("-s", "--series"):
            index1, index2 = a.split(',')
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"

    #read the data file location
    absPath= os.getcwd()  + '/' + dataLarge

    #see if file exists and then proceed
    if not exists(dataLarge):
        eprint("Data file does not exists aborting : %s" %(dataLarge))
        eprint("abs path : %s" %(absPath))
        usage()
        sys.exit()
    else:
        eprint("Processing data file : %s " %(dataLarge))

    if not os.path.exists('images'):
        os.makedirs('images')

    index1 = int(index1)
    index2 = int(index2)
    if(index1 == 0 or index2 == 0):
        eprint("Please provide correct indices of time series to comapre with option -s");
        usage()
        sys.exit()

    main_iterator()

# Ivoke the main
################################################
main()

