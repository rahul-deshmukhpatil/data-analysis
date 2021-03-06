#!/usr/bin/python

# Global imports
import os
import sys
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import getopt
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
    length = ts.count() 
    sum = ts.sum() 
    square_sum = (ts * ts).sum() 
    with sum_lock:
		total_sum[index] += sum 
		total_square_sum[index] += square_sum 
		total_samples[index] += length
    #eprint("total_sum %f, total_square_sum %f" %(total_sum[index], total_square_sum[index]))

# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" %s -f <data file> [-m]" %(argv[0]))
    eprint(" eg %s -f dataLarge")
    eprint(" -f --file  : data file containing time series with 0th coloumn as index")
    eprint(" -r --rows  : count of rows to load at a time from time series")
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
    
    #get the data into data frame
    #df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0)

    total_rows = num_records(dataLarge)
    rows_read = 0
    
    if(num_rows == 0):
        num_rows = total_rows

    while(rows_read < total_rows): 
        # skip the first records_read and read only nrows
        df = pd.read_csv(dataLarge, engine='python', sep=' ', header=None, index_col=0, skiprows=rows_read, nrows=num_rows)

        # for each time series calculate the data quality
        for i in df.columns:
            calculate_stats(df[[i]], i);

        rows_read += num_rows
    
    #Now calculte the mean and std_deviation with summerizing all above computed values
    for i in df.columns:
        cal_mean_std(i)

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
        opts, args = getopt.getopt(sys.argv[1:], "hf:r:m", ["help", "file=", "rows=", "mt"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

   
    global num_rows
    global dataLarge

    num_rows = 0
    dataLarge = 'None'

    global total_sum
    global total_square_sum
    global total_samples

    total_sum = [0.0] * 26
    total_square_sum = [0.0] * 26
    total_samples = [0.0] * 26

    for o, a in opts:
        if o in ("-f", "--file"):
            dataLarge = a
        elif o in ("-r", "--rows"):
            num_rows = int(a)
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

    main_iterator()

# Ivoke the main
################################################
main()

