#!/usr/bin/python

# Global imports
import os
import sys
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
def cal_mean_std(ts):
    samples = 0
    samples_sum = 0
    square_sum = 0
    with sum_lock:
        samples = ts.count()
        samples_sum = ts.sum()
        square_sum = (ts * ts).sum()

    mean = samples_sum / samples
    variance = 0.0
    variance = (samples * mean * mean) + square_sum  - (2 * samples_sum * mean ) 

    variance /= (samples - 1)
    stddev = variance ** (0.5) 
    return mean, stddev 

# Function definitions
###############################################################
def calculate_stats(ts, index):
    mean, stddev = cal_mean_std(ts)

    with print_lock:
        print "ts: %d, mean: %f, stddev: %f" %(index, mean, stddev)

# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" %s -f <data file> [-m]" %(argv[0]))
    eprint(" eg %s -f dataLarge")
    eprint(" -f --file  : data file containing time series with 0th coloumn as index")
    eprint(" -m --mt    : run in multithreaded mode")
    eprint(" -h --help  : help")
    eprint(" ****************************************************************************")
    
# Main iterator over each series 
###############################################################
def main_iterator():
    #get the data into data frame
    df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0)
    if(multi_threaded):
        threads = [] 
        for i in df.columns:
            t = Thread(target=calculate_stats, args=(df[[i]], i))
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # Join all threads
        for t in threads:
            t.join()

    else:
        # for each time series calculate the data quality
        for i in df.columns:
            calculate_stats(df[[i]], i);

# main function: 
# initialize locks and global variables
# get the command line options
# check if input file exists
# start the main iterator function
######################################################################
def main():
		 
    # to synchronize the output
    global print_lock
    global sum_lock 
    sum_lock = Lock()
    print_lock = Lock()
    try:
            opts, args = getopt.getopt(sys.argv[1:], "hf:m", ["help", "file=", "mt"])
    except getopt.GetoptError as err:
            # print help information and exit:
            print str(err)  # will print something like "option -a not recognized"
            usage()
            sys.exit(2)

    global dataLarge
    global multi_threaded
    multi_threaded = False

    dataLarge = 'None'

    for o, a in opts:
            if o in ("-m", "--mt"):
                multi_threaded = True
            elif o in ("-f", "--file"):
                dataLarge = a
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
