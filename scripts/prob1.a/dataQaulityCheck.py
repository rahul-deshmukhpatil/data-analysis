#!/usr/bin/python

# Global imports
import os
import sys
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

# check_dq_of_ts of time series
#   find the count of nan values
#   no of records whos values fall behind threshold of 
#   +ve and -ve side of (5 * standard deviation)
#   Plot the graph
###############################################################
def check_dq_of_ts(ts, index):
    stddev=ts.std()
    mean=ts.mean()

    # minimum and maximum std deviation
    # values of you this min and max range are outliers
    min_std= mean-stddev
    max_std = mean+stddev

    lower_breaks = ts.where(ts < (min_std*5)).count()
    upper_breaks = ts.where(ts > (max_std*5)).count() 

    nan_count = ts.isnull().sum()
    bad_records = nan_count + lower_breaks + upper_breaks;

    total_records = ts.count() + nan_count;
    prct_bad = (bad_records * 100) / total_records
    with print_lock:
        print "ts: %d, mean: %f, stddev : %f, total_records %d, bad_prct %f, bad_records %d ( nan_count %d + lower_breaks %d + upper_breaks %d)" %(index, mean, stddev, total_records, prct_bad, bad_records, nan_count, lower_breaks, upper_breaks)

    #plot the incrimental graph
    plt.xlabel('Time')
    plt.ylabel('Incrimental Difference Summation')
    plt.title('Incrimental Difference Summation: Time series ' +  `index`)
    plt.plot(ts.cumsum())
    fig='images/ts' + `index` + '-incremental.png'
    plt.savefig(fig)
    plt.close()

    # Plot the difference graph 
    stddevstr='stddev : ' + str(float(stddev))
    #f, ax = plt.subplots(1,1)
    #plt.text(0.05, 0.95, s=stddevstr, ha='left', va='center', transform = ax.transAxes)
    plt.xlabel('Time')
    plt.ylabel('Values(Difference recorded)')
    plt.title('Distribution of Events Values : Time series ' +  `index` + ',' + stddevstr)
    plt.plot(ts.replace(to_replace='NaN', value=stddev*5), 'bo')
    fig='images/ts' + `index` + '-dataQaulity.png'
    plt.savefig(fig)
    plt.close()

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
    
# Main 
###############################################################
def main_iterator():
    #get the data into data frame
    #df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0, nrows=10000)
    df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0)

    if (multi_threaded):
        threads = [] 
        for i in df.columns:
             t = Thread(target=check_dq_of_ts, args=(df[[i]], i))
             threads.append(t)
        
        # Start all threads
        for x in threads:
            x.start()
        
        # Join all threads
        for x in threads:
            x.join()
    else:
        # for each time series calculate the data quality
        for i in df.columns:
            check_dq_of_ts(df[[i]], i);

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
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:m", ["help", "file=", "mt"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    global dataLarge
    global multi_threaded
    dataLarge = ''
    multi_threaded = False

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
        if(dataLarge == ''):
            eprint("Please provide valid data file as -f argument on command line")
        else:
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
