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


# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" %s -f <data file> [-g]" %(argv[0]))
    eprint(" eg %s -f dataLarge")
    eprint(" -f --file  : data file containing time series with 0th coloumn as index")
    eprint(" -g --graph : plot the graphs, Needs addition time ")
    eprint(" -h --help  : help")
    eprint(" ****************************************************************************")
    
# Main 
###############################################################
def main_iterator():
    #get the data into data frame
    ts = pd.read_csv(dataLarge, engine='c', sep=' ', header=None, index_col=0)
    columns = ts.shape[1]

    # for each time series calculate the data quality
    for i in range(1, columns+1):
        check_dq_of_ts(ts[[i]], i);

    if(plot_graphs):
        for i in range(1, columns+1):
            stddev = ts[[i]].std()
            # Plot the difference graph 
            stddevstr='stddev : ' + str(float(stddev))
            #f, ax = plt.subplots(1,1)
            #plt.text(0.05, 0.95, s=stddevstr, ha='left', va='center', transform = ax.transAxes)
            plt.xlabel('Time')
            plt.ylabel('Values(Difference recorded)')
            plt.title('Distribution of Events Values : Time series ' +  `i` + ',' + stddevstr)
            ts[[i]] = ts[[i]].replace(to_replace='NaN', value=stddev*5)
            plt.plot(ts[[i]], 'bo')
            fig='images/ts' + `i` + '-dataQaulity.png'
            plt.savefig(fig)
            plt.close()

            #plot the incrimental graph
            plt.xlabel('Time')
            plt.ylabel('Incrimental Difference Summation')
            plt.title('Incrimental Difference Summation: Time series ' +  `i`)
            plt.plot(ts[[i]].cumsum())
            fig='images/ts' + `i` + '-incremental.png'
            plt.savefig(fig)
            plt.close()

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
        opts, args = getopt.getopt(sys.argv[1:], "hf:g", ["help", "file=", "graph"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    global dataLarge
    global plot_graphs
    dataLarge = ''
    plot_graphs = False

    for o, a in opts:
        if o in ("-g", "--graph"):
            plot_graphs = True
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
