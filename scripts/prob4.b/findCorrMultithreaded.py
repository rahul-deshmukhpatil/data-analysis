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

# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" For correlation between series 2 and 3. Do analysis in 4 equal parts")
    eprint(" %s -f <data file> [-m] -s 2,3 -p 4" %(argv[0]))
    eprint(" eg %s -f dataLarge")
    eprint(" -f --file  : data file containing time series with 0th coloumn as index")
    eprint(" -a --aggr  : num of records to aggr and represent as single record, default is 100000")
    eprint(" -s --series : , saperated two series indices to compare ")
    eprint(" -p --parts : No of equal parts for which to calculate the correlation")
    eprint(" -h --help  : help")
    eprint(" ****************************************************************************")
    
# No of records  
###############################################################
def num_records(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# Find the parts co-relation 
###############################################################
def find_corr(part_index, skip_rows, parts_size):
    #eprint("Read rows till %d. skip rows: %d , parts_size %d " %(skip_rows + parts_size, skip_rows, parts_size))
    df = pd.read_csv(dataLarge, sep=' ', engine='python', header=None, index_col=0, usecols=[0, index1, index2], skiprows=skip_rows, nrows=parts_size)
    df = df.replace(to_replace='NaN', value =0.0).cumsum()
    total_rows = df.shape[0]
    rindex1 = [0.0] * ((total_rows/aggr_records) + 1)
    rindex2 = [0.0] * ((total_rows/aggr_records) + 1)
    
    i = 0
    rows_read = 0
    while(rows_read < total_rows):
        read_till = rows_read + aggr_records
        if(read_till > total_rows):
            read_till = total_rows
        rindex1[i] = df[[index1]].iloc[read_till-1]
        rindex2[i] = df[[index2]].iloc[read_till-1]
        #eprint("%d] %f : %f" %(i, rindex1[i], rindex2[i]))
        i += 1
        rows_read += aggr_records

    #free the memory 
    del df

    #Now calculte the rho  and p-value by spearman ranking for correlation
    rho, pvalue = st.spearmanr(rindex1, rindex2)
    result_rho[part_index] = rho
    #print "For index %d and %d, part %d spearman rho: %f, pvalue %f" %(index1, index2, part_index, rho, pvalue)


# Main iterator over each series
###############################################################
def main_iterator():
    #global array to collect the results
    global result_rho
    result_rho = [0.0] * parts    

    records_in_file = num_records(dataLarge)
    parts_size = records_in_file / parts

    skip_rows = 0
    part_index = 0

    threads = []
    while(part_index < parts):
        t = Thread(target=find_corr, args=(part_index, skip_rows, parts_size))
        threads.append(t)
        skip_rows += parts_size
        part_index += 1

    # Start all threads
    for x in threads:
        x.start()
    
    # Join all threads
    for x in threads:
        x.join()

    #now print the results
    for i in range(0,parts):
        print "For index %d and %d, part %d spearman rho: %f" %(index1, index2, i, result_rho[i])
    

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
        opts, args = getopt.getopt(sys.argv[1:], "ha:f:s:p:", ["help", "aggr=", "file=", "series=", "parts="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

   
    global dataLarge
    global index1
    global index2
    global aggr_records 
    global parts

    dataLarge = 'None'
    multi_threaded = False
    index1 = 0
    index2 = 0
    aggr_records = 100000 
    parts = 1

    for o, a in opts:
        if o in ("-a", "--aggr"):
            aggr_records = int(a)
        elif o in ("-f", "--file"):
            dataLarge = a
        elif o in ("-p", "--parts"):
            parts = int(a)
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

