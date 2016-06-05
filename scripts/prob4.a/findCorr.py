#!/usr/bin/python

# Global imports
import os
import sys
import math
import matplotlib
matplotlib.use('Agg')
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

# Main iterator over each series
###############################################################
def main_iterator():
    global num_rows
    global revised_values

    records_in_file = num_records(dataLarge)
    parts_size = records_in_file / parts

    skip_rows = 0
    part_index = 1

    while(part_index <= parts):

        #df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0, usecols=[0, index1, index2], skiprows=2500000, nrows=2500000)
        #eprint("Read rows till %d. skip rows: %d , parts_size %d " %(skip_rows + parts_size, skip_rows, parts_size))
        df = pd.read_csv(dataLarge, sep=' ', engine='python', header=None, index_col=0, usecols=[0, index1, index2], skiprows=skip_rows, nrows=parts_size)
        df = df.replace(to_replace='NaN', value =0.0).cumsum()
        total_rows = df.shape[0]
        
        rindex1 = []
        rindex2 = []

        i = 0
        rows_read = 0
        while(rows_read < total_rows):
            read_till = rows_read + aggr_records
            if(read_till > total_rows):
                read_till = total_rows
            #eprint("i %d, read_till %d" %(i, read_till))
            #rindex1[i] = df[[index1]].iloc[rows_read:read_till].sum();
            #rindex2[i] = df[[index2]].iloc[rows_read:read_till].sum();
            rindex1.append(df[[index1]].iloc[read_till-1])
            rindex2.append(df[[index2]].iloc[read_till-1])
            #eprint("%d] %f : %f" %(i, rindex1[i], rindex2[i]))
            i += 1
            rows_read += aggr_records

        #Now calculte the rho  and p-value by spearman ranking for correlation
        rho, pvalue = st.spearmanr(rindex1, rindex2)
        print "For series %d and %d, part %d spearman rho: %f" %(index1, index2, part_index, rho)

        #plot the graph per part
        #plot the incrimental graph
        image_name='images/TS' + `index1` + '-' + 'TS' + `index2` + '-Part-' + `part_index` + '.png'
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        plt.xlabel('Time')
        plt.ylabel('Incrimental Difference Summation')
        plt.title('TS' +  `index1` + ' Vs' + ' TS' + `index2` + ' [ Part' + `part_index` + '] rho(corr coef):' + `rho`)
        plt.plot(rindex1)
        plt.plot(rindex2)
        eprint("Fig: %s" %(image_name))
        plt.savefig(image_name)
        plt.close()

        skip_rows += parts_size
        part_index += 1

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

