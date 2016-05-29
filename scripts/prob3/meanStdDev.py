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
    cumsum  = 0.0
    length = ts.count() + ts.isnull().sum() 
    for i in range(0, length):
        if(not np.isnan(float(ts.iloc[i]))):
            cumsum += ts.iloc[i] 
    
    mean = cumsum/ts.count()

    variance = 0.0
    for i in range(0, length):
        if(not np.isnan(float(ts.iloc[i]))):
           variance += (mean - ts.iloc[i]) * (mean - ts.iloc[i])
    
    variance /= (ts.count() -1)
    stddev = variance ** (0.5) 
    return mean, stddev 

# Function definitions
###############################################################
def check_dq_of_ts(ts, index):
    stddev = ts.std()
    mean = ts.mean()
    cmean, cstddev = cal_mean_std(ts)
    with print_lock:
        print "ts: %d, mean: %f, cmean: %f, stddev: %f, cstddev: %f" %(index, mean, cmean, stddev, cstddev)

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
def main():
    #get the data into data frame
    #df = pd.read_csv(dataLarge, sep=' ', header=None, index_col=0, nrows=10)
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

dataLarge = 'None'
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
    eprint("Data file does not exists aborting : %s" %(dataLarge))
    eprint("abs path : %s" %(absPath))
    usage()
    sys.exit()
else:
    eprint("Processing data file : %s " %(dataLarge))

if not os.path.exists('images'):
    os.makedirs('images')

main()
